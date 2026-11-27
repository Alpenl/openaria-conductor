use miro_ego_core::config::RecordingEncoding;
use miro_ego_core::ids::{new_uuid_v4, new_uuid_v7};
use miro_ego_core::locks::{LockMode, acquire_flock};
use miro_ego_core::paths::DataVolumePaths;
use miro_ego_storage::artifact_io::{
    open_verified_artifact_checked_with_volume, validate_artifact_relative_path,
};
use miro_ego_storage::audit::{ApiAuditEvent, AuditConfig, AuditLogger, AuditWriteTarget};
use miro_ego_storage::catalog::BinarySessionCatalog;
use miro_ego_storage::coordinator::{
    ActiveCaptureHandle, BackendStartRequest, CameraFocusSetRequest, CameraFocusStatus,
    CaptureBackend, CaptureCoordinator, CapturePhase, CaptureProgressSnapshot, CaptureStartRequest,
    CaptureStopRequest, CapturedArtifact, CapturedAudioOutcome, CapturedSessionArtifacts,
    CapturedVideoSegment, CoordinatorConfig, DeleteSessionTarget, DeleteSessionsRequest,
    RuntimeObservation, SESSION_DELETE_REQUEST_SCHEMA, StopReason,
};
use miro_ego_storage::logs::LogStore;
use miro_ego_storage::recovery::{build_manifest_encoding_json, sanitize_ndjson_file};
use miro_ego_storage::util::sha256_hex;
use miro_ego_storage::volume::{ExfatVolumeManager, FsCapacity, VolumePolicy, VolumeProbe};
use miro_ego_storage::{CATALOG_WAL_MAGIC, CATALOG_WAL_V2_MAGIC, StorageResult, VolumeManager};
use serde_json::json;
use std::fs;
use std::io;
use std::os::unix::fs::{MetadataExt, symlink};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

struct TestEnv {
    root: PathBuf,
    run_dir: PathBuf,
}

impl TestEnv {
    fn new(tag: &str) -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let base =
            std::env::temp_dir().join(format!("miro-ego-r3-{tag}-{}-{unique}", std::process::id()));
        let root = base.join("data");
        let run_dir = base.join("run");
        fs::create_dir_all(&root).unwrap();
        fs::create_dir_all(&run_dir).unwrap();
        Self { root, run_dir }
    }

    fn paths(&self) -> DataVolumePaths {
        DataVolumePaths::new(&self.root)
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        if let Some(parent) = self.root.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }
}

#[derive(Clone)]
struct DynamicProbe {
    cap: Arc<Mutex<FsCapacity>>,
    probe_calls: Arc<AtomicUsize>,
}

impl DynamicProbe {
    fn new(dir: &Path) -> Self {
        let dev = fs::metadata(dir).unwrap().dev();
        Self {
            cap: Arc::new(Mutex::new(FsCapacity {
                total_bytes: 64 * 1024 * 1024 * 1024,
                available_bytes: 32 * 1024 * 1024 * 1024,
                available_inodes: 100_000,
                read_only: false,
                writable: true,
                is_exfat: true,
                device_id: dev,
                mount_identity: format!("mount:r3:{dev}"),
                mounted: true,
            })),
            probe_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn set(&self, f: impl FnOnce(&mut FsCapacity)) {
        let mut guard = self.cap.lock().unwrap();
        f(&mut guard);
    }
}

impl VolumeProbe for DynamicProbe {
    fn probe_volume(&self, _mountpoint: &Path) -> io::Result<FsCapacity> {
        self.probe_calls.fetch_add(1, Ordering::AcqRel);
        Ok(self.cap.lock().unwrap().clone())
    }

    fn probe_system(&self, _state_root: &Path) -> io::Result<(u64, u64)> {
        Ok((8 * 1024 * 1024 * 1024, 100_000))
    }
}

struct DummyBackend;

struct DummyHandle {
    staging_dir: PathBuf,
    started_at: String,
    started_monotonic_ns: u64,
}

impl ActiveCaptureHandle for DummyHandle {
    fn progress(&self) -> CaptureProgressSnapshot {
        CaptureProgressSnapshot {
            elapsed_seconds: 1.0,
            captured_frames: 2,
            bytes_written: 1024,
        }
    }

    fn finish(self: Box<Self>, _reason: StopReason) -> StorageResult<CapturedSessionArtifacts> {
        let left_bytes = b"left-mp4-bytes";
        let right_bytes = b"right-mp4-bytes";
        let frames_bytes = b"{\"frame\":0}\n{\"frame\":1}\n";
        let imu_bytes = b"{\"seq\":0}\n";
        let left_path = "video/left_00000.mp4".to_owned();
        let right_path = "video/right_00000.mp4".to_owned();
        let frames_path = "frames.ndjson".to_owned();
        let imu_path = "imu.ndjson".to_owned();
        fs::write(self.staging_dir.join(&left_path), left_bytes).unwrap();
        fs::write(self.staging_dir.join(&right_path), right_bytes).unwrap();
        fs::write(self.staging_dir.join(&frames_path), frames_bytes).unwrap();
        fs::write(self.staging_dir.join(&imu_path), imu_bytes).unwrap();
        let _ = (&self.started_at, self.started_monotonic_ns);
        Ok(CapturedSessionArtifacts {
            duration_seconds: 1.0,
            effective_fps: 30.0,
            frames_count: 2,
            imu_sample_count: 1,
            dropped_frames: 0,
            drop_events: Vec::new(),
            segments: vec![CapturedVideoSegment {
                index: 0,
                start_frame: 0,
                end_frame: 2,
                start_time_seconds: 0.0,
                end_time_seconds: 1.0,
                left_path,
                left_bytes: left_bytes.len() as u64,
                left_sha256: Some(sha256_hex(left_bytes)),
                right_path,
                right_bytes: right_bytes.len() as u64,
                right_sha256: Some(sha256_hex(right_bytes)),
            }],
            frames_artifact: CapturedArtifact {
                role: "frames.index".to_owned(),
                path: frames_path,
                media_type: "application/x-ndjson".to_owned(),
                bytes: frames_bytes.len() as u64,
                sha256: Some(sha256_hex(frames_bytes)),
            },
            imu_artifact: CapturedArtifact {
                role: "imu.samples".to_owned(),
                path: imu_path,
                media_type: "application/x-ndjson".to_owned(),
                bytes: imu_bytes.len() as u64,
                sha256: Some(sha256_hex(imu_bytes)),
            },
            audio: CapturedAudioOutcome::NotRecorded {
                requested_mode: "disabled".to_owned(),
                reason: "disabled".to_owned(),
            },
            logs: Vec::new(),
        })
    }

    fn abort(self: Box<Self>) {}
}

impl CaptureBackend for DummyBackend {
    fn is_camera_connected(&self) -> bool {
        true
    }

    fn camera_focus(&self) -> StorageResult<Option<CameraFocusStatus>> {
        Ok(None)
    }

    fn set_camera_focus(&self, _req: &CameraFocusSetRequest) -> StorageResult<CameraFocusStatus> {
        unreachable!("focus not used in r3_hardening tests")
    }

    fn runtime_observation(&self) -> RuntimeObservation {
        RuntimeObservation::default_with_camera(true, None)
    }

    fn begin_capture(
        &self,
        req: &BackendStartRequest,
    ) -> StorageResult<Box<dyn ActiveCaptureHandle>> {
        Ok(Box::new(DummyHandle {
            staging_dir: req.staging_dir.clone(),
            started_at: req.started_at.clone(),
            started_monotonic_ns: req.started_monotonic_ns,
        }))
    }
}

fn make_coord(
    env: &TestEnv,
    probe: DynamicProbe,
) -> (
    Arc<ExfatVolumeManager>,
    Arc<BinarySessionCatalog>,
    Arc<CaptureCoordinator>,
) {
    let mut policy = VolumePolicy::relaxed_for_tests(1024 * 1024);
    policy.require_exfat = true;
    let vol = Arc::new(ExfatVolumeManager::with_probe(
        env.paths(),
        policy,
        Arc::new(probe),
    ));
    let _ = vol.ensure_layout().unwrap();
    let cat = Arc::new(BinarySessionCatalog::open(env.paths()).unwrap());
    let cfg = CoordinatorConfig {
        run_dir: Some(env.run_dir.clone()),
        require_clock_sync: false,
        ..Default::default()
    };
    let coord = Arc::new(CaptureCoordinator::new(
        cfg,
        Arc::clone(&vol),
        Arc::clone(&cat),
        Arc::new(DummyBackend),
    ));
    (vol, cat, coord)
}

fn write_sealed_session_on_disk(
    paths: &DataVolumePaths,
    rel_dir: &str,
    session_id: &str,
    shared_video_bytes: Option<&[u8]>,
) -> String {
    let dir = paths.recordings_dir().join(rel_dir);
    fs::create_dir_all(dir.join("video")).unwrap();
    let left_bytes = shared_video_bytes.unwrap_or(b"left-video");
    let right_bytes = shared_video_bytes.unwrap_or(b"right-video");
    let frames_bytes = b"{\"frame\":0}\n";
    let imu_bytes = b"{\"seq\":0}\n";
    fs::write(dir.join("video/left_00000.mp4"), left_bytes).unwrap();
    fs::write(dir.join("video/right_00000.mp4"), right_bytes).unwrap();
    fs::write(dir.join("frames.ndjson"), frames_bytes).unwrap();
    fs::write(dir.join("imu.ndjson"), imu_bytes).unwrap();

    let left_sha = sha256_hex(left_bytes);
    let right_sha = sha256_hex(right_bytes);
    let frames_sha = sha256_hex(frames_bytes);
    let imu_sha = sha256_hex(imu_bytes);

    let vol_id = fs::read(paths.volume_metadata_file())
        .ok()
        .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
        .and_then(|v| v["volume_id"].as_str().map(str::to_owned))
        .unwrap_or_else(new_uuid_v4);

    let manifest = json!({
        "schema": "miro-ego.device-session.v1",
        "manifest_id": new_uuid_v7(),
        "sealed": true,
        "sealed_at": "2026-11-27T10:00:05Z",
        "session_id": session_id,
        "volume_id": vol_id,
        "capture_mode": "production",
        "display_name": "R3 Test Session",
        "device": {
            "device_id": new_uuid_v4(),
            "device_label": "Miro-Ego-12345678",
            "hardware_fingerprint": format!("sha256:{}", "a".repeat(64)),
            "platform": "rdk-x5",
            "software_version": "0.3.0",
            "commit": "0".repeat(40),
        },
        "time": {
            "started_at": "2026-11-27T10:00:00Z",
            "ended_at": "2026-11-27T10:00:05Z",
            "timezone": "Etc/UTC",
            "duration_seconds": 5.0,
            "duration_clock": "host_monotonic",
        },
        "take": {
            "take_id": new_uuid_v7(),
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
            "codec": RecordingEncoding::default().codec.as_str(),
            "container": "mp4",
            "encoding": build_manifest_encoding_json(&RecordingEncoding::default(), "main"),
            "segments": [{
                "index": 0,
                "start_frame": 0,
                "end_frame": 2,
                "start_time_seconds": 0.0,
                "end_time_seconds": 5.0,
                "artifacts": {
                    "left": {
                        "artifact_id": left_sha,
                        "role": "video.left",
                        "path": "video/left_00000.mp4",
                        "media_type": "video/mp4",
                        "bytes": left_bytes.len() as u64,
                        "sha256": left_sha,
                    },
                    "right": {
                        "artifact_id": right_sha,
                        "role": "video.right",
                        "path": "video/right_00000.mp4",
                        "media_type": "video/mp4",
                        "bytes": right_bytes.len() as u64,
                        "sha256": right_sha,
                    }
                }
            }]
        },
        "imu": {
            "artifact": {
                "artifact_id": imu_sha,
                "role": "imu.samples",
                "path": "imu.ndjson",
                "media_type": "application/x-ndjson",
                "bytes": imu_bytes.len() as u64,
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
                "bytes": frames_bytes.len() as u64,
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
            "verified_at": "2026-11-27T10:00:05Z",
            "dropped_frames": 0,
            "drop_events": [],
            "fatal_errors": [],
        }
    });
    let mut bytes = serde_json::to_vec_pretty(&manifest).unwrap();
    bytes.push(b'\n');
    fs::write(dir.join("manifest.json"), &bytes).unwrap();
    sha256_hex(&bytes)
}

#[test]
fn r3_b02_b05_b07_wal_v2_stale_rejection_torn_fallback_readonly_and_shared_digest_verify() {
    let env = TestEnv::new("wal-v2");
    let paths = env.paths();
    let _ = paths.ensure_layout().unwrap();

    // 1. Fresh catalog + admit session writes MEGOWAL2 header bound to snapshot revision_seq.
    let sid1 = new_uuid_v7();
    let rel1 = "2026-11-27/10-00-00";
    write_sealed_session_on_disk(&paths, rel1, &sid1, None);
    let cat = BinarySessionCatalog::open(paths.clone()).unwrap();
    let snap_rev = cat.revision_seq().unwrap();

    let sid2 = new_uuid_v7();
    let rel2 = "2026-11-27/10-01-00";
    write_sealed_session_on_disk(&paths, rel2, &sid2, None);
    let _ = cat.admit_from_session_dir(rel2).unwrap();

    let wal_bytes = fs::read(paths.catalog_wal_file()).unwrap();
    assert_eq!(&wal_bytes[..8], &CATALOG_WAL_V2_MAGIC);
    let wal_base_rev = u64::from_le_bytes(wal_bytes[8..16].try_into().unwrap());
    assert_eq!(wal_base_rev, snap_rev);

    // 2. Plant a stale MEGOWAL2 WAL bound to a mismatched base_revision_seq (e.g. snap_rev + 99)
    //    containing a forget op for sid1. open_read_only ignores it without deleting; RW open
    //    ignores AND unlinks it!
    let mut stale_wal = wal_bytes.clone();
    stale_wal[8..16].copy_from_slice(&(snap_rev + 99).to_le_bytes());
    cat.compact().unwrap();
    fs::write(paths.catalog_wal_file(), &stale_wal).unwrap();

    let ro = BinarySessionCatalog::open_read_only(paths.clone()).unwrap();
    assert!(ro.get_indexed_session(&sid2).unwrap().is_some());
    assert!(
        paths.catalog_wal_file().is_file(),
        "open_read_only must not unlink stale WAL"
    );

    let rw = BinarySessionCatalog::open(paths.clone()).unwrap();
    assert!(rw.get_indexed_session(&sid2).unwrap().is_some());
    assert!(
        !paths.catalog_wal_file().exists(),
        "RW open must unlink stale WAL whose base_revision_seq != snapshot.revision_seq"
    );

    // 3. Verify backward read support for legacy MEGOWAL1 header when catalog.bin + WAL are intact.
    rw.compact().unwrap();
    let sid3 = new_uuid_v7();
    let rel3 = "2026-11-27/10-02-00";
    write_sealed_session_on_disk(&paths, rel3, &sid3, None);
    let _ = rw.admit_from_session_dir(rel3).unwrap();
    let v2_wal = fs::read(paths.catalog_wal_file()).unwrap();
    let mut v1_wal = Vec::from(CATALOG_WAL_MAGIC);
    v1_wal.extend_from_slice(&v2_wal[16..]);
    fs::write(paths.catalog_wal_file(), &v1_wal).unwrap();
    let ro_v1 = BinarySessionCatalog::open_read_only(paths.clone()).unwrap();
    assert!(ro_v1.get_indexed_session(&sid3).unwrap().is_some());

    // 4. Torn WAL tail forces open_fast_if_clean to fall back to full open(), and live RW open
    //    marks mount dirty by removing mount-clean.marker.
    rw.mark_mount_clean();
    assert!(paths.mount_clean_marker_file().is_file());
    let mut torn = Vec::from(CATALOG_WAL_V2_MAGIC);
    torn.extend_from_slice(&rw.revision_seq().unwrap().to_le_bytes());
    torn.extend_from_slice(&[1, 5, 0, 0]); // truncated 4-byte frame
    fs::write(paths.catalog_wal_file(), &torn).unwrap();
    let (fast_cat, skipped_scan) = BinarySessionCatalog::open_fast_if_clean(paths.clone()).unwrap();
    assert!(
        !skipped_scan,
        "torn WAL tail must force open_fast_if_clean to fall back to full open()"
    );
    assert!(
        !paths.mount_clean_marker_file().exists(),
        "live RW open must mark mount dirty"
    );
    drop(fast_cat);

    // 5. open_read_only: never sweeps control markers, never creates catalog.lock, and fails with
    //    lock_busy when catalog.lock is held exclusively.
    let marker_file = paths.recordings_dir().join(rel1).join("recording.json");
    fs::write(&marker_file, b"{}").unwrap();
    let _ = fs::remove_file(paths.catalog_file());
    let _ = fs::remove_file(paths.catalog_lock_file());
    let ro_no_lock = BinarySessionCatalog::open_read_only(paths.clone()).unwrap();
    assert!(ro_no_lock.get_indexed_session(&sid1).unwrap().is_some());
    assert!(
        marker_file.is_file(),
        "open_read_only must NEVER sweep control markers"
    );
    assert!(
        !paths.catalog_lock_file().exists(),
        "open_read_only must NEVER create catalog.lock when absent"
    );

    let ex_flock = acquire_flock(paths.catalog_lock_file(), LockMode::Exclusive, true).unwrap();
    let busy_err = BinarySessionCatalog::open_read_only(paths.clone())
        .err()
        .expect("open_read_only must fail instead of proceeding unlocked on exclusive lock");
    assert_eq!(busy_err.code(), "lock_busy");
    drop(ex_flock);

    // 6. R3-B07: Shared SHA-256 across two paths still checks EVERY path on disk!
    let sid_dup = new_uuid_v7();
    let rel_dup = "2026-11-27/10-03-00";
    write_sealed_session_on_disk(&paths, rel_dup, &sid_dup, Some(b"identical-stereo-bytes"));
    fs::remove_file(
        paths
            .recordings_dir()
            .join(rel_dup)
            .join("video/right_00000.mp4"),
    )
    .unwrap();
    let rebuilt = BinarySessionCatalog::open(paths.clone()).unwrap();
    let idx_dup = rebuilt.get_indexed_session(&sid_dup).unwrap().unwrap();
    assert_eq!(
        idx_dup.verification.unwrap().verdict,
        "unusable",
        "missing second path with shared SHA-256 must be marked unusable"
    );
}

#[test]
fn r3_b01_safeswap_lease_barrier_audit_lock_recheck_and_fsm_exclusion() {
    let env = TestEnv::new("safeswap-excl");
    let probe = DynamicProbe::new(&env.root);
    let (vol, cat, coord) = make_coord(&env, probe.clone());

    // 1. try_acquire_handle_lease + LogStore::with_volume + open_verified_artifact_checked_with_volume
    let lease = vol.try_acquire_handle_lease().unwrap();
    assert_eq!(vol.open_handle_count(), 1);
    drop(lease);

    fs::write(vol.paths().logs_dir().join("sys.log"), b"hello\n").unwrap();
    let log_store = LogStore::new(
        vol.paths().clone(),
        Arc::new({
            let v = Arc::clone(&vol);
            move || v.is_mounted_and_writable()
        }),
    )
    .with_volume(&vol);
    assert!(log_store.list_logs(None, None, None).is_ok());
    let opened_log = log_store.open_log("sys.log").unwrap();
    assert_eq!(vol.open_handle_count(), 1);
    drop(opened_log);

    let sid = new_uuid_v7();
    let rel = "2026-11-27/11-00-00";
    write_sealed_session_on_disk(vol.paths(), rel, &sid, None);
    let indexed = cat.admit_from_session_dir(rel).unwrap();
    let left_sha = sha256_hex(b"left-video");

    vol.set_volume_released(true);
    assert!(vol.try_acquire_handle_lease().is_none());
    assert!(log_store.open_log("sys.log").is_err());
    assert!(log_store.list_logs(None, None, None).is_err());
    assert!(
        open_verified_artifact_checked_with_volume(
            &vol.paths().recordings_dir(),
            &indexed.relative_dir,
            &left_sha,
            Some(&sid),
            None,
            false,
            Some(&vol),
        )
        .is_err()
    );
    vol.set_volume_released(false);

    // 2. recover_on_startup rejected when volume_released == true; safe_swap_receipt returns None
    //    when volume_released == false.
    assert!(coord.safe_swap_receipt().unwrap().is_none());
    let swap_ok = coord
        .stop_capture(CaptureStopRequest::safe_swap(), Some("idle-swap-1"))
        .unwrap();
    assert!(swap_ok.value.is_none());
    assert!(coord.safe_swap_receipt().unwrap().is_some());
    let rec_err = coord.recover_on_startup().unwrap_err();
    assert_eq!(rec_err.code(), "volume_released");
    coord.refresh_volume_admission().unwrap();
    assert!(coord.safe_swap_receipt().unwrap().is_none());

    // 3. Post-commit zero-handle recheck abort: hook re-acquires a handle during
    //    close_all_volume_handles_for_safe_swap after drain finishes -> SafeSwap aborts cleanly!
    let leaked_lease: Arc<Mutex<Option<miro_ego_storage::VolumeHandleLease>>> =
        Arc::new(Mutex::new(None));
    let hook_calls = Arc::new(AtomicUsize::new(0));
    {
        let leaked = Arc::clone(&leaked_lease);
        let calls = Arc::clone(&hook_calls);
        let c_ref = Arc::clone(&coord);
        coord.set_safe_swap_release_hook(Arc::new(move || {
            let n = calls.fetch_add(1, Ordering::AcqRel);
            // On the second pass (post-commit close_all_volume_handles_for_safe_swap), inject a lingering handle!
            if n == 1 {
                *leaked.lock().unwrap() = Some(c_ref.acquire_open_handle_lease());
            }
        }));
    }
    let abort_err = coord
        .stop_capture(CaptureStopRequest::safe_swap(), Some("idle-swap-abort"))
        .unwrap_err();
    assert_eq!(abort_err.code(), "volume_busy");
    assert!(!vol.is_volume_released());
    assert!(coord.safe_swap_receipt().unwrap().is_none());
    *leaked_lease.lock().unwrap() = None;

    // 4. FSM busy exclusion during Idle SafeSwap drain: concurrent start_capture and
    //    refresh_volume_admission fail with capture_busy.
    let (_vol2, _cat2, coord2) = make_coord(&env, probe.clone());
    coord2.set_safe_swap_drain_timeout(Duration::from_millis(250));
    let held_lease = coord2.acquire_open_handle_lease();
    let c_swap = Arc::clone(&coord2);
    let swap_thread = thread::spawn(move || {
        c_swap.stop_capture(CaptureStopRequest::safe_swap(), Some("slow-idle-swap"))
    });
    thread::sleep(Duration::from_millis(40));
    let start_err = coord2
        .start_capture(CaptureStartRequest::new_production(), None)
        .unwrap_err();
    assert_eq!(start_err.code(), "capture_busy");
    let adm_err = coord2.refresh_volume_admission().unwrap_err();
    assert_eq!(adm_err.code(), "capture_busy");
    drop(held_lease);
    assert!(swap_thread.join().unwrap().is_ok());

    // 5. AuditLogger re-checks volume_ready() inside disk.lock().
    let ready_flag = Arc::new(AtomicBool::new(true));
    let ready_calls = Arc::new(AtomicUsize::new(0));
    let logger = AuditLogger::new(
        vol.paths().clone(),
        AuditConfig::default(),
        Arc::new({
            let f = Arc::clone(&ready_flag);
            let c = Arc::clone(&ready_calls);
            move || {
                // First check (outside lock) returns true; second check (inside lock) flips false!
                let call = c.fetch_add(1, Ordering::AcqRel);
                if call >= 1 {
                    f.store(false, Ordering::Release);
                }
                f.load(Ordering::Acquire)
            }
        }),
    );
    let ev = ApiAuditEvent::new("race-audit", None, "getCaptureStatus", None, "allowed");
    let target = logger.record(&ev).unwrap();
    assert_eq!(target, AuditWriteTarget::MemoryFallback);
    assert!(!logger.has_open_file());
    assert_eq!(logger.fallback_stats().unwrap().entry_count, 1);
}

#[test]
fn r3_b04_b06_pending_delete_validation_and_early_break_and_memory_fallback() {
    let env = TestEnv::new("del-intent");
    let probe = DynamicProbe::new(&env.root);
    let (vol, cat, coord) = make_coord(&env, probe.clone());

    let sid_good = new_uuid_v7();
    let rel_good = "2026-11-27/12-00-00";
    write_sealed_session_on_disk(vol.paths(), rel_good, &sid_good, None);
    let _ = cat.admit_from_session_dir(rel_good).unwrap();

    let sid_sym = new_uuid_v7();
    let rel_sym = "2026-11-27/12-01-00";
    let outside_dir = env.root.parent().unwrap().join("outside-target");
    fs::create_dir_all(&outside_dir).unwrap();
    fs::write(outside_dir.join("keep.txt"), b"must-not-delete").unwrap();
    symlink(&outside_dir, vol.paths().recordings_dir().join(rel_sym)).unwrap();

    let sid_bad_rel = new_uuid_v7();
    let del_key = "crash-intent-key";
    let del_req = DeleteSessionsRequest {
        schema: SESSION_DELETE_REQUEST_SCHEMA.to_owned(),
        sessions: vec![DeleteSessionTarget {
            session_id: sid_good.clone(),
            manifest_sha256: "0".repeat(64),
        }],
    };
    let req_hash = sha256_hex(&serde_json::to_vec(&del_req).unwrap());

    let vol_id = vol.read_metadata().unwrap().volume_id;
    let receipts_path = vol.paths().receipts_dir().join("coordinator-receipts.json");
    fs::write(
        &receipts_path,
        serde_json::to_vec(&json!({
            "volume_id": vol_id,
            "idempotency": [],
            "safe_swap": null,
            "unsuccessful": [],
            "pending_delete": {
                "principal_id": "op-r3",
                "key": del_key,
                "request_hash": req_hash,
                "targets": [
                    [sid_good, rel_good],
                    [sid_sym, rel_sym],
                    [sid_bad_rel, "../escape"],
                ],
                "recorded_at": "2026-11-27T12:00:00Z",
            }
        }))
        .unwrap(),
    )
    .unwrap();
    drop(coord);
    drop(cat);

    // Boot fresh coordinator & run recover_on_startup() -> validates pending_delete targets!
    let (_vol2, cat2, coord2) = make_coord(&env, probe.clone());
    let _ = coord2.recover_on_startup().unwrap();
    assert!(
        outside_dir.join("keep.txt").is_file(),
        "symlink target must never be unlinked"
    );
    assert!(!vol.paths().recordings_dir().join(rel_good).exists());
    assert!(cat2.get_indexed_session(&sid_good).unwrap().is_none());

    let replayed = coord2
        .delete_sessions_for_principal(del_req, del_key, "op-r3")
        .unwrap();
    assert!(replayed.replayed);
    assert_eq!(replayed.value.deleted_session_ids, vec![sid_good.clone()]);
    assert_eq!(replayed.value.failed_sessions.len(), 2);
    assert!(
        replayed
            .value
            .failed_sessions
            .iter()
            .all(|f| f.session_id != sid_good)
    );

    // Verify Phase A early break pushes unattempted remaining targets into failed_sessions
    // AND never puts the same session_id in both deleted_session_ids and failed_sessions!
    let sid_a = new_uuid_v7();
    let sid_b = new_uuid_v7();
    let rel_a = "2026-11-27/12-02-00";
    let rel_b = "2026-11-27/12-03-00";
    let sha_a = write_sealed_session_on_disk(vol.paths(), rel_a, &sid_a, None);
    let sha_b = write_sealed_session_on_disk(vol.paths(), rel_b, &sid_b, None);
    let _ = cat2.admit_from_session_dir(rel_a).unwrap();
    let _ = cat2.admit_from_session_dir(rel_b).unwrap();

    // Obstruct `deleting/<sid_b>` with a read-only non-empty dir or simulate forget_paths_in_memory:
    // First test `forget_paths_in_memory` removes from in-memory catalog cleanly:
    let forgotten = cat2.forget_paths_in_memory(&[rel_a.to_owned()]).unwrap();
    assert_eq!(forgotten, 1);
    assert!(cat2.get_indexed_session(&sid_a).unwrap().is_none());

    // Now test `forget_paths` failure fallback during `delete_sessions`:
    // Replace `index/` with a file? No, `index/` already exists; make `catalog.wal` a directory so
    // `append_wal_mutation` & `write_snapshot` fail or wait: `write_bytes_atomic` uses `catalog.bin.tmp`,
    // whereas `remove_file(catalog.wal)` fails with `EISDIR`/`EPERM` when `catalog.wal` is a non-empty dir!
    cat2.compact().unwrap();
    let wal_dir = vol.paths().catalog_wal_file();
    fs::create_dir_all(wal_dir.join("nested")).unwrap();
    let del_b = coord2
        .delete_sessions_for_principal(
            DeleteSessionsRequest {
                schema: SESSION_DELETE_REQUEST_SCHEMA.to_owned(),
                sessions: vec![DeleteSessionTarget {
                    session_id: sid_b.clone(),
                    manifest_sha256: sha_b.clone(),
                }],
            },
            "del-fallback-b",
            "op-r3",
        )
        .unwrap();
    assert_eq!(del_b.value.deleted_session_ids, vec![sid_b.clone()]);
    assert!(
        del_b.value.failed_sessions.is_empty(),
        "physically deleted session must NOT appear in failed_sessions when forget_paths falls back to memory"
    );
    assert!(
        cat2.get_indexed_session(&sid_b).unwrap().is_none(),
        "forget_paths_in_memory fallback must prevent ghost catalog entries"
    );
    let _ = fs::remove_dir_all(&wal_dir);
    let _ = sha_a;
}

#[test]
fn r3_b07_b04_case_insensitive_validation_crash_safe_ndjson_and_failed_backoff() {
    let env = TestEnv::new("misc-b07");
    let probe = DynamicProbe::new(&env.root);
    let mut policy = VolumePolicy::relaxed_for_tests(1024 * 1024);
    policy.expected_uuid = Some("AABBCCDD-1122-3344-5566-77889900AABB".to_owned());
    policy.expected_partuuid = Some("DEAD-BEEF".to_owned());

    let vol = ExfatVolumeManager::with_probe(env.paths(), policy, Arc::new(probe.clone()));
    let _ = vol.ensure_layout().unwrap();
    fs::write(
        vol.paths().volume_metadata_file(),
        serde_json::to_vec_pretty(&json!({
            "schema": "miro-ego.volume.v1",
            "volume_id": new_uuid_v4(),
            "label": "MIRO-EGO",
            "uuid": "aabbccdd-1122-3344-5566-77889900aabb",
            "partuuid": "dead-beef",
        }))
        .unwrap(),
    )
    .unwrap();
    assert!(vol.admit(None).is_ok());

    // Case-insensitive control/temp path rejection
    assert!(validate_artifact_relative_path("MANIFEST.JSON").is_err());
    assert!(validate_artifact_relative_path("Recording.Json").is_err());
    assert!(validate_artifact_relative_path("video/chunk.TMP").is_err());
    assert!(validate_artifact_relative_path("video/left_00000.mp4").is_ok());

    // Crash-safe NDJSON sanitization (prefix + torn tail + empty line compaction via .tmp)
    let ndjson_path = env.root.join("test.ndjson");
    fs::write(
        &ndjson_path,
        b"{\"frame\":0}\n\n{\"frame\":1}\n{\"broken\":",
    )
    .unwrap();
    let kept = sanitize_ndjson_file(&ndjson_path).unwrap();
    assert_eq!(kept, 2);
    assert_eq!(
        fs::read_to_string(&ndjson_path).unwrap(),
        "{\"frame\":0}\n{\"frame\":1}\n"
    );
    assert!(!env.root.join("test.ndjson.tmp").exists());

    // Enforce storage reserve: CapturePhase::Failed must obey exponential backoff without
    // spinning at 1 Hz when `released_mount_signature` is None.
    let (_v, _c, coord) = make_coord(&env, probe.clone());
    coord
        .start_capture(CaptureStartRequest::new_production(), None)
        .unwrap();
    probe.set(|c| {
        c.mounted = false;
        c.writable = false;
    });
    let _ = coord.enforce_storage_reserve().unwrap();
    assert_eq!(coord.phase().unwrap(), CapturePhase::Failed);

    // Remount with broken volume.json (unadmittable) -> first tick calls `probe_raw_volume` (+1)
    // AND retries `reconcile_and_admit_volume` -> `admit` (+1 = +2 total) and sets 2s backoff;
    // immediate second tick ONLY calls `probe_raw_volume` (+1) and MUST NOT call `admit`!
    fs::write(vol.paths().volume_metadata_file(), b"{bad-json").unwrap();
    probe.set(|c| {
        c.mounted = true;
        c.writable = true;
    });
    let calls_before = probe.probe_calls.load(Ordering::Acquire);
    let _ = coord.enforce_storage_reserve().unwrap();
    let calls_after_first = probe.probe_calls.load(Ordering::Acquire);
    assert_eq!(
        calls_after_first - calls_before,
        2,
        "first tick must probe raw volume and attempt one admit()"
    );

    let _ = coord.enforce_storage_reserve().unwrap();
    let calls_after_second = probe.probe_calls.load(Ordering::Acquire);
    assert_eq!(
        calls_after_second - calls_after_first,
        1,
        "CapturePhase::Failed must obey exponential backoff (only probe_raw_volume, no admit retry)"
    );
}
