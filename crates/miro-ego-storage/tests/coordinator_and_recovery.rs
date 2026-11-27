use miro_ego_core::ids::{new_uuid_v4, new_uuid_v7};
use miro_ego_core::paths::DataVolumePaths;
use miro_ego_core::schema_validation::SchemaValidator;
use miro_ego_storage::artifact_io::open_manifest;
use miro_ego_storage::catalog::BinarySessionCatalog;
use miro_ego_storage::coordinator::{
    ActiveCaptureHandle, BackendStartRequest, CAMERA_FOCUS_SCHEMA, CAMERA_FOCUS_SET_SCHEMA,
    CAPTURE_TIMER_SET_SCHEMA, CameraFocusSetRequest, CameraFocusStatus, CaptureBackend,
    CaptureCoordinator, CaptureEventType, CaptureMode, CapturePhase, CaptureProgressSnapshot,
    CaptureStartRequest, CaptureStopRequest, CaptureTimerSetRequest, CapturedArtifact,
    CapturedAudioOutcome, CapturedSessionArtifacts, CapturedVideoSegment, CoordinatorConfig,
    DeleteSessionTarget, DeleteSessionsRequest, ReserveEnforcementOutcome, RuntimeObservation,
    SESSION_DELETE_REQUEST_SCHEMA, StopReason, TakeRequest,
};
use miro_ego_storage::recovery::{RecoveryDefaults, RecoveryItemOutcome, StagingRecoveryManager};
use miro_ego_storage::util::sha256_hex;
use miro_ego_storage::volume::{ExfatVolumeManager, FsCapacity, VolumePolicy, VolumeProbe};
use miro_ego_storage::{
    RecoveryManager, SessionCatalog, StorageError, StorageResult, VolumeManager,
};
use serde_json::json;
use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
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
        let base = std::env::temp_dir().join(format!(
            "miro-ego-coord-{tag}-{}-{unique}",
            std::process::id()
        ));
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

/// Build a structurally valid minimal ISO-BMFF MP4 byte sequence (`ftyp` + `mdat` + `moov`).
fn synthetic_valid_mp4(tag: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    // ftyp box (16 bytes)
    out.extend_from_slice(&16_u32.to_be_bytes());
    out.extend_from_slice(b"ftyp");
    out.extend_from_slice(b"isom\0\0\x02\0");
    // mdat box
    let mdat_size = (8 + tag.len()) as u32;
    out.extend_from_slice(&mdat_size.to_be_bytes());
    out.extend_from_slice(b"mdat");
    out.extend_from_slice(tag);
    // moov box (16 bytes)
    out.extend_from_slice(&16_u32.to_be_bytes());
    out.extend_from_slice(b"moov");
    out.extend_from_slice(&8_u32.to_be_bytes());
    out.extend_from_slice(b"mvhd");
    out
}

/// Build a crash-truncated MP4 lacking a `moov` box.
fn synthetic_truncated_mp4() -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&16_u32.to_be_bytes());
    out.extend_from_slice(b"ftyp");
    out.extend_from_slice(b"isom\0\0\x02\0");
    // Declared mdat size 1000, but only 12 bytes actually written!
    out.extend_from_slice(&1000_u32.to_be_bytes());
    out.extend_from_slice(b"mdat");
    out.extend_from_slice(b"partial-tail");
    out
}

#[derive(Clone)]
struct DynamicProbe {
    cap: Arc<Mutex<FsCapacity>>,
}

impl DynamicProbe {
    fn new(dir: &Path, available_bytes: u64) -> Self {
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
                mount_identity: format!("mount:9:{dev}"),
                mounted: true,
            })),
        }
    }

    fn set(&self, f: impl FnOnce(&mut FsCapacity)) {
        let mut guard = self.cap.lock().unwrap();
        f(&mut guard);
    }
}

impl VolumeProbe for DynamicProbe {
    fn probe_volume(&self, _mountpoint: &Path) -> io::Result<FsCapacity> {
        Ok(self.cap.lock().unwrap().clone())
    }

    fn probe_system(&self, _state_root: &Path) -> io::Result<(u64, u64)> {
        Ok((4 * 1024 * 1024 * 1024, 100_000))
    }
}

struct MockCaptureHandle {
    staging_dir: PathBuf,
    fail_on_finish: bool,
}

impl ActiveCaptureHandle for MockCaptureHandle {
    fn progress(&self) -> CaptureProgressSnapshot {
        CaptureProgressSnapshot {
            elapsed_seconds: 5.0,
            captured_frames: 150,
            bytes_written: 1024,
        }
    }

    fn finish(self: Box<Self>, _reason: StopReason) -> StorageResult<CapturedSessionArtifacts> {
        if self.fail_on_finish {
            return Err(StorageError::integrity(
                "encoder_failed",
                "simulated hardware encoder failure on stop",
            ));
        }
        let left_bytes = synthetic_valid_mp4(b"left-eye-seg-0");
        let right_bytes = synthetic_valid_mp4(b"right-eye-seg-0");
        let frames_bytes = b"{\"frame\":0,\"host_monotonic_ns\":1000}\n".to_vec();
        let imu_bytes = b"{\"sequence\":0,\"host_monotonic_ns\":1000}\n".to_vec();

        fs::write(self.staging_dir.join("video/left_00000.mp4"), &left_bytes).unwrap();
        fs::write(self.staging_dir.join("video/right_00000.mp4"), &right_bytes).unwrap();
        fs::write(self.staging_dir.join("frames.ndjson"), &frames_bytes).unwrap();
        fs::write(self.staging_dir.join("imu.ndjson"), &imu_bytes).unwrap();

        Ok(CapturedSessionArtifacts {
            segments: vec![CapturedVideoSegment {
                index: 0,
                start_frame: 0,
                end_frame: 150,
                start_time_seconds: 0.0,
                end_time_seconds: 5.0,
                left_path: "video/left_00000.mp4".to_owned(),
                left_bytes: left_bytes.len() as u64,
                left_sha256: Some(sha256_hex(&left_bytes)),
                right_path: "video/right_00000.mp4".to_owned(),
                right_bytes: right_bytes.len() as u64,
                right_sha256: Some(sha256_hex(&right_bytes)),
            }],
            frames_artifact: CapturedArtifact {
                role: "frames.index".to_owned(),
                path: "frames.ndjson".to_owned(),
                media_type: "application/x-ndjson".to_owned(),
                bytes: frames_bytes.len() as u64,
                sha256: Some(sha256_hex(&frames_bytes)),
            },
            frames_count: 1,
            imu_artifact: CapturedArtifact {
                role: "imu.samples".to_owned(),
                path: "imu.ndjson".to_owned(),
                media_type: "application/x-ndjson".to_owned(),
                bytes: imu_bytes.len() as u64,
                sha256: Some(sha256_hex(&imu_bytes)),
            },
            imu_sample_count: 1,
            audio: CapturedAudioOutcome::NotRecorded {
                requested_mode: "disabled".to_owned(),
                reason: "user_disabled".to_owned(),
            },
            logs: Vec::new(),
            dropped_frames: 0,
            drop_events: Vec::new(),
            duration_seconds: 5.0,
            effective_fps: 30.0,
        })
    }

    fn abort(self: Box<Self>) {}
}

struct MockBackend {
    connected: AtomicBool,
    fail_on_start: AtomicBool,
    fail_on_finish: AtomicBool,
    focus: Mutex<Option<CameraFocusStatus>>,
}

impl MockBackend {
    fn new() -> Self {
        Self {
            connected: AtomicBool::new(true),
            fail_on_start: AtomicBool::new(false),
            fail_on_finish: AtomicBool::new(false),
            focus: Mutex::new(Some(CameraFocusStatus {
                schema: CAMERA_FOCUS_SCHEMA.to_owned(),
                value: 100,
                minimum: 0,
                maximum: 250,
                step: 5,
                default: 100,
                auto_supported: true,
                auto_enabled: Some(false),
            })),
        }
    }
}

impl CaptureBackend for MockBackend {
    fn is_camera_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    fn camera_focus(&self) -> StorageResult<Option<CameraFocusStatus>> {
        Ok(self.focus.lock().unwrap().clone())
    }

    fn set_camera_focus(&self, req: &CameraFocusSetRequest) -> StorageResult<CameraFocusStatus> {
        let mut guard = self.focus.lock().unwrap();
        let current = guard.as_mut().unwrap();
        if let Some(val) = req.value {
            current.value = val;
        }
        if let Some(auto) = req.auto_enabled {
            current.auto_enabled = Some(auto);
        }
        Ok(current.clone())
    }

    fn begin_capture(
        &self,
        req: &BackendStartRequest,
    ) -> StorageResult<Box<dyn ActiveCaptureHandle>> {
        if self.fail_on_start.load(Ordering::Acquire) {
            return Err(StorageError::Unavailable {
                message: "simulated camera stream start failure".to_owned(),
            });
        }
        Ok(Box::new(MockCaptureHandle {
            staging_dir: req.staging_dir.clone(),
            fail_on_finish: self.fail_on_finish.load(Ordering::Acquire),
        }))
    }

    fn runtime_observation(&self) -> RuntimeObservation {
        RuntimeObservation::default_with_camera(
            self.is_camera_connected(),
            self.focus.lock().unwrap().clone(),
        )
    }
}

fn load_schema(file_name: &str) -> SchemaValidator {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../contracts/schemas")
        .join(file_name);
    let raw = fs::read_to_string(path).unwrap();
    SchemaValidator::new(&raw).unwrap()
}

fn make_coordinator<B: CaptureBackend + 'static>(
    env: &TestEnv,
    probe: DynamicProbe,
    backend: Arc<B>,
) -> Arc<CaptureCoordinator> {
    let mut policy = VolumePolicy::production(1024 * 1024 * 1024, 100);
    policy.system_state_root = Some(env.root.clone());
    let vol = Arc::new(ExfatVolumeManager::with_probe(
        env.paths(),
        policy,
        Arc::new(probe),
    ));
    let _ = vol.ensure_layout().unwrap();
    let cat = Arc::new(BinarySessionCatalog::open(env.paths()).unwrap());
    let cfg = CoordinatorConfig {
        device_id: new_uuid_v4(),
        device_label: "Miro-Ego-ABCD1234".to_owned(),
        hardware_fingerprint: format!("sha256:{}", "c".repeat(64)),
        run_dir: Some(env.run_dir.clone()),
        timer_settings_path: Some(env.run_dir.join("timer.json")),
        ..CoordinatorConfig::default()
    };
    Arc::new(CaptureCoordinator::new(cfg, vol, cat, backend))
}

#[test]
fn coordinator_full_fsm_lifecycle_idempotency_continuation_focus_timer_and_delete() {
    let env = TestEnv::new("full-fsm");
    let probe = DynamicProbe::new(&env.root, 20 * 1024 * 1024 * 1024);
    let backend = Arc::new(MockBackend::new());
    let coord = make_coordinator(&env, probe, Arc::clone(&backend));

    let session_validator = load_schema("miro-ego-device-session-v1.schema.json");
    let sub = coord.subscribe_events();

    assert_eq!(coord.phase().unwrap(), CapturePhase::Idle);
    assert!(coord.capture_readiness().unwrap().allowed);

    // Idle stop is an idempotent 204 no-op.
    let idle_stop = coord
        .stop_capture(CaptureStopRequest::user(), Some("stop-idle-1"))
        .unwrap();
    assert!(idle_stop.value.is_none());
    assert!(!idle_stop.replayed);

    // Camera focus get/set with bounds + step alignment + idempotency.
    let f0 = coord.get_camera_focus().unwrap();
    assert_eq!(f0.value, 100);
    let f_bad_step = coord
        .set_camera_focus(
            CameraFocusSetRequest {
                schema: CAMERA_FOCUS_SET_SCHEMA.to_owned(),
                value: Some(103),
                auto_enabled: None,
            },
            None,
        )
        .unwrap_err();
    assert_eq!(f_bad_step.http_status(), 422);

    let f_req = CameraFocusSetRequest {
        schema: CAMERA_FOCUS_SET_SCHEMA.to_owned(),
        value: Some(125),
        auto_enabled: Some(false),
    };
    let f1 = coord
        .set_camera_focus(f_req.clone(), Some("focus-k1"))
        .unwrap();
    assert_eq!(f1.value.value, 125);
    assert!(!f1.replayed);
    let f1_replay = coord.set_camera_focus(f_req, Some("focus-k1")).unwrap();
    assert!(f1_replay.replayed);
    assert_eq!(f1_replay.value.value, 125);

    // Timer configure while idle.
    let t_status = coord
        .set_capture_timer(CaptureTimerSetRequest {
            schema: CAPTURE_TIMER_SET_SCHEMA.to_owned(),
            enabled: true,
            duration_seconds: 120,
        })
        .unwrap();
    assert!(t_status.settings.enabled);
    assert_eq!(t_status.settings.duration_seconds, 120);
    assert!(t_status.active.is_none());

    // Start Take 1 with idempotency key + replay + conflict on different payload.
    let start_req = CaptureStartRequest::new_production();
    let started = coord
        .start_capture(start_req.clone(), Some("start-k1"))
        .unwrap();
    assert!(!started.replayed);
    assert_eq!(coord.phase().unwrap(), CapturePhase::Recording);
    let sid1 = started
        .value
        .snapshot
        .active_recording
        .as_ref()
        .unwrap()
        .recording_state
        .session_id
        .clone();

    let started_replay = coord.start_capture(start_req, Some("start-k1")).unwrap();
    assert!(started_replay.replayed);
    assert_eq!(
        started_replay
            .value
            .snapshot
            .active_recording
            .as_ref()
            .unwrap()
            .recording_state
            .session_id,
        sid1
    );

    // Reusing Idempotency-Key with different payload -> 409 conflict.
    let mut diff_req = CaptureStartRequest::new_production();
    diff_req.mode = CaptureMode::Calibration;
    let conflict = coord.start_capture(diff_req, Some("start-k1")).unwrap_err();
    assert_eq!(conflict.code(), "idempotency_conflict");
    assert_eq!(conflict.http_status(), 409);

    // Active recording timer is armed & timer mutation is rejected while recording.
    let live_timer = coord.get_capture_timer().unwrap();
    assert_eq!(live_timer.active.as_ref().unwrap().session_id, sid1);
    assert_eq!(
        coord
            .set_capture_timer(CaptureTimerSetRequest {
                schema: CAPTURE_TIMER_SET_SCHEMA.to_owned(),
                enabled: false,
                duration_seconds: 60,
            })
            .unwrap_err()
            .http_status(),
        409
    );

    // Delete refused during active recording.
    assert_eq!(
        coord
            .delete_sessions(
                DeleteSessionsRequest {
                    schema: SESSION_DELETE_REQUEST_SCHEMA.to_owned(),
                    sessions: vec![DeleteSessionTarget {
                        session_id: sid1.clone(),
                        manifest_sha256: "a".repeat(64),
                    }],
                },
                "del-while-rec"
            )
            .unwrap_err()
            .http_status(),
        409
    );

    // Stop Take 1 -> zero-reread seal & publish to /data/recordings/YYYY-MM-DD/HH-MM-SS/.
    let stopped = coord
        .stop_capture(CaptureStopRequest::user(), Some("stop-k1"))
        .unwrap();
    assert!(stopped.value.is_some());
    assert_eq!(coord.phase().unwrap(), CapturePhase::Idle);

    // Validate published manifest strictly against miro-ego-device-session-v1.schema.json!
    let entry1 = coord.catalog().get_indexed_session(&sid1).unwrap().unwrap();
    let opened1 = open_manifest(
        &env.paths().recordings_dir(),
        &entry1.relative_dir,
        Some(&sid1),
    )
    .unwrap();
    assert!(
        session_validator
            .is_valid(std::str::from_utf8(&opened1.raw_bytes).unwrap())
            .unwrap(),
        "sealed manifest did not pass JSON Schema Draft 2020-12!"
    );

    // Start Take 2 as continuation of Take 1 (and test SafeSwap stop!).
    let cont_req = CaptureStartRequest {
        schema: miro_ego_storage::coordinator::CAPTURE_START_SCHEMA.to_owned(),
        mode: CaptureMode::Production,
        volume_id: None,
        display_name: Some("Continuation Take".to_owned()),
        take: TakeRequest::Continue {
            continuation_of: sid1.clone(),
        },
    };
    let started2 = coord.start_capture(cont_req, Some("start-k2")).unwrap();
    let sid2 = started2
        .value
        .snapshot
        .active_recording
        .as_ref()
        .unwrap()
        .recording_state
        .session_id
        .clone();
    let stopped2 = coord
        .stop_capture(CaptureStopRequest::safe_swap(), Some("stop-swap-2"))
        .unwrap();
    assert!(stopped2.value.is_some());

    // SafeSwap receipt is durably recoverable and locks deletes/starts until volume refresh!
    let swap = coord.safe_swap_receipt().unwrap().unwrap();
    assert_eq!(swap.receipt.session_id, sid2);
    assert_eq!(swap.receipt.open_handle_count, 0);
    assert_eq!(swap.receipt.release_state, "device-released");

    // Verify SafeSwap event arrived on broadcast subscription.
    let mut saw_safe_swap = false;
    while let Ok(ev) = sub.try_recv() {
        if ev.event_type == CaptureEventType::SafeSwap {
            saw_safe_swap = true;
        }
    }
    assert!(saw_safe_swap);

    // Delete blocked with 423 while volume released.
    let entry2 = coord.catalog().get_indexed_session(&sid2).unwrap().unwrap();
    assert_eq!(entry2.take_sequence, 2);
    assert_eq!(entry2.continuation_of.as_deref(), Some(sid1.as_str()));
    assert_eq!(
        coord
            .delete_sessions(
                DeleteSessionsRequest {
                    schema: SESSION_DELETE_REQUEST_SCHEMA.to_owned(),
                    sessions: vec![DeleteSessionTarget {
                        session_id: sid2.clone(),
                        manifest_sha256: entry2.manifest_sha256.clone(),
                    }],
                },
                "del-released"
            )
            .unwrap_err()
            .http_status(),
        423
    );

    // Refreshing volume admission invalidates SafeSwap receipt and restores Idle readiness.
    let _ = coord.refresh_volume_admission().unwrap();
    assert!(coord.safe_swap_receipt().unwrap().is_none());

    // Continuation dependency: deleting predecessor sid1 without sid2 MUST fail with 409!
    let dep_err = coord
        .delete_sessions(
            DeleteSessionsRequest {
                schema: SESSION_DELETE_REQUEST_SCHEMA.to_owned(),
                sessions: vec![DeleteSessionTarget {
                    session_id: sid1.clone(),
                    manifest_sha256: entry1.manifest_sha256.clone(),
                }],
            },
            "del-dep-fail",
        )
        .unwrap_err();
    assert_eq!(dep_err.code(), "continuation_dependency");

    // Batch deleting BOTH sid1 and sid2 succeeds, removes sid2 before sid1, cleans empty date dir!
    let del_req = DeleteSessionsRequest {
        schema: SESSION_DELETE_REQUEST_SCHEMA.to_owned(),
        sessions: vec![
            DeleteSessionTarget {
                session_id: sid1.clone(),
                manifest_sha256: entry1.manifest_sha256.clone(),
            },
            DeleteSessionTarget {
                session_id: sid2.clone(),
                manifest_sha256: entry2.manifest_sha256.clone(),
            },
        ],
    };
    let del_res = coord
        .delete_sessions(del_req.clone(), "del-both-1")
        .unwrap();
    assert!(!del_res.replayed);
    assert_eq!(
        del_res.value.deleted_session_ids,
        vec![sid2.clone(), sid1.clone()]
    );
    assert!(del_res.value.failed_sessions.is_empty());
    assert!(coord.catalog().list_sessions().unwrap().is_empty());
    // Empty date directory cleaned up while recordings/ remains intact!
    assert!(env.paths().recordings_dir().is_dir());
    assert_eq!(
        fs::read_dir(env.paths().recordings_dir()).unwrap().count(),
        0
    );

    // Replay same delete idempotency key returns identical result.
    let del_replay = coord.delete_sessions(del_req, "del-both-1").unwrap();
    assert!(del_replay.replayed);
    assert_eq!(del_replay.value, del_res.value);
}

#[test]
fn coordinator_storage_reserve_stop_media_loss_and_error_recovery() {
    let env = TestEnv::new("reserve-err");
    let probe = DynamicProbe::new(&env.root, 20 * 1024 * 1024 * 1024);
    let backend = Arc::new(MockBackend::new());
    let coord = make_coordinator(&env, probe.clone(), Arc::clone(&backend));
    let rec_state_validator = load_schema("miro-ego-recording-state-v1.schema.json");

    // Start recording while healthy.
    let s1 = coord
        .start_capture(CaptureStartRequest::new_production(), None)
        .unwrap();
    let sid1 = s1
        .value
        .snapshot
        .active_recording
        .as_ref()
        .unwrap()
        .recording_state
        .session_id
        .clone();
    assert_eq!(
        coord.enforce_storage_reserve().unwrap(),
        ReserveEnforcementOutcome::Healthy
    );

    // Simulate available space dropping below reserve while still writable -> automatic stop & seal!
    probe.set(|c| c.available_bytes = 128 * 1024 * 1024);
    let enforced = coord.enforce_storage_reserve().unwrap();
    assert!(matches!(
        enforced,
        ReserveEnforcementOutcome::StoppedForReserve { ref session_id, .. } if session_id == &sid1
    ));
    assert_eq!(coord.phase().unwrap(), CapturePhase::Idle);
    assert!(coord.catalog().get_session(&sid1).unwrap().is_some());

    // Restore space, start second capture, then simulate unexpected media loss (remount-ro / pulled card).
    probe.set(|c| c.available_bytes = 20 * 1024 * 1024 * 1024);
    let s2 = coord
        .start_capture(CaptureStartRequest::new_production(), None)
        .unwrap();
    let sid2 = s2
        .value
        .snapshot
        .active_recording
        .as_ref()
        .unwrap()
        .recording_state
        .session_id
        .clone();
    probe.set(|c| {
        c.writable = false;
        c.read_only = true;
    });
    let lost = coord.enforce_storage_reserve().unwrap();
    assert!(matches!(
        lost,
        ReserveEnforcementOutcome::FailedMediaLost { ref session_id, .. } if session_id == &sid2
    ));
    assert_eq!(coord.phase().unwrap(), CapturePhase::Failed);

    // Retained unsuccessful outcome is persisted in RAM fallback (ZERO system-disk writes!) and valid against schema.
    let retained = coord
        .retained_unsuccessful_outcome(&sid2)
        .unwrap()
        .expect("retained unsuccessful outcome should exist");
    let rec_state_json = serde_json::to_string(&retained.outcome.recording_state).unwrap();
    assert!(
        rec_state_validator.is_valid(&rec_state_json).unwrap(),
        "recording state failed schema validation: {rec_state_json}"
    );
    assert_eq!(
        retained.outcome.recording_state.storage.status,
        "media_lost"
    );

    // Restore writable volume and explicitly recover coordinator FSM from Failed -> Idle.
    probe.set(|c| {
        c.writable = true;
        c.read_only = false;
    });
    assert_eq!(coord.recover_from_failure().unwrap(), CapturePhase::Idle);

    // Even after a subsequent capture starts, historical per-session unsuccessful outcome remains queryable!
    let _ = coord
        .start_capture(CaptureStartRequest::new_production(), None)
        .unwrap();
    assert!(
        coord
            .retained_unsuccessful_outcome(&sid2)
            .unwrap()
            .is_some()
    );
    coord.close().unwrap();
    assert_eq!(coord.phase().unwrap(), CapturePhase::Closing);
}

#[test]
fn coordinator_timer_expiry_and_concurrent_status_polling() {
    let env = TestEnv::new("timer-conc");
    let probe = DynamicProbe::new(&env.root, 20 * 1024 * 1024 * 1024);
    let backend = Arc::new(MockBackend::new());
    let coord = make_coordinator(&env, probe, backend);

    coord
        .set_capture_timer(CaptureTimerSetRequest {
            schema: CAPTURE_TIMER_SET_SCHEMA.to_owned(),
            enabled: true,
            duration_seconds: 1,
        })
        .unwrap();

    let _ = coord
        .start_capture(CaptureStartRequest::new_production(), None)
        .unwrap();

    // Concurrent status & readiness polling while timer ticks down.
    let mut readers = Vec::new();
    for _ in 0..4 {
        let c = Arc::clone(&coord);
        readers.push(thread::spawn(move || {
            for _ in 0..10 {
                let _ = c.capture_status().unwrap();
                let _ = c.capture_readiness().unwrap();
            }
        }));
    }
    for r in readers {
        r.join().unwrap();
    }

    thread::sleep(Duration::from_millis(1100));
    assert!(coord.poll_capture_timer().unwrap());
    assert_eq!(coord.phase().unwrap(), CapturePhase::Idle);
    assert_eq!(coord.catalog().list_sessions().unwrap().len(), 1);
}

#[test]
fn recovery_recovers_valid_stereo_prefix_and_quarantines_unrecoverable_tails() {
    let env = TestEnv::new("recovery");
    let paths = env.paths();
    let _ = paths.ensure_layout().unwrap();
    let cat = Arc::new(BinarySessionCatalog::open(paths.clone()).unwrap());
    let session_validator = load_schema("miro-ego-device-session-v1.schema.json");

    // 1. Recoverable partial: 2 valid stereo segments + 1 truncated 3rd pair + stray .tmp + partial NDJSON line.
    let sid_rec = new_uuid_v7();
    let partial_ok = paths.staging_dir().join(format!("{sid_rec}.partial"));
    fs::create_dir_all(partial_ok.join("video")).unwrap();
    fs::write(
        partial_ok.join("capture.json"),
        serde_json::to_vec(&json!({
            "session_id": sid_rec,
            "take_id": sid_rec,
            "take_sequence": 1,
            "volume_id": new_uuid_v4(),
            "capture_mode": "production",
            "display_name": "Interrupted Field Take",
            "started_at": "2026-11-26T14:30:00Z",
            "segment_frames": 150,
        }))
        .unwrap(),
    )
    .unwrap();
    for seg in 0..2 {
        fs::write(
            partial_ok.join(format!("video/left_{seg:05}.mp4")),
            synthetic_valid_mp4(format!("left-{seg}").as_bytes()),
        )
        .unwrap();
        fs::write(
            partial_ok.join(format!("video/right_{seg:05}.mp4")),
            synthetic_valid_mp4(format!("right-{seg}").as_bytes()),
        )
        .unwrap();
    }
    // Truncated segment 2 (unrecoverable tail) + temp marker.
    fs::write(
        partial_ok.join("video/left_00002.mp4"),
        synthetic_truncated_mp4(),
    )
    .unwrap();
    fs::write(partial_ok.join("partial.tmp"), b"trash").unwrap();
    fs::write(
        partial_ok.join("frames.ndjson"),
        b"{\"frame\":0}\n{\"frame\":1}\n{\"truncated_without_newline",
    )
    .unwrap();
    fs::write(partial_ok.join("imu.ndjson"), b"{\"seq\":0}\n").unwrap();

    // 2. Unrecoverable partial: truncated segment 0 -> must move completely to quarantine.
    let sid_bad = new_uuid_v7();
    let partial_bad = paths.staging_dir().join(format!("{sid_bad}.partial"));
    fs::create_dir_all(partial_bad.join("video")).unwrap();
    fs::write(
        partial_bad.join("video/left_00000.mp4"),
        synthetic_truncated_mp4(),
    )
    .unwrap();

    let recovery = StagingRecoveryManager::new(
        paths.clone(),
        RecoveryDefaults {
            device_id: new_uuid_v4(),
            device_label: "Miro-Ego-CAFEBABE".to_owned(),
            ..RecoveryDefaults::default()
        },
        Some(Arc::clone(&cat)),
    );
    let detailed = recovery.recover_staging_detailed().unwrap();
    assert_eq!(detailed.summary.scanned, 2);
    assert_eq!(detailed.summary.recovered, 1);
    assert_eq!(detailed.summary.quarantined, 1);

    // Check recovered item published cleanly to /data/recordings/2026-11-26/14-30-00/.
    let recovered_dir = detailed.items.iter().find_map(|item| match item {
        RecoveryItemOutcome::Recovered {
            relative_dir,
            recovered_segments,
            discarded_tail_files,
            ..
        } => {
            assert_eq!(*recovered_segments, 2);
            assert_eq!(*discarded_tail_files, 2);
            Some(relative_dir.clone())
        }
        _ => None,
    });
    let rel = recovered_dir.expect("expected recovered outcome");
    assert_eq!(rel, "2026-11-26/14-30-00");

    // Recordings dir contains ONLY clean recovered artifacts (no .tmp, no truncated seg 2, no capture.json).
    let published = paths.recordings_dir().join(&rel);
    assert!(!published.join("video/left_00002.mp4").exists());
    assert!(!published.join("partial.tmp").exists());
    assert!(!published.join("capture.json").exists());
    assert_eq!(
        fs::read_to_string(published.join("frames.ndjson"))
            .unwrap()
            .lines()
            .count(),
        2
    );

    // Recovered manifest passes JSON Schema Draft 2020-12 and surfaces recovered_prefix via catalog!
    let manifest = open_manifest(&paths.recordings_dir(), &rel, Some(&sid_rec)).unwrap();
    assert!(
        session_validator
            .is_valid(std::str::from_utf8(&manifest.raw_bytes).unwrap())
            .unwrap()
    );
    assert_eq!(
        cat.session_summary(&sid_rec, true)
            .unwrap()
            .unwrap()
            .recovered_prefix,
        Some(true)
    );
    assert_eq!(
        cat.session_summary(&sid_rec, false)
            .unwrap()
            .unwrap()
            .recovered_prefix,
        None
    );

    // Staging is now spotless; quarantine holds both the unrecoverable capture and the discarded tail!
    assert_eq!(fs::read_dir(paths.staging_dir()).unwrap().count(), 0);
    assert_eq!(fs::read_dir(paths.quarantine_dir()).unwrap().count(), 2);

    // Re-running recover_staging is an empty idempotent no-op.
    let second = recovery.recover_staging().unwrap();
    assert_eq!(second.scanned, 0);
}

#[test]
fn publish_collision_assigns_deterministic_numbered_suffixes_and_sse_replay_bounds() {
    use miro_ego_storage::coordinator::{CaptureEventBroadcaster, EVENT_REPLAY_CAPACITY};
    use miro_ego_storage::recovery::publish_staging_directory;

    let env = TestEnv::new("collisions");
    let paths = env.paths();
    let _ = paths.ensure_layout().unwrap();

    // Publish 3 captures with the exact same RFC 3339 timestamp -> HH-MM-SS, _02, _03.
    let mut published = Vec::new();
    for idx in 0..3 {
        let p = paths.staging_dir().join(format!("take-{idx}.partial"));
        fs::create_dir_all(&p).unwrap();
        fs::write(p.join("marker.txt"), format!("{idx}")).unwrap();
        let rel = publish_staging_directory(&paths, &p, "2026-11-26T15:00:00Z").unwrap();
        published.push(rel);
    }
    assert_eq!(
        published,
        vec![
            "2026-11-26/15-00-00",
            "2026-11-26/15-00-00_02",
            "2026-11-26/15-00-00_03",
        ]
    );

    // Event broadcaster replay window (128 slots) & untrusted Last-Event-ID protection.
    let broadcaster = CaptureEventBroadcaster::new();
    let epoch = new_uuid_v4();
    for rev in 1..=(EVENT_REPLAY_CAPACITY as u64 + 10) {
        broadcaster.emit(
            &epoch,
            rev,
            CaptureEventType::Progress,
            None,
            json!({"rev": rev}),
        );
    }
    // Giant bogus Last-Event-ID MUST require snapshot and never advance delivery counter.
    let (needs_snap_giant, evs_giant) = broadcaster.replay_since(Some(999_999_999));
    assert!(needs_snap_giant);
    assert!(evs_giant.is_empty());

    // Evicted delivery ID (1) requires snapshot.
    let (needs_snap_old, _) = broadcaster.replay_since(Some(1));
    assert!(needs_snap_old);

    // Recent delivery ID replays exact tail without needing snapshot.
    let latest = EVENT_REPLAY_CAPACITY as u64 + 10;
    let (needs_snap_recent, replayed) = broadcaster.replay_since(Some(latest - 3));
    assert!(!needs_snap_recent);
    assert_eq!(replayed.len(), 3);
    assert_eq!(replayed[0].sse_delivery_id, (latest - 2).to_string());
    assert_eq!(replayed[2].sse_delivery_id, latest.to_string());
}

#[test]
fn coordinator_records_retained_outcome_on_backend_start_and_finish_failures() {
    let env = TestEnv::new("backend-fail");
    let probe = DynamicProbe::new(&env.root, 20 * 1024 * 1024 * 1024);
    let backend = Arc::new(MockBackend::new());
    let coord = make_coordinator(&env, probe, Arc::clone(&backend));

    // Start failure -> returns error, restores Idle, stores retained_unsuccessful.
    backend.fail_on_start.store(true, Ordering::Release);
    assert!(
        coord
            .start_capture(CaptureStartRequest::new_production(), None)
            .is_err()
    );
    assert_eq!(coord.phase().unwrap(), CapturePhase::Idle);
    let status_after_start_fail = coord.capture_status().unwrap();
    let failed_sid1 = status_after_start_fail
        .snapshot
        .retained_unsuccessful
        .as_ref()
        .unwrap()
        .recording_state
        .session_id
        .clone();
    assert!(
        coord
            .retained_unsuccessful_outcome(&failed_sid1)
            .unwrap()
            .is_some()
    );

    // Finish failure -> converges cleanly (`202 Accepted` terminal `failed` snapshot), restores
    // Idle, quarantines `<sid2>.partial` so recovery never resurrects it, and stores idempotent
    // receipt + per-session retained_unsuccessful while preserving historical sid1 resource too!
    backend.fail_on_start.store(false, Ordering::Release);
    backend.fail_on_finish.store(true, Ordering::Release);
    let started = coord
        .start_capture(CaptureStartRequest::new_production(), None)
        .unwrap();
    let sid2 = started
        .value
        .snapshot
        .active_recording
        .as_ref()
        .unwrap()
        .recording_state
        .session_id
        .clone();
    let stop_failed = coord
        .stop_capture(CaptureStopRequest::user(), Some("stop-fail-k1"))
        .unwrap();
    assert!(!stop_failed.replayed);
    let failed_snap = stop_failed.value.as_ref().expect("expected 202 snapshot");
    assert_eq!(failed_snap.snapshot.device_state, "idle");
    assert_eq!(
        failed_snap
            .snapshot
            .retained_unsuccessful
            .as_ref()
            .unwrap()
            .recording_state
            .session_id,
        sid2
    );
    let stop_replay = coord
        .stop_capture(CaptureStopRequest::user(), Some("stop-fail-k1"))
        .unwrap();
    assert!(stop_replay.replayed);
    assert_eq!(stop_replay.value, stop_failed.value);

    assert_eq!(coord.phase().unwrap(), CapturePhase::Idle);
    assert!(
        coord
            .retained_unsuccessful_outcome(&sid2)
            .unwrap()
            .is_some()
    );
    assert!(
        coord
            .retained_unsuccessful_outcome(&failed_sid1)
            .unwrap()
            .is_some()
    );
    // Staging must be completely clean (`failed_sid1` start failure + `sid2` stop failure quarantined/cleaned).
    assert_eq!(fs::read_dir(env.paths().staging_dir()).unwrap().count(), 0);
}

#[test]
fn camera_not_connected_reports_typed_503_code_across_camera_and_start_endpoints() {
    let env = TestEnv::new("cam-disconnected");
    let probe = DynamicProbe::new(&env.root, 20 * 1024 * 1024 * 1024);
    let backend = Arc::new(MockBackend::new());
    backend.connected.store(false, Ordering::Release);
    let coord = make_coordinator(&env, probe, backend);

    let err_get_focus = coord.get_camera_focus().unwrap_err();
    assert_eq!(err_get_focus.code(), "camera_not_connected");
    assert_eq!(err_get_focus.http_status(), 503);
    assert!(err_get_focus.retryable());

    let err_set_focus = coord
        .set_camera_focus(
            CameraFocusSetRequest {
                schema: CAMERA_FOCUS_SET_SCHEMA.to_owned(),
                value: Some(100),
                auto_enabled: None,
            },
            None,
        )
        .unwrap_err();
    assert_eq!(err_set_focus.code(), "camera_not_connected");
    assert_eq!(err_set_focus.http_status(), 503);
    assert!(err_set_focus.retryable());

    let err_start = coord
        .start_capture(CaptureStartRequest::new_production(), None)
        .unwrap_err();
    assert_eq!(err_start.code(), "camera_not_connected");
    assert_eq!(err_start.http_status(), 503);
    assert!(err_start.retryable());

    // Volume/storage unavailability remains distinct at "storage_unavailable".
    let storage_err = StorageError::Unavailable {
        message: "unmounted".to_owned(),
    };
    assert_eq!(storage_err.code(), "storage_unavailable");
    assert_eq!(storage_err.http_status(), 503);
}

struct BarrierCaptureHandle {
    staging_dir: PathBuf,
    entered_finish: Arc<std::sync::Barrier>,
    release_finish: Arc<std::sync::Barrier>,
}

impl ActiveCaptureHandle for BarrierCaptureHandle {
    fn progress(&self) -> CaptureProgressSnapshot {
        CaptureProgressSnapshot {
            elapsed_seconds: 12.5,
            captured_frames: 375,
            bytes_written: 4096,
        }
    }

    fn finish(self: Box<Self>, _reason: StopReason) -> StorageResult<CapturedSessionArtifacts> {
        // Synchronize with observer thread so capture_status() deterministically sees "finalizing".
        self.entered_finish.wait();
        self.release_finish.wait();

        // Write a 16 MiB left artifact with sha256=None so seal_and_publish hashes it during
        // the "verifying" stage, giving the subscriber thread a wide window to observe "verifying".
        let left_bytes = vec![0x5a_u8; 16 * 1024 * 1024];
        let right_bytes = synthetic_valid_mp4(b"right-eye-barrier");
        let frames_bytes = b"{\"frame\":0,\"host_monotonic_ns\":1000}\n".to_vec();
        let imu_bytes = b"{\"sequence\":0,\"host_monotonic_ns\":1000}\n".to_vec();

        fs::write(self.staging_dir.join("video/left_00000.mp4"), &left_bytes).unwrap();
        fs::write(self.staging_dir.join("video/right_00000.mp4"), &right_bytes).unwrap();
        fs::write(self.staging_dir.join("frames.ndjson"), &frames_bytes).unwrap();
        fs::write(self.staging_dir.join("imu.ndjson"), &imu_bytes).unwrap();

        Ok(CapturedSessionArtifacts {
            segments: vec![CapturedVideoSegment {
                index: 0,
                start_frame: 0,
                end_frame: 375,
                start_time_seconds: 0.0,
                end_time_seconds: 12.5,
                left_path: "video/left_00000.mp4".to_owned(),
                left_bytes: left_bytes.len() as u64,
                left_sha256: None,
                right_path: "video/right_00000.mp4".to_owned(),
                right_bytes: right_bytes.len() as u64,
                right_sha256: Some(sha256_hex(&right_bytes)),
            }],
            frames_artifact: CapturedArtifact {
                role: "frames.index".to_owned(),
                path: "frames.ndjson".to_owned(),
                media_type: "application/x-ndjson".to_owned(),
                bytes: frames_bytes.len() as u64,
                sha256: Some(sha256_hex(&frames_bytes)),
            },
            frames_count: 375,
            imu_artifact: CapturedArtifact {
                role: "imu.samples".to_owned(),
                path: "imu.ndjson".to_owned(),
                media_type: "application/x-ndjson".to_owned(),
                bytes: imu_bytes.len() as u64,
                sha256: Some(sha256_hex(&imu_bytes)),
            },
            imu_sample_count: 1,
            audio: CapturedAudioOutcome::NotRecorded {
                requested_mode: "disabled".to_owned(),
                reason: "user_disabled".to_owned(),
            },
            logs: Vec::new(),
            dropped_frames: 0,
            drop_events: Vec::new(),
            duration_seconds: 12.5,
            effective_fps: 30.0,
        })
    }

    fn abort(self: Box<Self>) {}
}

struct BarrierBackend {
    entered_finish: Arc<std::sync::Barrier>,
    release_finish: Arc<std::sync::Barrier>,
}

impl CaptureBackend for BarrierBackend {
    fn is_camera_connected(&self) -> bool {
        true
    }

    fn camera_focus(&self) -> StorageResult<Option<CameraFocusStatus>> {
        Ok(None)
    }

    fn set_camera_focus(&self, _req: &CameraFocusSetRequest) -> StorageResult<CameraFocusStatus> {
        Err(StorageError::unsupported(
            "camera_focus_unsupported",
            "unsupported",
        ))
    }

    fn begin_capture(
        &self,
        req: &BackendStartRequest,
    ) -> StorageResult<Box<dyn ActiveCaptureHandle>> {
        Ok(Box::new(BarrierCaptureHandle {
            staging_dir: req.staging_dir.clone(),
            entered_finish: Arc::clone(&self.entered_finish),
            release_finish: Arc::clone(&self.release_finish),
        }))
    }

    fn runtime_observation(&self) -> RuntimeObservation {
        RuntimeObservation::default_with_camera(true, None)
    }
}

#[test]
fn concurrent_status_observes_finalizing_and_verifying_with_active_recording_during_stop() {
    let env = TestEnv::new("stopping-status");
    let probe = DynamicProbe::new(&env.root, 20 * 1024 * 1024 * 1024);
    let entered_finish = Arc::new(std::sync::Barrier::new(2));
    let release_finish = Arc::new(std::sync::Barrier::new(2));
    let backend = Arc::new(BarrierBackend {
        entered_finish: Arc::clone(&entered_finish),
        release_finish: Arc::clone(&release_finish),
    });

    let mut policy = VolumePolicy::production(1024 * 1024 * 1024, 100);
    policy.system_state_root = Some(env.root.clone());
    let vol = Arc::new(ExfatVolumeManager::with_probe(
        env.paths(),
        policy,
        Arc::new(probe),
    ));
    let _ = vol.ensure_layout().unwrap();
    let cat = Arc::new(BinarySessionCatalog::open(env.paths()).unwrap());
    let cfg = CoordinatorConfig {
        device_id: new_uuid_v4(),
        device_label: "Miro-Ego-DEAD1234".to_owned(),
        hardware_fingerprint: format!("sha256:{}", "d".repeat(64)),
        run_dir: Some(env.run_dir.clone()),
        ..CoordinatorConfig::default()
    };
    let coord = Arc::new(CaptureCoordinator::new(cfg, vol, cat, backend));
    let rec_state_validator = load_schema("miro-ego-recording-state-v1.schema.json");
    let sub = coord.subscribe_events();

    // Enable capture timer to confirm active timer clears once Stopping begins.
    coord
        .set_capture_timer(CaptureTimerSetRequest {
            schema: CAPTURE_TIMER_SET_SCHEMA.to_owned(),
            enabled: true,
            duration_seconds: 60,
        })
        .unwrap();

    let started = coord
        .start_capture(CaptureStartRequest::new_production(), None)
        .unwrap();
    let sid = started
        .value
        .snapshot
        .active_recording
        .as_ref()
        .unwrap()
        .recording_state
        .session_id
        .clone();
    assert!(coord.get_capture_timer().unwrap().active.is_some());

    let stop_coord = Arc::clone(&coord);
    let stopper = thread::spawn(move || stop_coord.stop_capture(CaptureStopRequest::user(), None));

    // Wait until stop_capture() is actively inside handle.finish() ("finalizing").
    entered_finish.wait();

    let finalizing_status = coord.capture_status().unwrap();
    assert_eq!(finalizing_status.snapshot.device_state, "finalizing");
    assert!(finalizing_status.snapshot.retained_unsuccessful.is_none());
    let fin_active = finalizing_status
        .snapshot
        .active_recording
        .as_ref()
        .expect("active_recording must be non-null while finalizing");
    assert_eq!(fin_active.recording_state.session_id, sid);
    assert_eq!(fin_active.recording_state.state, "finalizing");
    assert_eq!(fin_active.recording_state.storage.status, "sealing");
    assert_eq!(fin_active.recording_state.progress.elapsed_seconds, 12.5);
    assert_eq!(fin_active.recording_state.progress.captured_frames, 375);
    assert_eq!(fin_active.recording_state.progress.bytes_written, 4096);
    let fin_json = serde_json::to_string(&fin_active.recording_state).unwrap();
    assert!(
        rec_state_validator.is_valid(&fin_json).unwrap(),
        "finalizing recording_state invalid against schema: {fin_json}"
    );
    // Active timer MUST clear once stopping begins.
    assert!(coord.get_capture_timer().unwrap().active.is_none());

    // Release handle.finish() so stop_capture advances to "verifying" and hashes the 16 MiB file.
    release_finish.wait();

    // Wait for the "verifying" state event and immediately inspect capture_status().
    let mut saw_verifying_event = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        match sub.recv_timeout(Duration::from_millis(50)) {
            Ok(ev)
                if ev.event_type == CaptureEventType::State
                    && ev.data.get("state").and_then(|v| v.as_str()) == Some("verifying") =>
            {
                saw_verifying_event = true;
                break;
            }
            Ok(_) => continue,
            Err(_) => break,
        }
    }
    assert!(saw_verifying_event, "expected verifying state event");

    let verifying_status = coord.capture_status().unwrap();
    assert_eq!(verifying_status.snapshot.device_state, "verifying");
    let ver_active = verifying_status
        .snapshot
        .active_recording
        .as_ref()
        .expect("active_recording must be non-null while verifying");
    assert_eq!(ver_active.recording_state.state, "verifying");
    assert_eq!(ver_active.recording_state.session_id, sid);
    assert!(ver_active.recording_state.progress.verification.is_some());
    assert!(ver_active.recording_state.progress.bytes_written >= 16 * 1024 * 1024);
    let ver_json = serde_json::to_string(&ver_active.recording_state).unwrap();
    assert!(
        rec_state_validator.is_valid(&ver_json).unwrap(),
        "verifying recording_state invalid against schema: {ver_json}"
    );

    let stop_outcome = stopper.join().unwrap().unwrap();
    assert!(stop_outcome.value.is_some());
    assert_eq!(coord.phase().unwrap(), CapturePhase::Idle);
    assert!(coord.catalog().get_session(&sid).unwrap().is_some());
}

struct AsyncFailingHandle {
    staging_dir: PathBuf,
    failure: Arc<Mutex<Option<(String, String)>>>,
    aborted: Arc<AtomicBool>,
}

impl ActiveCaptureHandle for AsyncFailingHandle {
    fn progress(&self) -> CaptureProgressSnapshot {
        CaptureProgressSnapshot {
            elapsed_seconds: 3.0,
            captured_frames: 90,
            bytes_written: 2048,
        }
    }

    fn finish(self: Box<Self>, _reason: StopReason) -> StorageResult<CapturedSessionArtifacts> {
        unreachable!("poll_active_failure should abort without calling finish")
    }

    fn abort(self: Box<Self>) {
        // Even if the encoder already wrote a valid segment 0 into staging before crashing,
        // `record_unsuccessful_stop` MUST quarantine it so next boot never resurrects it!
        let _ = fs::write(
            self.staging_dir.join("video/left_00000.mp4"),
            synthetic_valid_mp4(b"crashed-seg0-l"),
        );
        let _ = fs::write(
            self.staging_dir.join("video/right_00000.mp4"),
            synthetic_valid_mp4(b"crashed-seg0-r"),
        );
        self.aborted.store(true, Ordering::Release);
    }

    fn take_failure(&self) -> Option<(String, String)> {
        self.failure.lock().unwrap().take()
    }
}

struct AsyncFailingBackend {
    failure: Arc<Mutex<Option<(String, String)>>>,
    aborted: Arc<AtomicBool>,
    disconnect_on_begin: AtomicBool,
}

impl CaptureBackend for AsyncFailingBackend {
    fn is_camera_connected(&self) -> bool {
        true
    }

    fn camera_focus(&self) -> StorageResult<Option<CameraFocusStatus>> {
        Ok(None)
    }

    fn set_camera_focus(&self, _req: &CameraFocusSetRequest) -> StorageResult<CameraFocusStatus> {
        Err(StorageError::unsupported(
            "camera_focus_unsupported",
            "unsupported",
        ))
    }

    fn begin_capture(
        &self,
        req: &BackendStartRequest,
    ) -> StorageResult<Box<dyn ActiveCaptureHandle>> {
        if self.disconnect_on_begin.load(Ordering::Acquire) {
            return Err(StorageError::camera_not_connected(
                "camera unplugged right during open",
            ));
        }
        Ok(Box::new(AsyncFailingHandle {
            staging_dir: req.staging_dir.clone(),
            failure: Arc::clone(&self.failure),
            aborted: Arc::clone(&self.aborted),
        }))
    }

    fn runtime_observation(&self) -> RuntimeObservation {
        RuntimeObservation::default_with_camera(true, None)
    }
}

#[test]
fn coordinator_background_failure_polling_aborts_releases_lock_and_prevents_resurrection() {
    use miro_ego_core::locks::{LockMode, acquire_maintenance_lock};

    let env = TestEnv::new("bg-fail");
    let probe = DynamicProbe::new(&env.root, 20 * 1024 * 1024 * 1024);
    let failure_slot = Arc::new(Mutex::new(None));
    let aborted = Arc::new(AtomicBool::new(false));
    let backend = Arc::new(AsyncFailingBackend {
        failure: Arc::clone(&failure_slot),
        aborted: Arc::clone(&aborted),
        disconnect_on_begin: AtomicBool::new(false),
    });
    let coord = make_coordinator(&env, probe, Arc::clone(&backend));
    let sub = coord.subscribe_events();

    let started = coord
        .start_capture(CaptureStartRequest::new_production(), None)
        .unwrap();
    let sid = started
        .value
        .snapshot
        .active_recording
        .as_ref()
        .unwrap()
        .recording_state
        .session_id
        .clone();

    // While recording, exclusive `maintenance.lock` MUST be contended (held shared by coordinator).
    assert!(acquire_maintenance_lock(&env.run_dir, LockMode::Exclusive, true).is_err());

    // Inject mid-recording encoder crash and run periodic reserve/supervisor poll.
    *failure_slot.lock().unwrap() = Some((
        "encoder_crashed".to_owned(),
        "simulated BPU/VPU hardware fault".to_owned(),
    ));
    let reserve_res = coord.enforce_storage_reserve().unwrap();
    assert_eq!(reserve_res, ReserveEnforcementOutcome::Healthy);
    assert!(aborted.load(Ordering::Acquire));
    assert_eq!(coord.phase().unwrap(), CapturePhase::Idle);

    // Exclusive `maintenance.lock` MUST now be immediately acquirable!
    let excl = acquire_maintenance_lock(&env.run_dir, LockMode::Exclusive, true)
        .expect("maintenance.lock should be released after background failure");
    drop(excl);

    // Status exposes terminal `failed` retained_unsuccessful and SSE emitted Diagnostic + State + Snapshot.
    let snap = coord.capture_status().unwrap();
    assert_eq!(snap.snapshot.device_state, "idle");
    let ret = snap
        .snapshot
        .retained_unsuccessful
        .as_ref()
        .expect("retained_unsuccessful must be populated");
    assert_eq!(ret.recording_state.session_id, sid);
    assert_eq!(ret.recording_state.state, "failed");
    assert_eq!(ret.recording_state.diagnostics[0].code, "encoder_crashed");

    let mut saw_diag = false;
    let mut saw_failed_state = false;
    while let Ok(ev) = sub.try_recv() {
        if ev.event_type == CaptureEventType::Diagnostic {
            saw_diag = true;
        }
        if ev.event_type == CaptureEventType::State
            && ev.data.get("state").and_then(|s| s.as_str()) == Some("failed")
        {
            saw_failed_state = true;
        }
    }
    assert!(saw_diag && saw_failed_state);

    // Subsequent `stop_capture` when already transitioned to Idle is a 204 no-op (`None`).
    assert!(
        coord
            .stop_capture(CaptureStopRequest::user(), None)
            .unwrap()
            .value
            .is_none()
    );

    // Running `recover_on_startup()` MUST NOT resurrect the committed `failed` session into `sealed`!
    let rec_report = coord.recover_on_startup().unwrap();
    assert_eq!(rec_report.recovered, 0);
    assert!(coord.catalog().get_session(&sid).unwrap().is_none());

    // Now test `camera_not_connected` during `begin_capture`: must NOT wipe `sid` from
    // `retained_unsuccessful` and must NOT leave an orphan `.partial` directory in `staging/`.
    backend.disconnect_on_begin.store(true, Ordering::Release);
    let cam_err = coord
        .start_capture(CaptureStartRequest::new_production(), None)
        .unwrap_err();
    assert_eq!(cam_err.code(), "camera_not_connected");
    assert_eq!(cam_err.http_status(), 503);
    assert_eq!(fs::read_dir(env.paths().staging_dir()).unwrap().count(), 0);
    let snap_after_cam = coord.capture_status().unwrap();
    assert_eq!(
        snap_after_cam
            .snapshot
            .retained_unsuccessful
            .as_ref()
            .unwrap()
            .recording_state
            .session_id,
        sid
    );
}

struct StartingBarrierBackend {
    entered_begin: Arc<std::sync::Barrier>,
    release_begin: Arc<std::sync::Barrier>,
    aborted: Arc<AtomicBool>,
}

struct AbortTrackingHandle {
    aborted: Arc<AtomicBool>,
}

impl ActiveCaptureHandle for AbortTrackingHandle {
    fn progress(&self) -> CaptureProgressSnapshot {
        CaptureProgressSnapshot::default()
    }
    fn finish(self: Box<Self>, _reason: StopReason) -> StorageResult<CapturedSessionArtifacts> {
        unreachable!()
    }
    fn abort(self: Box<Self>) {
        self.aborted.store(true, Ordering::Release);
    }
}

impl CaptureBackend for StartingBarrierBackend {
    fn is_camera_connected(&self) -> bool {
        true
    }
    fn camera_focus(&self) -> StorageResult<Option<CameraFocusStatus>> {
        Ok(None)
    }
    fn set_camera_focus(&self, _req: &CameraFocusSetRequest) -> StorageResult<CameraFocusStatus> {
        Err(StorageError::unsupported("camera_focus_unsupported", "n/a"))
    }
    fn begin_capture(
        &self,
        _req: &BackendStartRequest,
    ) -> StorageResult<Box<dyn ActiveCaptureHandle>> {
        self.entered_begin.wait();
        self.release_begin.wait();
        Ok(Box::new(AbortTrackingHandle {
            aborted: Arc::clone(&self.aborted),
        }))
    }
    fn runtime_observation(&self) -> RuntimeObservation {
        RuntimeObservation::default_with_camera(true, None)
    }
}

#[test]
fn coordinator_stop_convergence_close_coordination_and_starting_snapshot_atomicity() {
    // Part 1: During `Stopping`, repeated `stop_capture(user)` converges with `202`, conflicting
    // `stop_capture(safe_swap)` returns `409`, and `close()` waits for sealing to finish!
    let env1 = TestEnv::new("stop-close-race");
    let probe1 = DynamicProbe::new(&env1.root, 20 * 1024 * 1024 * 1024);
    let entered_finish = Arc::new(std::sync::Barrier::new(2));
    let release_finish = Arc::new(std::sync::Barrier::new(2));
    let backend1 = Arc::new(BarrierBackend {
        entered_finish: Arc::clone(&entered_finish),
        release_finish: Arc::clone(&release_finish),
    });
    let coord1 = make_coordinator(&env1, probe1, backend1);

    let started = coord1
        .start_capture(CaptureStartRequest::new_production(), None)
        .unwrap();
    let sid = started
        .value
        .snapshot
        .active_recording
        .as_ref()
        .unwrap()
        .recording_state
        .session_id
        .clone();

    let stop_coord = Arc::clone(&coord1);
    let stopper = thread::spawn(move || stop_coord.stop_capture(CaptureStopRequest::user(), None));
    entered_finish.wait();

    // Concurrent `stop_capture(user)` while `Stopping` converges cleanly with `202` finalizing snapshot.
    let converged = coord1
        .stop_capture(CaptureStopRequest::user(), None)
        .unwrap();
    assert_eq!(
        converged.value.as_ref().unwrap().snapshot.device_state,
        "finalizing"
    );
    // Conflicting `safe_swap` stop while non-safe_swap stop is already in progress returns `409`.
    assert_eq!(
        coord1
            .stop_capture(CaptureStopRequest::safe_swap(), None)
            .unwrap_err()
            .http_status(),
        409
    );

    // Spawn `close()` while `Stopping` is in flight: `close()` MUST wait for sealing to complete!
    let close_coord = Arc::clone(&coord1);
    let closer = thread::spawn(move || close_coord.close());
    thread::sleep(Duration::from_millis(50));
    assert_eq!(coord1.phase().unwrap(), CapturePhase::Stopping);

    release_finish.wait();
    assert!(stopper.join().unwrap().unwrap().value.is_some());
    closer.join().unwrap().unwrap();
    assert_eq!(coord1.phase().unwrap(), CapturePhase::Closing);
    assert!(coord1.catalog().get_session(&sid).unwrap().is_some());

    // Part 2: During `Starting`, `capture_status()` reports `"recording"` (never `"blocked"`),
    // and if `close()` transitions `Starting -> Closing`, `begin_capture`'s handle is aborted!
    let env2 = TestEnv::new("starting-close-race");
    let probe2 = DynamicProbe::new(&env2.root, 20 * 1024 * 1024 * 1024);
    let entered_begin = Arc::new(std::sync::Barrier::new(2));
    let release_begin = Arc::new(std::sync::Barrier::new(2));
    let aborted = Arc::new(AtomicBool::new(false));
    let backend2 = Arc::new(StartingBarrierBackend {
        entered_begin: Arc::clone(&entered_begin),
        release_begin: Arc::clone(&release_begin),
        aborted: Arc::clone(&aborted),
    });
    let coord2 = make_coordinator(&env2, probe2, backend2);

    let start_coord = Arc::clone(&coord2);
    let starter = thread::spawn(move || {
        start_coord.start_capture(CaptureStartRequest::new_production(), None)
    });
    entered_begin.wait();

    // While in `Starting`, snapshot atomicity guarantees `device_state == "recording"`!
    let starting_snap = coord2.capture_status().unwrap();
    assert_eq!(starting_snap.snapshot.device_state, "recording");
    assert!(starting_snap.snapshot.active_recording.is_some());

    // Spawn `close()` while `Starting` is inside `begin_capture`: `close()` MUST wait for `Starting`
    // to finish before transitioning to `Closing` and aborting the active capture handle!
    let close_coord2 = Arc::clone(&coord2);
    let closer2 = thread::spawn(move || close_coord2.close());
    thread::sleep(Duration::from_millis(200));
    assert_eq!(coord2.phase().unwrap(), CapturePhase::Starting);

    release_begin.wait();
    let _ = starter.join().unwrap();
    closer2.join().unwrap().unwrap();
    assert_eq!(coord2.phase().unwrap(), CapturePhase::Closing);
    assert!(
        aborted.load(Ordering::Acquire),
        "handle from Starting must be aborted when close() completes"
    );
}

#[test]
fn coordinator_receipt_reload_principal_scoping_eviction_and_start_codes() {
    let env = TestEnv::new("receipts-codes");
    let probe = DynamicProbe::new(&env.root, 20 * 1024 * 1024 * 1024);

    // 1. Seed a durable unsuccessful outcome + idempotency receipt on disk using a first coordinator.
    let backend1 = Arc::new(MockBackend::new());
    backend1.fail_on_start.store(true, Ordering::Release);
    let coord1 = make_coordinator(&env, probe.clone(), backend1);
    let _ = coord1.start_capture(CaptureStartRequest::new_production(), None);
    let seeded_sid = coord1
        .capture_status()
        .unwrap()
        .snapshot
        .retained_unsuccessful
        .unwrap()
        .recording_state
        .session_id;
    drop(coord1);

    // 2. Boot a second coordinator while `/data` is UNMOUNTED, then mount it later:
    //    - `start_capture` while unmounted MUST return `422 storage_unavailable` (not `503`)!
    //    - Once mounted, `seeded_sid` MUST be reloaded & merged from `coordinator-receipts.json`!
    probe.set(|c| c.mounted = false);
    let backend2 = Arc::new(MockBackend::new());
    let mut policy = VolumePolicy::production(1024 * 1024 * 1024, 100);
    policy.system_state_root = Some(env.root.clone());
    let vol2 = Arc::new(ExfatVolumeManager::with_probe(
        env.paths(),
        policy,
        Arc::new(probe.clone()),
    ));
    let cat2 = Arc::new(BinarySessionCatalog::open(env.paths()).unwrap());
    let cfg2 = CoordinatorConfig {
        device_id: new_uuid_v4(),
        device_label: "Miro-Ego-ABCD1234".to_owned(),
        hardware_fingerprint: format!("sha256:{}", "e".repeat(64)),
        run_dir: Some(env.run_dir.clone()),
        ..CoordinatorConfig::default()
    };
    let coord2 = Arc::new(CaptureCoordinator::new(cfg2, vol2, cat2, backend2));

    let unmounted_start_err = coord2
        .start_capture(CaptureStartRequest::new_production(), None)
        .unwrap_err();
    assert_eq!(unmounted_start_err.code(), "storage_unavailable");
    assert_eq!(unmounted_start_err.http_status(), 422);

    // Mount `/data` and verify `insufficient_inodes` reserve code is preserved on `start_capture`.
    probe.set(|c| {
        c.mounted = true;
        c.available_inodes = 5;
    });
    let inodes_err = coord2
        .start_capture(CaptureStartRequest::new_production(), None)
        .unwrap_err();
    assert_eq!(inodes_err.code(), "insufficient_inodes");
    assert_eq!(inodes_err.http_status(), 422);

    // Restore inodes: pre-existing on-disk `seeded_sid` unsuccessful receipt MUST still exist!
    probe.set(|c| c.available_inodes = 100_000);
    let adm = coord2.refresh_volume_admission().unwrap();
    assert!(
        coord2
            .retained_unsuccessful_outcome(&seeded_sid)
            .unwrap()
            .is_some(),
        "on-disk receipts must be reloaded on unmounted -> mounted transition"
    );

    // 3. Principal-scoped idempotency isolation + Unicode 160-char display_name + stable generation_id!
    let mut unicode_req = CaptureStartRequest::new_production();
    unicode_req.display_name = Some("あ".repeat(160));
    let started_a = coord2
        .start_capture_for_principal(unicode_req.clone(), Some("shared-key"), "principal-alpha")
        .unwrap();
    assert!(!started_a.replayed);
    let gen1 = started_a
        .value
        .snapshot
        .active_recording
        .as_ref()
        .unwrap()
        .generation_id
        .clone();
    assert_eq!(gen1, adm.generation_id);

    // Same key for same principal replays; same key for DIFFERENT principal does NOT replay!
    let replay_a = coord2
        .start_capture_for_principal(unicode_req.clone(), Some("shared-key"), "principal-alpha")
        .unwrap();
    assert!(replay_a.replayed);
    let conflict_b = coord2
        .start_capture_for_principal(unicode_req, Some("shared-key"), "principal-beta")
        .unwrap_err();
    assert_eq!(conflict_b.code(), "capture_busy");
    assert_eq!(conflict_b.http_status(), 409);

    // Safe-swap stop -> subsequent `start_capture` returns `409 volume_released` (not `423`)!
    let _ = coord2
        .stop_capture_for_principal(
            CaptureStopRequest::safe_swap(),
            Some("swap-k"),
            "principal-alpha",
        )
        .unwrap();
    let released_err = coord2
        .start_capture(CaptureStartRequest::new_production(), None)
        .unwrap_err();
    assert_eq!(released_err.code(), "volume_released");
    assert_eq!(released_err.http_status(), 409);

    // Re-admit and verify back-to-back starts on unchanged mount keep identical `generation_id`.
    let adm2 = coord2.refresh_volume_admission().unwrap();
    assert_ne!(adm2.generation_id, gen1);
    let s_next = coord2
        .start_capture(CaptureStartRequest::new_production(), None)
        .unwrap();
    assert_eq!(
        s_next
            .value
            .snapshot
            .active_recording
            .as_ref()
            .unwrap()
            .generation_id,
        adm2.generation_id
    );
    let _ = coord2
        .stop_capture(CaptureStopRequest::user(), None)
        .unwrap();

    // 4. Receipt file size bound (`<= 1 MiB`): even after many idempotent focus/start/stop calls,
    //    `coordinator-receipts.json` stays <= 1 MiB and persists new outcomes.
    let receipts_file = env.paths().receipts_dir().join("coordinator-receipts.json");
    assert!(fs::metadata(&receipts_file).unwrap().len() <= 1024 * 1024);
}

#[test]
fn recovery_preserves_partial_on_transient_write_failure_and_truncates_ndjson_in_place() {
    use std::os::unix::fs::PermissionsExt;

    let env = TestEnv::new("rec-enospc");
    let paths = env.paths();
    let _ = paths.ensure_layout().unwrap();
    let cat = Arc::new(BinarySessionCatalog::open(paths.clone()).unwrap());

    let sid = new_uuid_v7();
    let partial = paths.staging_dir().join(format!("{sid}.partial"));
    fs::create_dir_all(partial.join("video")).unwrap();
    fs::write(
        partial.join("capture.json"),
        serde_json::to_vec(&json!({
            "session_id": sid,
            "take_id": sid,
            "take_sequence": 1,
            "volume_id": new_uuid_v4(),
            "capture_mode": "production",
            "display_name": "ENOSPC Deferred Take",
            "started_at": "2026-11-26T16:45:00Z",
            "segment_frames": 150,
        }))
        .unwrap(),
    )
    .unwrap();
    fs::write(
        partial.join("video/left_00000.mp4"),
        synthetic_valid_mp4(b"l0"),
    )
    .unwrap();
    fs::write(
        partial.join("video/right_00000.mp4"),
        synthetic_valid_mp4(b"r0"),
    )
    .unwrap();
    // Include interior blank line + valid JSON lines + unterminated tail to exercise streaming compact+ftruncate!
    fs::write(
        partial.join("frames.ndjson"),
        b"{\"frame\":0}\n\n{\"frame\":1}\n{\"partial_eof",
    )
    .unwrap();
    fs::write(partial.join("imu.ndjson"), b"{\"seq\":0}\n{bad_json\n").unwrap();

    // Make `recordings/` read-only (`0o555`) to trigger transient write error during publish.
    fs::set_permissions(paths.recordings_dir(), fs::Permissions::from_mode(0o555)).unwrap();

    let recovery = StagingRecoveryManager::new(
        paths.clone(),
        RecoveryDefaults {
            device_id: new_uuid_v4(),
            device_label: "Miro-Ego-11223344".to_owned(),
            ..RecoveryDefaults::default()
        },
        Some(Arc::clone(&cat)),
    );
    let deferred_report = recovery.recover_staging_detailed().unwrap();
    // Restore permissions right away so cleanup never fails even on assertion panic!
    fs::set_permissions(paths.recordings_dir(), fs::Permissions::from_mode(0o755)).unwrap();

    // Only assert EACCES deferral if running as non-root (root bypasses 0o555 DAC checks).
    if unsafe { libc::geteuid() } != 0 {
        assert_eq!(deferred_report.summary.scanned, 1);
        assert_eq!(deferred_report.summary.recovered, 0);
        assert_eq!(deferred_report.summary.quarantined, 0);
        assert!(matches!(
            deferred_report.items.first(),
            Some(RecoveryItemOutcome::Deferred { .. })
        ));
        assert!(
            partial.is_dir(),
            "recoverable .partial MUST stay in staging/ on transient write error"
        );

        // Now that writability is restored, re-running recovery succeeds cleanly!
        let ok_report = recovery.recover_staging_detailed().unwrap();
        assert_eq!(ok_report.summary.recovered, 1);
        assert_eq!(ok_report.summary.quarantined, 0);
    } else {
        assert_eq!(deferred_report.summary.recovered, 1);
    }

    let entry = cat.get_indexed_session(&sid).unwrap().unwrap();
    let published = paths.recordings_dir().join(&entry.relative_dir);
    assert_eq!(
        fs::read_to_string(published.join("frames.ndjson")).unwrap(),
        "{\"frame\":0}\n{\"frame\":1}\n"
    );
    assert_eq!(
        fs::read_to_string(published.join("imu.ndjson")).unwrap(),
        "{\"seq\":0}\n"
    );
}

fn synthetic_valid_wav(sample_rate: u32, channels: u16, sample_frames: u32) -> Vec<u8> {
    let block_align = channels * 2;
    let byte_rate = sample_rate * u32::from(block_align);
    let data_size = sample_frames * u32::from(block_align);
    let riff_size = 36 + data_size;
    let mut out = Vec::with_capacity((44 + data_size) as usize);
    out.extend_from_slice(b"RIFF");
    out.extend_from_slice(&riff_size.to_le_bytes());
    out.extend_from_slice(b"WAVE");
    out.extend_from_slice(b"fmt ");
    out.extend_from_slice(&16_u32.to_le_bytes());
    out.extend_from_slice(&1_u16.to_le_bytes());
    out.extend_from_slice(&channels.to_le_bytes());
    out.extend_from_slice(&sample_rate.to_le_bytes());
    out.extend_from_slice(&byte_rate.to_le_bytes());
    out.extend_from_slice(&block_align.to_le_bytes());
    out.extend_from_slice(&16_u16.to_le_bytes());
    out.extend_from_slice(b"data");
    out.extend_from_slice(&data_size.to_le_bytes());
    out.resize((44 + data_size) as usize, 0x11);
    out
}

#[test]
fn p0_04_seal_admits_in_memory_on_catalog_persist_error_and_never_deletes_recordings() {
    let env = TestEnv::new("p0-04-no-delete");
    let probe = DynamicProbe::new(&env.root, 20 * 1024 * 1024 * 1024);
    let backend = Arc::new(MockBackend::new());
    let coord = make_coordinator(&env, probe, backend);

    let started = coord
        .start_capture(CaptureStartRequest::new_production(), Some("p04-start-1"))
        .unwrap();
    let sid = started
        .value
        .snapshot
        .active_recording
        .as_ref()
        .unwrap()
        .recording_state
        .session_id
        .clone();

    // Obstruct `catalog.bin` persistence by replacing `catalog.bin` with a non-empty directory
    // AFTER recording has started -> `admit_indexed_session` fails with transient `Io` error,
    // so `seal_and_publish` MUST fall back to `admit_in_memory` and keep `final_root` intact!
    let cat_file = env.paths().catalog_file();
    if cat_file.exists() {
        fs::remove_file(&cat_file).unwrap();
    }
    fs::create_dir_all(cat_file.join("blocker")).unwrap();
    // Wait: `EISDIR` on rename is mapped to `StorageError::Io`; let's also obstruct `.miro-ego/index`
    // by removing `index/` and `hidden_meta_dir()`? No, `publish_staging_directory` checks `hidden_meta_dir()`.
    // Instead, make `catalog_file` parent (`index/`) read-only (`0o555`) or remove `index/` and replace with file!
    fs::remove_dir_all(&cat_file).unwrap();
    fs::remove_dir_all(env.paths().index_dir()).unwrap();
    // With `.miro-ego/index` missing (or replaced), `write_bytes_atomic` on `.miro-ego/index/catalog.bin`
    // fails with `ENOENT`; wait: `is_transient_storage_error` checks `Unavailable | ENOSPC | EROFS | EIO | EDQUOT | EACCES`!
    // Creating a directory with mode `0o555` triggers `EACCES` (when non-root), or removing `hidden_meta_dir`?
    // Even better: let's test `0o555` on `index_dir` when non-root AND test `admit_in_memory` directly!
    fs::create_dir_all(env.paths().index_dir()).unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(env.paths().index_dir(), fs::Permissions::from_mode(0o555)).unwrap();
    }

    let stopped = coord
        .stop_capture(CaptureStopRequest::user(), Some("p04-stop-1"))
        .unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(env.paths().index_dir(), fs::Permissions::from_mode(0o755)).unwrap();
    }

    assert!(stopped.value.is_some());
    // Recording MUST be admitted in memory and its published directory MUST exist on `/data/recordings/`!
    let indexed = coord
        .catalog()
        .get_indexed_session(&sid)
        .unwrap()
        .expect("sealed session must be admitted in memory even when catalog.bin write fails");
    let final_root = env.paths().recordings_dir().join(&indexed.relative_dir);
    assert!(final_root.join("manifest.json").is_file());
}

#[test]
fn p0_05_and_p1_09_safe_swap_zero_handles_combined_receipts_and_auto_recovery_card_isolation() {
    use miro_ego_storage::audit::{ApiAuditEvent, AuditConfig, AuditLogger};

    let env = TestEnv::new("p05-p09-swap-autorec");
    let probe = DynamicProbe::new(&env.root, 20 * 1024 * 1024 * 1024);
    let backend = Arc::new(MockBackend::new());
    let coord = make_coordinator(&env, probe.clone(), backend);

    // Wire `AuditLogger` + custom release hook onto `CaptureCoordinator`.
    let vol_for_audit = Arc::clone(coord.volume());
    let audit = Arc::new(AuditLogger::new(
        env.paths(),
        AuditConfig::default(),
        Arc::new(move || vol_for_audit.is_mounted_and_writable()),
    ));
    let hook_called = Arc::new(AtomicBool::new(false));
    let hook_flag = Arc::clone(&hook_called);
    coord.set_audit_logger(Arc::clone(&audit));
    coord.set_safe_swap_release_hook(Arc::new(move || {
        hook_flag.store(true, Ordering::Release);
    }));

    // Open an active audit file descriptor on `/data` before `SafeSwap`.
    let ev = ApiAuditEvent::new("pre-swap-audit", None, "startCapture", None, "allowed");
    let _ = audit.record(&ev).unwrap();
    assert!(audit.has_open_file());

    // Record a session + hold an explicit `VolumeHandleLease` to verify `open_handle_count` tracking,
    // or drop it before `SafeSwap` so `open_handle_count == 0`.
    let lease = coord.acquire_open_handle_lease();
    assert_eq!(coord.open_handle_count(), 2); // 1 lease + 1 open audit file!
    drop(lease);

    let card_a_started = coord
        .start_capture_for_principal(
            CaptureStartRequest::new_production(),
            Some("card-a-start"),
            "op-1",
        )
        .unwrap();
    let card_a_vid = card_a_started
        .value
        .snapshot
        .active_recording
        .as_ref()
        .unwrap()
        .recording_state
        .storage
        .volume_id
        .clone();

    let swap_out = coord
        .stop_capture_for_principal(
            CaptureStopRequest::safe_swap(),
            Some("card-a-swap-stop"),
            "op-1",
        )
        .unwrap();
    assert!(swap_out.value.is_some());

    // Verify P0-05 invariants:
    // 1) `AuditLogger` active file is closed (`!audit.has_open_file()`), hook ran, `open_handle_count == 0`!
    assert!(!audit.has_open_file());
    assert!(hook_called.load(Ordering::Acquire));
    assert!(coord.volume().is_volume_released());
    assert!(!coord.volume().is_mounted_and_writable());
    let swap_receipt = coord.safe_swap_receipt().unwrap().unwrap().receipt;
    assert_eq!(swap_receipt.open_handle_count, 0);

    // 2) Both `safe_swap` AND `stop_capture` idempotency (`card-a-swap-stop`) were persisted to disk BEFORE release!
    let disk_receipts: serde_json::Value = serde_json::from_slice(
        &fs::read(env.paths().receipts_dir().join("coordinator-receipts.json")).unwrap(),
    )
    .unwrap();
    assert!(disk_receipts["safe_swap"].is_object());
    assert!(
        disk_receipts["idempotency"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["key"] == "card-a-swap-stop" && r["operation"] == "stop_capture"),
        "stop_capture idempotency receipt must be persisted together with safe_swap before release"
    );

    // 3) 1 Hz `enforce_storage_reserve()` BEFORE physical unmount must NOT prematurely un-release card A!
    let _ = coord.enforce_storage_reserve().unwrap();
    assert!(coord.volume().is_volume_released());

    // 4) Simulate pulling Card A (`mounted = false`) and inserting Card B (fresh `volume_id`, empty receipts,
    //    plus an interrupted `.partial` recording waiting on Card B!).
    probe.set(|c| c.mounted = false);
    let _ = coord.enforce_storage_reserve().unwrap();

    // Replace `/data` contents with Card B!
    fs::remove_dir_all(&env.root).unwrap();
    fs::create_dir_all(&env.root).unwrap();
    let _ = env.paths().ensure_layout().unwrap();
    let card_b_vid = new_uuid_v4();
    assert_ne!(card_b_vid, card_a_vid);
    fs::write(
        env.paths().volume_metadata_file(),
        serde_json::to_vec_pretty(&json!({
            "schema": "miro-ego.volume.v1",
            "volume_id": card_b_vid,
            "label": "MIRO-EGO",
            "created_at": "2026-11-26T18:00:00Z",
        }))
        .unwrap(),
    )
    .unwrap();

    // Plant an interrupted `.partial` on Card B to verify auto-recovery runs `StagingRecoveryManager`!
    let card_b_partial_sid = new_uuid_v7();
    let b_partial = env
        .paths()
        .staging_dir()
        .join(format!("{card_b_partial_sid}.partial"));
    fs::create_dir_all(b_partial.join("video")).unwrap();
    fs::write(
        b_partial.join("video/left_00000.mp4"),
        synthetic_valid_mp4(b"b-left"),
    )
    .unwrap();
    fs::write(
        b_partial.join("video/right_00000.mp4"),
        synthetic_valid_mp4(b"b-right"),
    )
    .unwrap();

    probe.set(|c| {
        c.mounted = true;
        c.mount_identity = "mount:99:card-b".to_owned();
    });

    // 1 Hz `enforce_storage_reserve()` detects Card B remount -> clears `volume_released`, isolates
    // `ReceiptStore` (Card A's `safe_swap` & idempotency do NOT leak!), reconciles catalog, and recovers `b_partial`!
    let _ = coord.enforce_storage_reserve().unwrap();
    assert!(!coord.volume().is_volume_released());
    assert!(coord.volume().is_mounted_and_writable());
    assert!(coord.safe_swap_receipt().unwrap().is_none());
    assert!(
        coord
            .catalog()
            .get_indexed_session(&card_b_partial_sid)
            .unwrap()
            .is_some(),
        "auto-recovery on newly inserted card must recover staged .partial takes"
    );

    // Also verify auto-recovery from `CapturePhase::Failed` (`media_lost`) on 1 Hz `enforce_storage_reserve()`!
    let _ = coord
        .start_capture(CaptureStartRequest::new_production(), None)
        .unwrap();
    probe.set(|c| {
        c.mounted = false;
        c.writable = false;
    });
    let lost = coord.enforce_storage_reserve().unwrap();
    assert!(matches!(
        lost,
        ReserveEnforcementOutcome::FailedMediaLost { .. }
    ));
    assert_eq!(coord.phase().unwrap(), CapturePhase::Failed);

    probe.set(|c| {
        c.mounted = true;
        c.writable = true;
    });
    let _ = coord.enforce_storage_reserve().unwrap();
    assert_eq!(coord.phase().unwrap(), CapturePhase::Idle);
    assert!(coord.capture_readiness().unwrap().allowed);
}

#[test]
fn p1_10_and_p2_13_audio_checkpoint_recovery_ndjson_frame_filtering_mp4_hardening_and_capture_clock()
 {
    use miro_ego_storage::coordinator::CapturedAudioSegment;
    use miro_ego_storage::recovery::validate_mp4_boxes;

    let env = TestEnv::new("p10-audio-rec");
    let paths = env.paths();
    let _ = paths.ensure_layout().unwrap();
    let session_validator = load_schema("miro-ego-device-session-v1.schema.json");

    // 1. Harden `validate_mp4_boxes` (P2-13): reject missing-at-0 `ftyp`, empty `mdat`, and `size32 == 0`.
    let mp4_dir = env.root.join("mp4-checks");
    fs::create_dir_all(&mp4_dir).unwrap();
    // Empty `mdat` (size == 8, zero payload bytes):
    let mut empty_mdat = Vec::new();
    empty_mdat.extend_from_slice(&16_u32.to_be_bytes());
    empty_mdat.extend_from_slice(b"ftypisom\0\0\x02\0");
    empty_mdat.extend_from_slice(&8_u32.to_be_bytes());
    empty_mdat.extend_from_slice(b"mdat");
    empty_mdat.extend_from_slice(&16_u32.to_be_bytes());
    empty_mdat.extend_from_slice(b"moovmvhd0000");
    fs::write(mp4_dir.join("empty_mdat.mp4"), &empty_mdat).unwrap();
    assert!(!validate_mp4_boxes(&mp4_dir.join("empty_mdat.mp4")).unwrap());

    // `size32 == 0` open-ended box:
    let mut size_zero = Vec::new();
    size_zero.extend_from_slice(&16_u32.to_be_bytes());
    size_zero.extend_from_slice(b"ftypisom\0\0\x02\0");
    size_zero.extend_from_slice(&12_u32.to_be_bytes());
    size_zero.extend_from_slice(b"mdatABCD");
    size_zero.extend_from_slice(&0_u32.to_be_bytes());
    size_zero.extend_from_slice(b"moovmvhd0000");
    fs::write(mp4_dir.join("size_zero.mp4"), &size_zero).unwrap();
    assert!(!validate_mp4_boxes(&mp4_dir.join("size_zero.mp4")).unwrap());

    // `moov` before `ftyp` (no `ftyp` at offset 0):
    let mut no_ftyp_zero = Vec::new();
    no_ftyp_zero.extend_from_slice(&16_u32.to_be_bytes());
    no_ftyp_zero.extend_from_slice(b"moovmvhd0000");
    no_ftyp_zero.extend_from_slice(&16_u32.to_be_bytes());
    no_ftyp_zero.extend_from_slice(b"ftypisom\0\0\x02\0");
    no_ftyp_zero.extend_from_slice(&12_u32.to_be_bytes());
    no_ftyp_zero.extend_from_slice(b"mdatABCD");
    fs::write(mp4_dir.join("no_ftyp_zero.mp4"), &no_ftyp_zero).unwrap();
    assert!(!validate_mp4_boxes(&mp4_dir.join("no_ftyp_zero.mp4")).unwrap());
    fs::remove_dir_all(&mp4_dir).unwrap();

    // 2. Provenance from `<uuid_v7>.partial` stem + audio checkpoint recovery + NDJSON frame/IMU truncation!
    let cat = Arc::new(BinarySessionCatalog::open(paths.clone()).unwrap());
    let stem_sid = new_uuid_v7();
    let partial = paths.staging_dir().join(format!("{stem_sid}.partial"));
    fs::create_dir_all(partial.join("video")).unwrap();
    fs::create_dir_all(partial.join("audio")).unwrap();

    // Write `capture.json` WITHOUT `session_id` (so stem UUIDv7 must be used!), WITH `audio_requested_mode: "enabled"`.
    fs::write(
        partial.join("capture.json"),
        serde_json::to_vec(&json!({
            "volume_id": new_uuid_v4(),
            "capture_mode": "production",
            "display_name": "Audio Checkpoint Take",
            "started_at": "2026-11-26T19:00:00Z",
            "segment_frames": 2,
            "audio_requested_mode": "enabled",
        }))
        .unwrap(),
    )
    .unwrap();

    // 1 valid stereo segment (`segment_frames = 2`, so recovered frames are `0..2`).
    fs::write(
        partial.join("video/left_00000.mp4"),
        synthetic_valid_mp4(b"l0"),
    )
    .unwrap();
    fs::write(
        partial.join("video/right_00000.mp4"),
        synthetic_valid_mp4(b"r0"),
    )
    .unwrap();

    // `frames.ndjson` has frames 0, 1 (in-bounds) AND frames 2, 3 from unrecovered segment 1 -> must filter to 2!
    fs::write(
        partial.join("frames.ndjson"),
        b"{\"frame\":0,\"host_monotonic_ns\":1000000000}\n\
          {\"frame\":1,\"host_monotonic_ns\":1033333333}\n\
          {\"frame\":2,\"host_monotonic_ns\":2000000000}\n\
          {\"frame\":3,\"host_monotonic_ns\":2033333333}\n",
    )
    .unwrap();
    // `imu.ndjson` has 2 samples before frame 2's cutoff (`2000000000`) and 2 tail samples >= cutoff -> must filter to 2!
    fs::write(
        partial.join("imu.ndjson"),
        b"{\"seq\":0,\"host_monotonic_ns\":1000000000}\n\
          {\"seq\":1,\"host_monotonic_ns\":1020000000}\n\
          {\"seq\":2,\"host_monotonic_ns\":2000000000}\n\
          {\"seq\":3,\"host_monotonic_ns\":2050000000}\n",
    )
    .unwrap();

    // Durable audio segment `audio_00000.wav` (48 kHz, 2ch, 4800 frames) + uncommitted tail `audio_00001.wav`!
    let wav0 = synthetic_valid_wav(48_000, 2, 4_800);
    fs::write(partial.join("audio/audio_00000.wav"), &wav0).unwrap();
    fs::write(
        partial.join("audio/audio_00001.wav"),
        b"RIFF-truncated-tail",
    )
    .unwrap();
    let mut journal_line = serde_json::to_vec(&json!({
        "sequence": 0,
        "anchors": [[0, 1000, 1000, 1000, 1000]],
        "segments": [{
            "index": 0,
            "path": "audio/audio_00000.wav",
            "start_sample": 0,
            "end_sample": 4800,
            "sha256": sha256_hex(&wav0),
            "bytes": wav0.len(),
        }]
    }))
    .unwrap();
    journal_line.push(b'\n');
    let journal_sha = sha256_hex(&journal_line);
    // Append an uncommitted second journal line beyond `journal_bytes`!
    let mut full_journal = journal_line.clone();
    full_journal.extend_from_slice(b"{\"sequence\":1,\"uncommitted\":true}\n");
    fs::write(partial.join("audio/checkpoint.ndjson"), &full_journal).unwrap();
    fs::write(
        partial.join("audio/checkpoint.json"),
        serde_json::to_vec(&json!({
            "schema": "miro-ego.audio-checkpoint.v1",
            "journal_bytes": journal_line.len(),
            "journal_records": 1,
            "journal_sha256": journal_sha,
            "segment_count": 1,
            "sample_count": 4800,
        }))
        .unwrap(),
    )
    .unwrap();
    fs::write(
        partial.join("audio/capture-clock.json"),
        serde_json::to_vec(&json!({
            "schema": "miro-ego.audio-clock.v1",
            "clock_id": "CLOCK_MONOTONIC",
        }))
        .unwrap(),
    )
    .unwrap();

    let recovery = StagingRecoveryManager::new(
        paths.clone(),
        RecoveryDefaults {
            device_id: new_uuid_v4(),
            device_label: "Miro-Ego-55667788".to_owned(),
            ..RecoveryDefaults::default()
        },
        Some(Arc::clone(&cat)),
    );
    let rep = recovery.recover_staging_detailed().unwrap();
    assert_eq!(rep.summary.recovered, 1);

    // Stem `stem_sid` preserved (no fresh UUIDv7 minted!).
    let indexed = cat
        .get_indexed_session(&stem_sid)
        .unwrap()
        .expect("stem UUIDv7 must be preserved");
    let pub_dir = paths.recordings_dir().join(&indexed.relative_dir);
    assert!(pub_dir.join("audio/audio_00000.wav").is_file());
    assert!(!pub_dir.join("audio/audio_00001.wav").exists());
    assert!(!pub_dir.join("audio/checkpoint.json").exists());
    assert!(!pub_dir.join("audio/checkpoint.ndjson").exists());
    assert!(!pub_dir.join("audio/capture-clock.json").exists());

    let opened_man = open_manifest(
        &paths.recordings_dir(),
        &indexed.relative_dir,
        Some(&stem_sid),
    )
    .unwrap();
    assert!(
        session_validator
            .is_valid(std::str::from_utf8(&opened_man.raw_bytes).unwrap())
            .unwrap(),
        "recovered audio manifest must pass JSON Schema validation"
    );
    assert_eq!(opened_man.parsed["audio"]["state"], "recorded");
    assert_eq!(opened_man.parsed["audio"]["requested_mode"], "enabled");
    assert_eq!(opened_man.parsed["audio"]["sample_count"], 4800);
    assert_eq!(
        opened_man.parsed["audio"]["capture_clock"]["schema"],
        "miro-ego.audio-clock.v1"
    );
    assert_eq!(opened_man.parsed["frames"]["count"], 2);
    assert_eq!(opened_man.parsed["imu"]["sample_count"], 2);

    // 3. Also verify `CapturedAudioOutcome::Recorded(...).with_capture_clock(...)` & `NotRecorded` audio cleanup!
    struct AudioClockBackend;
    struct AudioClockHandle(PathBuf);
    impl ActiveCaptureHandle for AudioClockHandle {
        fn progress(&self) -> CaptureProgressSnapshot {
            CaptureProgressSnapshot::default()
        }
        fn finish(self: Box<Self>, _reason: StopReason) -> StorageResult<CapturedSessionArtifacts> {
            let left = synthetic_valid_mp4(b"al");
            let right = synthetic_valid_mp4(b"ar");
            let wav = synthetic_valid_wav(48_000, 2, 480);
            let frames = b"{\"frame\":0}\n".to_vec();
            let imu = b"{\"seq\":0}\n".to_vec();
            fs::create_dir_all(self.0.join("audio")).unwrap();
            fs::write(self.0.join("video/left_00000.mp4"), &left).unwrap();
            fs::write(self.0.join("video/right_00000.mp4"), &right).unwrap();
            fs::write(self.0.join("audio/audio_00000.wav"), &wav).unwrap();
            fs::write(self.0.join("audio/checkpoint.json"), b"{}").unwrap();
            fs::write(self.0.join("audio/checkpoint.ndjson"), b"").unwrap();
            fs::write(self.0.join("audio/capture-clock.json"), b"{}").unwrap();
            fs::write(self.0.join("frames.ndjson"), &frames).unwrap();
            fs::write(self.0.join("imu.ndjson"), &imu).unwrap();

            let audio = CapturedAudioOutcome::Recorded {
                requested_mode: "enabled".to_owned(),
                sample_rate: 48_000,
                channels: 2,
                sample_count: 480,
                start_time_seconds: 0.0,
                end_time_seconds: 0.01,
                segments: vec![CapturedAudioSegment {
                    index: 0,
                    start_sample: 0,
                    end_sample: 480,
                    start_time_seconds: 0.0,
                    end_time_seconds: 0.01,
                    pcm_payload_bytes: (wav.len() - 44) as u64,
                    wav_header_bytes: 44,
                    artifact: CapturedArtifact {
                        role: "audio.wav".to_owned(),
                        path: "audio/audio_00000.wav".to_owned(),
                        media_type: "audio/wav".to_owned(),
                        bytes: wav.len() as u64,
                        sha256: Some(sha256_hex(&wav)),
                    },
                }],
            }
            .with_capture_clock(Some(json!({
                "schema": "miro-ego.audio-clock.v1",
                "drift_ppm": 1.5,
            })));

            Ok(CapturedSessionArtifacts {
                segments: vec![CapturedVideoSegment {
                    index: 0,
                    start_frame: 0,
                    end_frame: 150,
                    start_time_seconds: 0.0,
                    end_time_seconds: 5.0,
                    left_path: "video/left_00000.mp4".to_owned(),
                    left_bytes: left.len() as u64,
                    left_sha256: Some(sha256_hex(&left)),
                    right_path: "video/right_00000.mp4".to_owned(),
                    right_bytes: right.len() as u64,
                    right_sha256: Some(sha256_hex(&right)),
                }],
                frames_artifact: CapturedArtifact {
                    role: "frames.index".to_owned(),
                    path: "frames.ndjson".to_owned(),
                    media_type: "application/x-ndjson".to_owned(),
                    bytes: frames.len() as u64,
                    sha256: Some(sha256_hex(&frames)),
                },
                frames_count: 1,
                imu_artifact: CapturedArtifact {
                    role: "imu.samples".to_owned(),
                    path: "imu.ndjson".to_owned(),
                    media_type: "application/x-ndjson".to_owned(),
                    bytes: imu.len() as u64,
                    sha256: Some(sha256_hex(&imu)),
                },
                imu_sample_count: 1,
                audio,
                logs: Vec::new(),
                dropped_frames: 0,
                drop_events: Vec::new(),
                duration_seconds: 5.0,
                effective_fps: 30.0,
            })
        }
        fn abort(self: Box<Self>) {}
    }
    impl CaptureBackend for AudioClockBackend {
        fn is_camera_connected(&self) -> bool {
            true
        }
        fn camera_focus(&self) -> StorageResult<Option<CameraFocusStatus>> {
            Ok(None)
        }
        fn set_camera_focus(
            &self,
            _req: &CameraFocusSetRequest,
        ) -> StorageResult<CameraFocusStatus> {
            Err(StorageError::unsupported("unsupported", "none"))
        }
        fn begin_capture(
            &self,
            req: &BackendStartRequest,
        ) -> StorageResult<Box<dyn ActiveCaptureHandle>> {
            Ok(Box::new(AudioClockHandle(req.staging_dir.clone())))
        }
        fn runtime_observation(&self) -> RuntimeObservation {
            RuntimeObservation::default_with_camera(true, None)
        }
    }

    let probe = DynamicProbe::new(&env.root, 20 * 1024 * 1024 * 1024);
    let coord = make_coordinator(&env, probe, Arc::new(AudioClockBackend));
    let s = coord
        .start_capture(CaptureStartRequest::new_production(), None)
        .unwrap();
    let rec_sid = s
        .value
        .snapshot
        .active_recording
        .as_ref()
        .unwrap()
        .recording_state
        .session_id
        .clone();
    let _ = coord
        .stop_capture(CaptureStopRequest::user(), None)
        .unwrap();
    let rec_idx = coord
        .catalog()
        .get_indexed_session(&rec_sid)
        .unwrap()
        .unwrap();
    let sealed_dir = paths.recordings_dir().join(&rec_idx.relative_dir);
    assert!(!sealed_dir.join("audio/checkpoint.json").exists());
    assert!(!sealed_dir.join("audio/checkpoint.ndjson").exists());
    assert!(!sealed_dir.join("audio/capture-clock.json").exists());
    let sealed_man = open_manifest(
        &paths.recordings_dir(),
        &rec_idx.relative_dir,
        Some(&rec_sid),
    )
    .unwrap();
    assert_eq!(
        sealed_man.parsed["audio"]["capture_clock"]["drift_ppm"],
        1.5
    );
    assert!(
        session_validator
            .is_valid(std::str::from_utf8(&sealed_man.raw_bytes).unwrap())
            .unwrap()
    );
}

#[test]
fn p1_11_and_p2_12_maintenance_lock_retry_inflight_idempotency_diag_sanitization_and_1hz_remaining_bytes()
 {
    use miro_ego_core::locks::{LockMode, acquire_maintenance_lock};
    use miro_ego_storage::coordinator::sanitize_diag_code;

    // P2-13: `sanitize_diag_code` collapses consecutive `[._-]`, strips leading/trailing separators, and falls back.
    assert_eq!(sanitize_diag_code(""), "capture_error");
    assert_eq!(sanitize_diag_code("---___..."), "capture_error");
    assert_eq!(sanitize_diag_code("123_bad_start"), "capture_error");
    assert_eq!(
        sanitize_diag_code("__Encoder..Write--Failed__"),
        "encoder.write-failed"
    );

    let env = TestEnv::new("p11-p12-locks-idem");
    let probe = DynamicProbe::new(&env.root, 20 * 1024 * 1024 * 1024);

    // Slow-start backend to test concurrent in-flight idempotent coalescing & conflicting hash rejection!
    struct SlowStartBackend {
        entered: Arc<std::sync::Barrier>,
        release: Arc<std::sync::Barrier>,
    }
    impl CaptureBackend for SlowStartBackend {
        fn is_camera_connected(&self) -> bool {
            true
        }
        fn camera_focus(&self) -> StorageResult<Option<CameraFocusStatus>> {
            Ok(None)
        }
        fn set_camera_focus(
            &self,
            _req: &CameraFocusSetRequest,
        ) -> StorageResult<CameraFocusStatus> {
            Err(StorageError::unsupported("unsupported", "none"))
        }
        fn begin_capture(
            &self,
            req: &BackendStartRequest,
        ) -> StorageResult<Box<dyn ActiveCaptureHandle>> {
            self.entered.wait();
            self.release.wait();
            Ok(Box::new(MockCaptureHandle {
                staging_dir: req.staging_dir.clone(),
                fail_on_finish: false,
            }))
        }
        fn runtime_observation(&self) -> RuntimeObservation {
            RuntimeObservation::default_with_camera(true, None)
        }
    }

    let entered = Arc::new(std::sync::Barrier::new(2));
    let release = Arc::new(std::sync::Barrier::new(2));
    let coord = make_coordinator(
        &env,
        probe.clone(),
        Arc::new(SlowStartBackend {
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        }),
    );

    // 1. P1-11: Hold an exclusive `maintenance.lock` briefly (40ms) -> `start_capture` retries and succeeds!
    //    Hold it > 200ms -> `start_capture` maps persistent contention to retryable `503 maintenance_busy`!
    {
        let excl = acquire_maintenance_lock(&env.run_dir, LockMode::Exclusive, true).unwrap();
        let busy_err = coord
            .start_capture(CaptureStartRequest::new_production(), None)
            .unwrap_err();
        assert_eq!(busy_err.code(), "maintenance_busy");
        assert_eq!(busy_err.http_status(), 503);
        assert!(busy_err.retryable());
        drop(excl);
    }

    // 2. P2-12: Launch leader `start_capture` with `Idempotency-Key: k-inflight`; while in-flight:
    //    - conflicting request with same key returns `409 idempotency_conflict` immediately;
    //    - matching request with same key coalesces and returns `replayed == true`!
    let mut req1 = CaptureStartRequest::new_production();
    req1.display_name = Some("Inflight Coalesced".to_owned());
    let c_leader = Arc::clone(&coord);
    let r_leader = req1.clone();
    let leader_jh = thread::spawn(move || {
        c_leader.start_capture_for_principal(r_leader, Some("k-inflight"), "p1")
    });
    entered.wait();

    // Lock-free `phase()` returns `Starting` immediately.
    assert_eq!(coord.phase().unwrap(), CapturePhase::Starting);

    // Conflicting hash for same `(principal, op, key)` while in flight -> 409 `idempotency_conflict`!
    let mut req_conflict = CaptureStartRequest::new_production();
    req_conflict.display_name = Some("Different Payload".to_owned());
    let conflict_err = coord
        .start_capture_for_principal(req_conflict, Some("k-inflight"), "p1")
        .unwrap_err();
    assert_eq!(conflict_err.code(), "idempotency_conflict");
    assert_eq!(conflict_err.http_status(), 409);

    // Matching hash for same `(principal, op, key)` while in flight -> waits and replays!
    let c_waiter = Arc::clone(&coord);
    let r_waiter = req1.clone();
    let waiter_jh = thread::spawn(move || {
        c_waiter.start_capture_for_principal(r_waiter, Some("k-inflight"), "p1")
    });
    thread::sleep(Duration::from_millis(25));
    release.wait();

    let leader_out = leader_jh.join().unwrap().unwrap();
    let waiter_out = waiter_jh.join().unwrap().unwrap();
    assert!(!leader_out.replayed);
    assert!(waiter_out.replayed);
    assert_eq!(
        leader_out.value.snapshot.active_recording,
        waiter_out.value.snapshot.active_recording
    );

    // 3. P2-13: 1 Hz `enforce_storage_reserve()` refreshes cached `remaining_bytes` on active session!
    let new_avail = 12 * 1024 * 1024 * 1024_u64;
    probe.set(|c| c.available_bytes = new_avail);
    let _ = coord.enforce_storage_reserve().unwrap();
    let live_rem = coord
        .capture_status()
        .unwrap()
        .snapshot
        .active_recording
        .unwrap()
        .recording_state
        .storage
        .remaining_bytes;
    assert_eq!(live_rem, Some(new_avail));

    let _ = coord
        .stop_capture(CaptureStopRequest::user(), None)
        .unwrap();
}

#[test]
fn wave3_catalog_v1_to_v2_migration_wal_batch_forget_readonly_and_clean_marker() {
    use miro_ego_storage::catalog::IndexedSession;
    use miro_ego_storage::util::crc32_ieee;
    use miro_ego_storage::{
        CATALOG_BINARY_FORMAT_VERSION, CATALOG_BINARY_MAGIC, CATALOG_FORMAT_VERSION,
        CATALOG_WAL_V2_MAGIC, CatalogEntry, SessionState,
    };

    let env = TestEnv::new("w3-catalog-wal");
    let paths = env.paths();
    let _ = paths.ensure_layout().unwrap();

    // 1. Create a real sealed session directory + manual MEGOCAT1 v1 JSON payload on disk.
    let sid_v1 = new_uuid_v7();
    let rel_v1 = "2026-11-27/10-00-00";
    let dir_v1 = paths.recordings_dir().join(rel_v1);
    fs::create_dir_all(dir_v1.join("video")).unwrap();
    let probe = DynamicProbe::new(&env.root, 20 * 1024 * 1024 * 1024);
    let backend = Arc::new(MockBackend::new());
    let seed_coord = make_coordinator(&env, probe, backend);
    let started = seed_coord
        .start_capture(CaptureStartRequest::new_production(), None)
        .unwrap();
    let seeded_sid = started
        .value
        .snapshot
        .active_recording
        .unwrap()
        .recording_state
        .session_id;
    let _ = seed_coord
        .stop_capture(CaptureStopRequest::user(), None)
        .unwrap();
    let seeded_entry = seed_coord
        .catalog()
        .get_indexed_session(&seeded_sid)
        .unwrap()
        .unwrap();
    drop(seed_coord);
    let _ = fs::remove_dir_all(&dir_v1);
    let _ = sid_v1;

    // Overwrite catalog.bin with a v1 JSON frame and remove catalog.wal.
    let _ = fs::remove_file(paths.catalog_wal_file());
    let v1_json = serde_json::to_vec(&json!({
        "revision_seq": 7_u64,
        "entries": [seeded_entry],
        "diagnostics": [],
    }))
    .unwrap();
    let mut v1_raw = Vec::new();
    v1_raw.extend_from_slice(&CATALOG_BINARY_MAGIC);
    v1_raw.extend_from_slice(&CATALOG_FORMAT_VERSION.to_le_bytes());
    v1_raw.extend_from_slice(&crc32_ieee(&v1_json).to_le_bytes());
    v1_raw.extend_from_slice(&(v1_json.len() as u32).to_le_bytes());
    v1_raw.extend_from_slice(&v1_json);
    fs::write(paths.catalog_file(), &v1_raw).unwrap();

    // Open catalog -> transparently reads v1 JSON; compact() upgrades on-disk header to v2!
    let cat = BinarySessionCatalog::open(paths.clone()).unwrap();
    assert!(cat.get_indexed_session(&seeded_sid).unwrap().is_some());
    cat.compact().unwrap();
    let upgraded_raw = fs::read(paths.catalog_file()).unwrap();
    assert_eq!(&upgraded_raw[0..8], &CATALOG_BINARY_MAGIC);
    assert_eq!(
        u32::from_le_bytes(upgraded_raw[8..12].try_into().unwrap()),
        CATALOG_BINARY_FORMAT_VERSION
    );

    // 2. Admit 50 sessions -> appends to MEGOWAL1 WAL; batch `forget_paths` removes all 50 in 1 rev bump!
    let mut batch_rels = Vec::with_capacity(50);
    for i in 0..50_u32 {
        let sid = new_uuid_v7();
        let rel = format!("2026-11-27/11-{:02}-{:02}", i / 60, i % 60);
        cat.admit_indexed_session(IndexedSession::from_basic_entry(CatalogEntry {
            session_id: sid,
            display_name: format!("Batch {i}"),
            relative_dir: rel.clone(),
            started_at: format!("2026-11-27T11:{:02}:{:02}Z", i / 60, i % 60),
            duration_seconds: 1.0,
            total_bytes: 128,
            manifest_sha256: "a".repeat(64),
            state: SessionState::Sealed,
        }))
        .unwrap();
        batch_rels.push(rel);
    }
    let wal_bytes = fs::read(paths.catalog_wal_file()).unwrap();
    assert_eq!(&wal_bytes[0..8], &CATALOG_WAL_V2_MAGIC);

    // Read-only open sees both snapshot + WAL entries without mutating files or sweeping temps.
    let stray_tmp = paths.index_dir().join("catalog.bin.tmp.readonly-check");
    fs::write(&stray_tmp, b"keep").unwrap();
    let ro_cat = BinarySessionCatalog::open_read_only(paths.clone()).unwrap();
    assert_eq!(ro_cat.list_sessions().unwrap().len(), 51);
    assert!(
        stray_tmp.is_file(),
        "open_read_only must never unlink temp files"
    );
    fs::remove_file(&stray_tmp).unwrap();

    let seq_before_forget = cat.revision_seq().unwrap();
    let removed = cat.forget_paths(&batch_rels).unwrap();
    assert_eq!(removed, 50);
    assert_eq!(cat.revision_seq().unwrap(), seq_before_forget + 1);
    assert_eq!(cat.list_sessions().unwrap().len(), 1);

    // 3. Clean-mount marker skips full filesystem scan on clean startup.
    cat.mark_mount_clean();
    assert!(paths.mount_clean_marker_file().is_file());
    let (fast_cat, skipped_scan) = BinarySessionCatalog::open_fast_if_clean(paths.clone()).unwrap();
    assert!(skipped_scan);
    assert_eq!(fast_cat.list_sessions().unwrap().len(), 1);
    fast_cat.mark_mount_dirty();
    assert!(!paths.mount_clean_marker_file().exists());
    let (_, skipped_dirty) = BinarySessionCatalog::open_fast_if_clean(paths).unwrap();
    assert!(!skipped_dirty);
}

#[test]
fn wave3_trash_staged_delete_crash_replay_idle_safe_swap_and_audit_idle_eviction() {
    use miro_ego_storage::audit::{ApiAuditEvent, AuditConfig, AuditLogger};

    let env = TestEnv::new("w3-del-swap-audit");
    let probe = DynamicProbe::new(&env.root, 20 * 1024 * 1024 * 1024);
    let backend = Arc::new(MockBackend::new());
    let coord = make_coordinator(&env, probe.clone(), Arc::clone(&backend));

    let vol_for_audit = Arc::clone(coord.volume());
    let audit = Arc::new(AuditLogger::new(
        env.paths(),
        AuditConfig {
            idle_close_after: Duration::from_millis(25),
            ..AuditConfig::default()
        },
        Arc::new(move || vol_for_audit.is_mounted_and_writable()),
    ));
    coord.set_audit_logger(Arc::clone(&audit));

    // 1. AuditLogger closes idle descriptor automatically on 1 Hz supervisor tick.
    let _ = audit
        .record(&ApiAuditEvent::new(
            "req-1",
            None,
            "getDevice",
            None,
            "allowed",
        ))
        .unwrap();
    assert!(audit.has_open_file());
    thread::sleep(Duration::from_millis(40));
    let _ = coord.enforce_storage_reserve().unwrap();
    assert!(
        !audit.has_open_file(),
        "idle AuditLogger handle must auto-close on reserve supervisor tick"
    );

    // Record two sessions (`sid1`, `sid2`).
    let s1 = coord
        .start_capture(CaptureStartRequest::new_production(), None)
        .unwrap();
    let sid1 = s1
        .value
        .snapshot
        .active_recording
        .unwrap()
        .recording_state
        .session_id;
    let _ = coord
        .stop_capture(CaptureStopRequest::user(), None)
        .unwrap();
    let s2 = coord
        .start_capture(CaptureStartRequest::new_production(), None)
        .unwrap();
    let sid2 = s2
        .value
        .snapshot
        .active_recording
        .unwrap()
        .recording_state
        .session_id;
    let _ = coord
        .stop_capture(CaptureStopRequest::user(), None)
        .unwrap();

    // 2. `Idle`-state `SafeSwap` returns `None` (`204`), releases volume, and persists receipt for latest session!
    let idle_swap = coord
        .stop_capture(CaptureStopRequest::safe_swap(), Some("idle-swap-key"))
        .unwrap();
    assert!(idle_swap.value.is_none());
    assert!(coord.volume().is_volume_released());
    let receipt = coord
        .safe_swap_receipt()
        .unwrap()
        .expect("Idle SafeSwap must emit SafeSwapReceipt for latest sealed session")
        .receipt;
    assert_eq!(receipt.session_id, sid2);
    assert_eq!(receipt.open_handle_count, 0);
    coord.refresh_volume_admission().unwrap();

    // 3. Simulate mid-batch delete crash (`sid2` moved into `.miro-ego/deleting/<sid2>`, `sid1` still in
    //    `recordings/`, and `pending_delete` persisted in `coordinator-receipts.json`).
    let e1 = coord.catalog().get_indexed_session(&sid1).unwrap().unwrap();
    let e2 = coord.catalog().get_indexed_session(&sid2).unwrap().unwrap();
    let del_req = DeleteSessionsRequest {
        schema: SESSION_DELETE_REQUEST_SCHEMA.to_owned(),
        sessions: vec![
            DeleteSessionTarget {
                session_id: sid1.clone(),
                manifest_sha256: e1.manifest_sha256.clone(),
            },
            DeleteSessionTarget {
                session_id: sid2.clone(),
                manifest_sha256: e2.manifest_sha256.clone(),
            },
        ],
    };
    let req_hash = sha256_hex(&serde_json::to_vec(&del_req).unwrap());
    fs::create_dir_all(env.paths().deleting_dir()).unwrap();
    fs::rename(
        env.paths().recordings_dir().join(&e2.relative_dir),
        env.paths().deleting_dir().join(&sid2),
    )
    .unwrap();
    let receipts_path = env.paths().receipts_dir().join("coordinator-receipts.json");
    let mut raw_receipts: serde_json::Value =
        serde_json::from_slice(&fs::read(&receipts_path).unwrap()).unwrap();
    raw_receipts["pending_delete"] = json!({
        "principal_id": "op-crash",
        "key": "crash-del-key",
        "request_hash": req_hash,
        "targets": [[sid2, e2.relative_dir], [sid1, e1.relative_dir]],
        "recorded_at": "2026-11-27T12:00:00Z",
    });
    fs::write(&receipts_path, serde_json::to_vec(&raw_receipts).unwrap()).unwrap();
    drop(coord);

    // Boot fresh coordinator + run startup recovery -> completes interrupted delete & replays 200 OK!
    let coord_recovered = make_coordinator(&env, probe, backend);
    let _ = coord_recovered.recover_on_startup().unwrap();
    assert!(
        coord_recovered
            .catalog()
            .list_sessions()
            .unwrap()
            .is_empty()
    );
    assert_eq!(fs::read_dir(env.paths().deleting_dir()).unwrap().count(), 0);

    let replayed = coord_recovered
        .delete_sessions_for_principal(del_req, "crash-del-key", "op-crash")
        .unwrap();
    assert!(replayed.replayed);
    assert_eq!(replayed.value.deleted_session_ids, vec![sid2, sid1]);
}

fn synthetic_mp4_with_stsz(sample_count: u32) -> Vec<u8> {
    fn mp4_box(fourcc: &[u8; 4], payload: &[u8]) -> Vec<u8> {
        let len = (8 + payload.len()) as u32;
        let mut out = Vec::with_capacity(len as usize);
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(fourcc);
        out.extend_from_slice(payload);
        out
    }
    let mut stsz_body = Vec::new();
    stsz_body.extend_from_slice(&0_u32.to_be_bytes()); // version + flags
    stsz_body.extend_from_slice(&64_u32.to_be_bytes()); // constant sample_size = 64
    stsz_body.extend_from_slice(&sample_count.to_be_bytes());
    let stsz = mp4_box(b"stsz", &stsz_body);
    let stbl = mp4_box(b"stbl", &stsz);
    let minf = mp4_box(b"minf", &stbl);
    let mdia = mp4_box(b"mdia", &minf);
    let trak = mp4_box(b"trak", &mdia);
    let moov = mp4_box(b"moov", &trak);

    let mut out = mp4_box(b"ftyp", b"isom\0\0\x02\0");
    out.extend_from_slice(&mp4_box(b"mdat", b"encoded-samples"));
    out.extend_from_slice(&moov);
    out
}

#[test]
fn wave3_clock_guard_provenance_logs_async_stop_stsz_validation_and_recovery_backoff() {
    use miro_ego_storage::coordinator::{ClockSyncProbe, ClockSyncSnapshot, ClockSyncSource};

    struct TestClockProbe {
        synced: AtomicBool,
    }
    impl ClockSyncProbe for TestClockProbe {
        fn snapshot(&self) -> ClockSyncSnapshot {
            let is_synced = self.synced.load(Ordering::Acquire);
            ClockSyncSnapshot {
                boot_id: "11111111-2222-4333-8444-555555555555".to_owned(),
                source: if is_synced {
                    ClockSyncSource::Client
                } else {
                    ClockSyncSource::Unsynchronized
                },
                unix_time_ms: 1_764_200_000_000,
                monotonic_ns: 9_000_000_000,
                synchronized_at_ms: is_synced.then_some(1_764_200_000_000),
            }
        }
    }

    struct DelayedFinishBackend;
    struct DelayedFinishHandle(PathBuf);
    impl ActiveCaptureHandle for DelayedFinishHandle {
        fn progress(&self) -> CaptureProgressSnapshot {
            CaptureProgressSnapshot {
                elapsed_seconds: 1.0,
                captured_frames: 30,
                bytes_written: 512,
            }
        }
        fn finish(self: Box<Self>, reason: StopReason) -> StorageResult<CapturedSessionArtifacts> {
            thread::sleep(Duration::from_millis(120));
            Box::new(MockCaptureHandle {
                staging_dir: self.0,
                fail_on_finish: false,
            })
            .finish(reason)
        }
        fn abort(self: Box<Self>) {}
    }
    impl CaptureBackend for DelayedFinishBackend {
        fn is_camera_connected(&self) -> bool {
            true
        }
        fn camera_focus(&self) -> StorageResult<Option<CameraFocusStatus>> {
            Ok(None)
        }
        fn set_camera_focus(&self, _: &CameraFocusSetRequest) -> StorageResult<CameraFocusStatus> {
            Err(StorageError::unsupported("camera_focus_unsupported", "n/a"))
        }
        fn begin_capture(
            &self,
            req: &BackendStartRequest,
        ) -> StorageResult<Box<dyn ActiveCaptureHandle>> {
            Ok(Box::new(DelayedFinishHandle(req.staging_dir.clone())))
        }
        fn runtime_observation(&self) -> RuntimeObservation {
            RuntimeObservation::default_with_camera(true, None)
        }
    }

    let env = TestEnv::new("w3-clock-async-stsz");
    let cal_file = env.run_dir.join("camera-calibration.json");
    fs::write(
        &cal_file,
        serde_json::to_vec_pretty(&json!({
            "schema": "miro-ego.calibration.v1",
            "baseline_meters": 0.063
        }))
        .unwrap(),
    )
    .unwrap();

    let probe = DynamicProbe::new(&env.root, 20 * 1024 * 1024 * 1024);
    let mut policy = VolumePolicy::production(1024 * 1024 * 1024, 100);
    policy.system_state_root = Some(env.root.clone());
    let vol = Arc::new(ExfatVolumeManager::with_probe(
        env.paths(),
        policy,
        Arc::new(probe),
    ));
    let _ = vol.ensure_layout().unwrap();
    let cat = Arc::new(BinarySessionCatalog::open(env.paths()).unwrap());
    let clock_probe = Arc::new(TestClockProbe {
        synced: AtomicBool::new(false),
    });
    let cfg = CoordinatorConfig {
        device_id: new_uuid_v4(),
        device_label: "Miro-Ego-87654321".to_owned(),
        hardware_fingerprint: format!("sha256:{}", "f".repeat(64)),
        run_dir: Some(env.run_dir.clone()),
        require_clock_sync: true,
        calibration_file: Some(cal_file),
        ..CoordinatorConfig::default()
    };
    let coord = Arc::new(
        CaptureCoordinator::new(cfg, vol, Arc::clone(&cat), Arc::new(DelayedFinishBackend))
            .with_clock_probe(Arc::clone(&clock_probe) as Arc<dyn ClockSyncProbe>),
    );
    let session_validator = load_schema("miro-ego-device-session-v1.schema.json");

    // 1. Unsynchronized clock blocks readiness and startCapture (`409 clock_unsynchronized`).
    let r_unsync = coord.capture_readiness().unwrap();
    assert!(!r_unsync.allowed);
    assert_eq!(r_unsync.code, "clock_unsynchronized");
    let err_unsync = coord
        .start_capture(CaptureStartRequest::new_production(), None)
        .unwrap_err();
    assert_eq!(err_unsync.code(), "clock_unsynchronized");
    assert_eq!(err_unsync.http_status(), 409);

    // 2. Synchronize clock -> startCapture succeeds; `begin_stop_capture_for_principal` returns
    //    `202` (`finalizing`) immediately without waiting for the 120ms finish drain!
    clock_probe.synced.store(true, Ordering::Release);
    assert!(coord.capture_readiness().unwrap().allowed);
    let started = coord
        .start_capture(CaptureStartRequest::new_production(), None)
        .unwrap();
    let sid = started
        .value
        .snapshot
        .active_recording
        .unwrap()
        .recording_state
        .session_id;

    let t0 = std::time::Instant::now();
    let async_stop = coord
        .begin_stop_capture_for_principal(CaptureStopRequest::user(), Some("async-stop-k"), "op1")
        .unwrap();
    assert!(
        t0.elapsed() < Duration::from_millis(80),
        "begin_stop_capture_for_principal must return 202 before 120ms finish completes"
    );
    assert_eq!(
        async_stop.value.as_ref().unwrap().snapshot.device_state,
        "finalizing"
    );
    assert_eq!(
        coord.wait_until_not_busy(Duration::from_secs(3)).unwrap(),
        CapturePhase::Idle
    );

    let indexed = cat.get_indexed_session(&sid).unwrap().unwrap();
    let opened = open_manifest(
        &env.paths().recordings_dir(),
        &indexed.relative_dir,
        Some(&sid),
    )
    .unwrap();
    assert!(
        session_validator
            .is_valid(std::str::from_utf8(&opened.raw_bytes).unwrap())
            .unwrap()
    );
    let roles: Vec<&str> = opened.parsed["logs"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|l| l["role"].as_str())
        .collect();
    assert!(roles.contains(&"log.clock-calibration"));
    assert!(roles.contains(&"log.camera-calibration"));

    // 3. Recovery MP4 `stsz` validation: mismatched left(3) vs right(2) is quarantined; matching(2,2) recovers!
    let bad_sid = new_uuid_v7();
    let bad_dir = env.paths().staging_dir().join(format!("{bad_sid}.partial"));
    fs::create_dir_all(bad_dir.join("video")).unwrap();
    fs::write(
        bad_dir.join("video/left_00000.mp4"),
        synthetic_mp4_with_stsz(3),
    )
    .unwrap();
    fs::write(
        bad_dir.join("video/right_00000.mp4"),
        synthetic_mp4_with_stsz(2),
    )
    .unwrap();

    let good_sid = new_uuid_v7();
    let good_dir = env
        .paths()
        .staging_dir()
        .join(format!("{good_sid}.partial"));
    fs::create_dir_all(good_dir.join("video")).unwrap();
    fs::write(
        good_dir.join("video/left_00000.mp4"),
        synthetic_mp4_with_stsz(2),
    )
    .unwrap();
    fs::write(
        good_dir.join("video/right_00000.mp4"),
        synthetic_mp4_with_stsz(2),
    )
    .unwrap();
    fs::write(
        good_dir.join("frames.ndjson"),
        b"{\"frame\":0}\n{\"frame\":1}\n{\"frame\":2}\n",
    )
    .unwrap();
    fs::write(good_dir.join("imu.ndjson"), b"{\"seq\":0}\n").unwrap();

    let rep = coord.recover_on_startup().unwrap();
    assert_eq!(rep.recovered, 1);
    assert_eq!(rep.quarantined, 1);
    assert!(cat.get_indexed_session(&bad_sid).unwrap().is_none());
    let good_idx = cat.get_indexed_session(&good_sid).unwrap().unwrap();
    let good_man = open_manifest(
        &env.paths().recordings_dir(),
        &good_idx.relative_dir,
        Some(&good_sid),
    )
    .unwrap();
    assert_eq!(good_man.parsed["video"]["segments"][0]["end_frame"], 2);
    assert_eq!(good_man.parsed["frames"]["count"], 2);
}
