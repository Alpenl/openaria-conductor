use miro_ego_capture::session::schema_validation::SchemaValidator;
use miro_ego_core::ids::{new_uuid_v4, new_uuid_v7};
use miro_ego_core::paths::DataVolumePaths;
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
    out.extend_from_slice(b"mvhd0000");
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

fn make_coordinator(
    env: &TestEnv,
    probe: DynamicProbe,
    backend: Arc<MockBackend>,
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

    // Finish failure -> returns error, restores Idle, stores retained_unsuccessful for sid2
    // while preserving historical sid1 resource too!
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
    assert!(
        coord
            .stop_capture(CaptureStopRequest::user(), None)
            .is_err()
    );
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
