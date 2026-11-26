//! Crash prefix recovery, quarantine isolation, and collision-safe atomic publication
//! for interrupted captures under `/data/.miro-ego/staging/`.

use crate::catalog::{BinarySessionCatalog, DEVICE_SESSION_SCHEMA};
use crate::util::{
    UNKNOWN_DATE_DIR_NAME, now_rfc3339_utc, parse_rfc3339_utc_nanos,
    readable_date_time_from_rfc3339, sha256_hex, sweep_stale_atomic_temps,
};
use crate::{RecoveryManager, RecoveryReport, StorageError, StorageResult};
use miro_ego_capture::session::session_io::hash_file;
use miro_ego_core::config::{RecordingEncoding, VideoCodec};
use miro_ego_core::ids::{is_valid_uuid_v4, is_valid_uuid_v7, new_uuid_v4, new_uuid_v7};
use miro_ego_core::paths::DataVolumePaths;
use miro_ego_core::sys::fs::{
    fstat, fsync_directory, open_directory_safe, open_readonly_safe, pread_retry,
    read_bounded_json_value, renameat2_noreplace, write_bytes_atomic,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::ffi::CString;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const RECOVERY_CONTROL_FILES: [&str; 3] = ["recording.json", "capture.json", "checkpoint.json"];
const MAX_CONTROL_JSON_BYTES: usize = 1024 * 1024;
const MAX_COLLISION_ORDINALS: u32 = 9_999;

/// Device and build defaults used when reconstructing a manifest from an interrupted partial dir.
#[derive(Debug, Clone)]
pub struct RecoveryDefaults {
    pub device_id: String,
    pub device_label: String,
    pub hardware_fingerprint: String,
    pub platform: String,
    pub software_version: String,
    pub commit: String,
    pub timezone: String,
    pub camera_width: u32,
    pub camera_height: u32,
    pub sensor_fps: f64,
    pub frame_decimation: u32,
    pub segment_frames: u64,
    pub encoding: RecordingEncoding,
}

impl Default for RecoveryDefaults {
    fn default() -> Self {
        Self {
            device_id: new_uuid_v4(),
            device_label: "Miro-Ego-00000000".to_owned(),
            hardware_fingerprint: format!("sha256:{}", "0".repeat(64)),
            platform: "rdk-x5".to_owned(),
            software_version: env!("CARGO_PKG_VERSION").to_owned(),
            commit: "0".repeat(40),
            timezone: "Etc/UTC".to_owned(),
            camera_width: 3840,
            camera_height: 1080,
            sensor_fps: 60.0,
            frame_decimation: 2,
            segment_frames: 150,
            encoding: RecordingEncoding::default(),
        }
    }
}

/// Outcome for a single interrupted `*.partial` directory processed by recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum RecoveryItemOutcome {
    Recovered {
        partial_name: String,
        session_id: String,
        relative_dir: String,
        recovered_segments: usize,
        discarded_tail_files: usize,
    },
    Quarantined {
        partial_name: String,
        quarantine_name: String,
        reason: String,
    },
    Deferred {
        partial_name: String,
        reason: String,
    },
}

/// Rich recovery report detailing every scanned staging entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DetailedRecoveryReport {
    pub summary: RecoveryReport,
    pub items: Vec<RecoveryItemOutcome>,
}

/// Scans `/data/.miro-ego/staging/*.partial`, recovers contiguous verified stereo prefixes into
/// `/data/recordings/YYYY-MM-DD/HH-MM-SS/`, and isolates unrecoverable tails or broken captures
/// into `/data/.miro-ego/quarantine/`.
pub struct StagingRecoveryManager {
    paths: DataVolumePaths,
    defaults: RecoveryDefaults,
    catalog: Option<Arc<BinarySessionCatalog>>,
}

impl StagingRecoveryManager {
    pub fn new(
        paths: DataVolumePaths,
        defaults: RecoveryDefaults,
        catalog: Option<Arc<BinarySessionCatalog>>,
    ) -> Self {
        Self {
            paths,
            defaults,
            catalog,
        }
    }

    pub fn paths(&self) -> &DataVolumePaths {
        &self.paths
    }

    /// Run crash prefix recovery over `/data/.miro-ego/staging/`, returning detailed outcomes.
    pub fn recover_staging_detailed(&self) -> StorageResult<DetailedRecoveryReport> {
        for sweep_dir in [
            self.paths.hidden_meta_dir(),
            self.paths.index_dir(),
            self.paths.receipts_dir(),
        ] {
            let _ = sweep_stale_atomic_temps(&sweep_dir);
        }

        let staging_dir = self.paths.staging_dir();
        if !staging_dir.is_dir() {
            return Ok(DetailedRecoveryReport {
                summary: RecoveryReport {
                    scanned: 0,
                    recovered: 0,
                    quarantined: 0,
                },
                items: Vec::new(),
            });
        }

        let staging_fd =
            open_directory_safe(&staging_dir).map_err(|err| StorageError::Unavailable {
                message: format!("unable to open staging directory: {err}"),
            })?;
        let staging_dev = fstat(staging_fd.as_raw_fd())
            .map_err(|err| StorageError::io("staging_stat_failed", "fstat staging dir", err))?
            .st_dev;

        let committed_terminal_unsuccessful = load_committed_terminal_session_ids(&self.paths);

        let mut partial_names: Vec<String> = fs::read_dir(&staging_dir)
            .map_err(|err| StorageError::io("staging_readdir_failed", "read staging dir", err))?
            .flatten()
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|name| name.ends_with(".partial") && !name.starts_with('.'))
            .collect();
        partial_names.sort();

        let mut report = RecoveryReport {
            scanned: 0,
            recovered: 0,
            quarantined: 0,
        };
        let mut items = Vec::new();

        for name in partial_names {
            let partial_path = staging_dir.join(&name);
            let Ok(meta) = fs::symlink_metadata(&partial_path) else {
                continue;
            };
            if meta.file_type().is_symlink() || !meta.is_dir() || meta.dev() != staging_dev {
                continue;
            }
            report.scanned += 1;

            // Never resurrect a session that was already durably committed as terminal failed/abandoned.
            if partial_matches_committed_unsuccessful(
                &partial_path,
                &name,
                &committed_terminal_unsuccessful,
            ) {
                match move_to_quarantine(&self.paths.quarantine_dir(), &partial_path, &name) {
                    Ok(q_name) => {
                        report.quarantined += 1;
                        items.push(RecoveryItemOutcome::Quarantined {
                            partial_name: name,
                            quarantine_name: q_name,
                            reason: "session already committed as terminal unsuccessful outcome"
                                .to_owned(),
                        });
                    }
                    Err(err) if is_transient_storage_error(&err) => {
                        items.push(RecoveryItemOutcome::Deferred {
                            partial_name: name,
                            reason: err.to_string(),
                        });
                    }
                    Err(err) => return Err(err),
                }
                continue;
            }

            match self.recover_single_partial(&partial_path, &name) {
                Ok(item @ RecoveryItemOutcome::Recovered { .. }) => {
                    report.recovered += 1;
                    items.push(item);
                }
                Ok(item @ RecoveryItemOutcome::Quarantined { .. }) => {
                    report.quarantined += 1;
                    items.push(item);
                }
                Ok(item @ RecoveryItemOutcome::Deferred { .. }) => {
                    items.push(item);
                }
                Err(err) if is_transient_storage_error(&err) => {
                    // Preserve recoverable `.partial` in `staging/` on ENOSPC/EROFS/EIO!
                    items.push(RecoveryItemOutcome::Deferred {
                        partial_name: name,
                        reason: err.to_string(),
                    });
                }
                Err(err) => {
                    if partial_path.exists() {
                        match move_to_quarantine(&self.paths.quarantine_dir(), &partial_path, &name)
                        {
                            Ok(q_name) => {
                                report.quarantined += 1;
                                items.push(RecoveryItemOutcome::Quarantined {
                                    partial_name: name,
                                    quarantine_name: q_name,
                                    reason: err.to_string(),
                                });
                            }
                            Err(q_err) if is_transient_storage_error(&q_err) => {
                                items.push(RecoveryItemOutcome::Deferred {
                                    partial_name: name,
                                    reason: q_err.to_string(),
                                });
                            }
                            Err(q_err) => return Err(q_err),
                        }
                    } else {
                        return Err(err);
                    }
                }
            }
        }

        Ok(DetailedRecoveryReport {
            summary: report,
            items,
        })
    }

    fn recover_single_partial(
        &self,
        partial_path: &Path,
        partial_name: &str,
    ) -> StorageResult<RecoveryItemOutcome> {
        let video_dir = partial_path.join("video");
        let valid_pairs = find_contiguous_valid_stereo_pairs(&video_dir)?;

        if valid_pairs.is_empty() {
            let q_name =
                move_to_quarantine(&self.paths.quarantine_dir(), partial_path, partial_name)?;
            return Ok(RecoveryItemOutcome::Quarantined {
                partial_name: partial_name.to_owned(),
                quarantine_name: q_name,
                reason: "no contiguous verified stereo video segments found".to_owned(),
            });
        }

        let frames_path = partial_path.join("frames.ndjson");
        let imu_path = partial_path.join("imu.ndjson");
        let frames_count = sanitize_ndjson_file(&frames_path)?;
        let imu_count = sanitize_ndjson_file(&imu_path)?;
        let frames_digest = hash_file(&frames_path).map_err(|err| {
            StorageError::integrity(
                "recovery_hash_failed",
                format!("frames.ndjson: {}", err.message),
            )
        })?;
        let imu_digest = hash_file(&imu_path).map_err(|err| {
            StorageError::integrity(
                "recovery_hash_failed",
                format!("imu.ndjson: {}", err.message),
            )
        })?;

        let meta = load_partial_metadata(partial_path, &self.paths, &self.defaults);
        let segment_frames = meta.segment_frames.max(1);
        let effective_fps = (meta.sensor_fps / f64::from(meta.frame_decimation.max(1))).max(1.0);
        let segment_secs = segment_frames as f64 / effective_fps;

        let mut video_segments_json = Vec::with_capacity(valid_pairs.len());
        for (idx, pair) in valid_pairs.iter().enumerate() {
            let left_rel = format!("video/{}", pair.left_name);
            let right_rel = format!("video/{}", pair.right_name);
            let left_digest = hash_file(&video_dir.join(&pair.left_name)).map_err(|err| {
                StorageError::integrity(
                    "recovery_hash_failed",
                    format!("{left_rel}: {}", err.message),
                )
            })?;
            let right_digest = hash_file(&video_dir.join(&pair.right_name)).map_err(|err| {
                StorageError::integrity(
                    "recovery_hash_failed",
                    format!("{right_rel}: {}", err.message),
                )
            })?;
            let start_frame = (idx as u64) * segment_frames;
            let end_frame = start_frame + segment_frames;
            let start_t = (idx as f64) * segment_secs;
            let end_t = ((idx + 1) as f64) * segment_secs;
            video_segments_json.push(json!({
                "index": idx as u64,
                "start_frame": start_frame,
                "end_frame": end_frame,
                "start_time_seconds": start_t,
                "end_time_seconds": end_t,
                "artifacts": {
                    "left": {
                        "artifact_id": left_digest.sha256,
                        "role": "video.left",
                        "path": left_rel,
                        "media_type": "video/mp4",
                        "bytes": left_digest.identity.size,
                        "sha256": left_digest.sha256,
                    },
                    "right": {
                        "artifact_id": right_digest.sha256,
                        "role": "video.right",
                        "path": right_rel,
                        "media_type": "video/mp4",
                        "bytes": right_digest.identity.size,
                        "sha256": right_digest.sha256,
                    }
                }
            }));
        }

        let kept_video_names: HashSet<String> = valid_pairs
            .iter()
            .flat_map(|pair| [pair.left_name.clone(), pair.right_name.clone()])
            .collect();
        let planned_discarded_tail_files =
            count_unrecoverable_tail_files(partial_path, &kept_video_names)?;

        let duration_seconds = (valid_pairs.len() as f64) * segment_secs;
        let now = now_rfc3339_utc();
        let manifest_id = new_uuid_v7();
        let codec_str = meta.encoding.codec.as_str();
        let profile_str = match meta.encoding.codec {
            VideoCodec::H264 => "high",
            VideoCodec::Hevc => "main",
        };

        let manifest_value = json!({
            "schema": DEVICE_SESSION_SCHEMA,
            "manifest_id": manifest_id,
            "sealed": true,
            "sealed_at": now,
            "session_id": meta.session_id,
            "volume_id": meta.volume_id,
            "capture_mode": meta.capture_mode,
            "display_name": meta.display_name,
            "device": {
                "device_id": meta.device_id,
                "device_label": meta.device_label,
                "hardware_fingerprint": meta.hardware_fingerprint,
                "platform": meta.platform,
                "software_version": meta.software_version,
                "commit": meta.commit,
            },
            "time": {
                "started_at": meta.started_at,
                "ended_at": now,
                "timezone": meta.timezone,
                "duration_seconds": duration_seconds,
                "duration_clock": "host_monotonic",
            },
            "take": {
                "take_id": meta.take_id,
                "sequence": meta.take_sequence,
                "continuation_of": meta.continuation_of,
            },
            "camera": {
                "width": meta.camera_width,
                "height": meta.camera_height,
                "eye_width": (meta.camera_width / 2).max(1),
                "sensor_fps": meta.sensor_fps,
                "frame_decimation": meta.frame_decimation,
                "effective_fps": effective_fps,
                "coordinate_frame": "opencv_optical",
            },
            "video": {
                "layout": "split-eyes",
                "codec": codec_str,
                "container": "mp4",
                "segments": video_segments_json,
                "encoding": build_manifest_encoding_json(&meta.encoding, profile_str),
            },
            "imu": {
                "artifact": {
                    "artifact_id": imu_digest.sha256,
                    "role": "imu.samples",
                    "path": "imu.ndjson",
                    "media_type": "application/x-ndjson",
                    "bytes": imu_digest.identity.size,
                    "sha256": imu_digest.sha256,
                },
                "sample_count": imu_count,
                "units": "raw_int16",
                "coordinate_frame": "raw_device_axes",
            },
            "frames": {
                "artifact": {
                    "artifact_id": frames_digest.sha256,
                    "role": "frames.index",
                    "path": "frames.ndjson",
                    "media_type": "application/x-ndjson",
                    "bytes": frames_digest.identity.size,
                    "sha256": frames_digest.sha256,
                },
                "count": frames_count,
            },
            "audio": {
                "state": "not_recorded",
                "requested_mode": "device_default",
                "resolved_mode": "disabled",
                "reason": "recording_interrupted",
            },
            "logs": [],
            "integrity": {
                "verified_at": now,
                "dropped_frames": 0,
                "drop_events": [],
                "fatal_errors": [],
            },
            "capture_audit": {
                "schema": "miro-ego.capture-audit.v1",
                "recovery": {
                    "recovered_prefix": true,
                    "recovered_segments": valid_pairs.len(),
                    "discarded_tail_files": planned_discarded_tail_files,
                    "recovered_at": now,
                }
            }
        });

        let mut manifest_bytes =
            serde_json::to_vec_pretty(&manifest_value).map_err(|err| StorageError::Catalog {
                message: format!("serialize recovered manifest failed: {err}"),
            })?;
        manifest_bytes.push(b'\n');
        let manifest_sha256 = sha256_hex(&manifest_bytes);

        // Pre-validate manifest schema & duplicate session_id BEFORE mutating or publishing partial_path!
        let _ = crate::catalog::parse_indexed_session_from_manifest(
            "2026-01-01/00-00-00",
            &manifest_value,
            &manifest_sha256,
        )?;
        if let Some(catalog) = &self.catalog {
            if catalog.get_indexed_session(&meta.session_id)?.is_some() {
                let q_name =
                    move_to_quarantine(&self.paths.quarantine_dir(), partial_path, partial_name)?;
                return Ok(RecoveryItemOutcome::Quarantined {
                    partial_name: partial_name.to_owned(),
                    quarantine_name: q_name,
                    reason: format!("session_id {} already exists in catalog", meta.session_id),
                });
            }
        }

        let manifest_path = partial_path.join("manifest.json");
        write_bytes_atomic(&manifest_path, &manifest_bytes, 0o640, true).map_err(|err| {
            StorageError::io(
                "recovery_manifest_write",
                "write recovered manifest.json",
                err,
            )
        })?;
        let _ = fsync_directory(partial_path);

        // Quarantine any unrecoverable tail / temporary / unpaired files so the recovered
        // recording directory placed into `/data/recordings/` stays completely spotless.
        let discarded_tail_files = quarantine_unrecoverable_tail_files(
            &self.paths.quarantine_dir(),
            partial_path,
            partial_name,
            &kept_video_names,
        )?;

        let relative_dir = publish_staging_directory(&self.paths, partial_path, &meta.started_at)?;

        // --- Post-publish section: `partial_path` no longer exists; operate strictly on `final_root`! ---
        let final_root = self.paths.recordings_dir().join(&relative_dir);
        let post_publish_res: StorageResult<()> = (|| {
            remove_control_markers(&final_root)?;
            let _ = fsync_directory(&final_root);
            if let Some(catalog) = &self.catalog {
                let indexed = crate::catalog::parse_indexed_session_from_manifest(
                    &relative_dir,
                    &manifest_value,
                    &manifest_sha256,
                )?;
                catalog.admit_indexed_session(indexed)?;
            }
            Ok(())
        })();

        if let Err(post_err) = post_publish_res {
            if is_transient_storage_error(&post_err) {
                // Roll back published directory back into `staging/` so recovery can retry cleanly
                // once disk space / writability returns without split-brain.
                if fs::rename(&final_root, partial_path).is_ok() {
                    cleanup_empty_date_parent(&self.paths.recordings_dir(), &final_root);
                    return Ok(RecoveryItemOutcome::Deferred {
                        partial_name: partial_name.to_owned(),
                        reason: post_err.to_string(),
                    });
                }
                return Err(post_err);
            }

            // Non-transient post-publish failure (e.g. duplicate_session_id race or corrupt state):
            // move `final_root` (NOT `partial_path`!) into `quarantine/` and clean up date parent.
            let q_name =
                move_to_quarantine(&self.paths.quarantine_dir(), &final_root, partial_name)?;
            cleanup_empty_date_parent(&self.paths.recordings_dir(), &final_root);
            return Ok(RecoveryItemOutcome::Quarantined {
                partial_name: partial_name.to_owned(),
                quarantine_name: q_name,
                reason: post_err.to_string(),
            });
        }

        Ok(RecoveryItemOutcome::Recovered {
            partial_name: partial_name.to_owned(),
            session_id: meta.session_id,
            relative_dir,
            recovered_segments: valid_pairs.len(),
            discarded_tail_files,
        })
    }
}

impl RecoveryManager for StagingRecoveryManager {
    fn recover_staging(&self) -> StorageResult<RecoveryReport> {
        Ok(self.recover_staging_detailed()?.summary)
    }
}

/// Atomically publish a sealed staging directory into `/data/recordings/YYYY-MM-DD/HH-MM-SS[_02]/`
/// using `renameat2(RENAME_NOREPLACE)` with deterministic `_02..` collision suffixes.
pub fn publish_staging_directory(
    paths: &DataVolumePaths,
    partial_root: &Path,
    started_at: &str,
) -> StorageResult<String> {
    if !paths.hidden_meta_dir().is_dir() {
        return Err(StorageError::Unavailable {
            message: "recording volume metadata directory does not exist".to_owned(),
        });
    }
    let recordings_root = paths.recordings_dir();
    if !recordings_root.is_dir() {
        fs::create_dir_all(&recordings_root).map_err(|err| {
            StorageError::io("recordings_dir_create", "create recordings directory", err)
        })?;
    }
    let root_dir =
        open_directory_safe(&recordings_root).map_err(|err| StorageError::Unavailable {
            message: format!("recordings directory unavailable: {err}"),
        })?;
    let root_dev = fstat(root_dir.as_raw_fd())
        .map_err(|err| StorageError::io("recordings_stat", "fstat recordings directory", err))?
        .st_dev;

    let (day_name, base_name, always_suffix) = match readable_date_time_from_rfc3339(started_at) {
        Some((day, time)) => (day, time, false),
        None => (
            UNKNOWN_DATE_DIR_NAME.to_owned(),
            "recording".to_owned(),
            true,
        ),
    };

    let day_path = recordings_root.join(&day_name);
    if let Ok(meta) = fs::symlink_metadata(&day_path) {
        if meta.file_type().is_symlink() || !meta.is_dir() || meta.dev() != root_dev {
            return Err(StorageError::integrity(
                "unsafe_publish_target",
                "target date directory is a symlink or crosses filesystem boundary",
            ));
        }
    } else {
        fs::create_dir(&day_path)
            .map_err(|err| StorageError::io("date_dir_create", "create recording date dir", err))?;
        let _ = fsync_directory(&recordings_root);
    }

    let old_c = path_cstring(partial_root)?;
    for ordinal in 1..=MAX_COLLISION_ORDINALS {
        let target_name = if ordinal == 1 && !always_suffix {
            base_name.clone()
        } else {
            format!("{base_name}_{ordinal:02}")
        };
        let target_path = day_path.join(&target_name);
        let new_c = path_cstring(&target_path)?;
        match renameat2_noreplace(&old_c, &new_c) {
            Ok(()) => {
                let _ = fsync_directory(&day_path);
                let _ = fsync_directory(&recordings_root);
                return Ok(format!("{day_name}/{target_name}"));
            }
            Err(err) if matches!(err.raw_os_error(), Some(libc::EEXIST | libc::ENOTEMPTY)) => {
                continue;
            }
            Err(err) if matches!(err.raw_os_error(), Some(libc::ENOSYS | libc::EINVAL)) => {
                if fs::symlink_metadata(&target_path).is_ok() {
                    continue;
                }
                fs::rename(partial_root, &target_path).map_err(|rename_err| {
                    StorageError::io("publish_rename_failed", "publish session dir", rename_err)
                })?;
                let _ = fsync_directory(&day_path);
                let _ = fsync_directory(&recordings_root);
                return Ok(format!("{day_name}/{target_name}"));
            }
            Err(err) => {
                return Err(StorageError::io(
                    "publish_rename_failed",
                    "renameat2 publish session dir",
                    err,
                ));
            }
        }
    }

    Err(StorageError::conflict(
        "publish_collision_limit",
        "exceeded maximum collision suffixes for recording timestamp",
    ))
}

/// Remove staging control markers (`recording.json`, `capture.json`, `checkpoint.json`) after publish.
pub fn remove_control_markers(session_root: &Path) -> StorageResult<()> {
    for marker in RECOVERY_CONTROL_FILES {
        let path = session_root.join(marker);
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(StorageError::io(
                    "remove_control_marker_failed",
                    format!("remove staging control file {marker}"),
                    err,
                ));
            }
        }
    }
    Ok(())
}

/// Build the `video.encoding` JSON object conforming to `miro-ego.device-session.v1`.
pub fn build_manifest_encoding_json(encoding: &RecordingEncoding, profile: &str) -> Value {
    use miro_ego_core::config::RateControlMode;
    let (bitrate, min_qp, max_qp, vbv_ms, initial_qp) = match encoding.rate_control {
        RateControlMode::Cbr | RateControlMode::Avbr => (
            json!(encoding.bitrate_kbps),
            json!(encoding.min_qp),
            json!(encoding.max_qp),
            json!(encoding.vbv_ms),
            json!(encoding.initial_qp),
        ),
        RateControlMode::Vbr => (
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ),
        RateControlMode::Fixqp => (
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            json!(encoding.initial_qp),
        ),
    };
    json!({
        "schema": miro_ego_core::config::RECORDING_ENCODING_SCHEMA,
        "codec": encoding.codec.as_str(),
        "rate_control": encoding.rate_control.as_str(),
        "profile": profile,
        "pixel_format": "yuv420p",
        "bit_depth": 8,
        "b_frames": 0,
        "bitrate_kbps": bitrate,
        "min_qp": min_qp,
        "max_qp": max_qp,
        "intra_qp": encoding.intra_qp,
        "initial_qp": initial_qp,
        "gop_frames": encoding.gop_frames,
        "vbv_ms": vbv_ms,
    })
}

struct VerifiedStereoPair {
    left_name: String,
    right_name: String,
}

fn find_contiguous_valid_stereo_pairs(video_dir: &Path) -> StorageResult<Vec<VerifiedStereoPair>> {
    if !video_dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut pairs = Vec::new();
    for idx in 0_u64..100_000 {
        let left_name = format!("left_{idx:05}.mp4");
        let right_name = format!("right_{idx:05}.mp4");
        let left_path = video_dir.join(&left_name);
        let right_path = video_dir.join(&right_name);
        let left_ok = validate_mp4_boxes(&left_path).unwrap_or(false);
        let right_ok = validate_mp4_boxes(&right_path).unwrap_or(false);
        if !left_ok || !right_ok {
            break;
        }
        pairs.push(VerifiedStereoPair {
            left_name,
            right_name,
        });
    }
    Ok(pairs)
}

/// Fast structural ISO-BMFF (MP4) box walk verifying `ftyp` and `moov` boxes are intact
/// (distinguishes finalized stereo segments from crash-truncated tail MP4 files).
pub fn validate_mp4_boxes(path: &Path) -> StorageResult<bool> {
    let mut file = match open_readonly_safe(path) {
        Ok(f) => f,
        Err(_) => return Ok(false),
    };
    let st = fstat(file.as_raw_fd())
        .map_err(|err| StorageError::io("mp4_stat_failed", "fstat mp4 segment", err))?;
    let Ok(file_size) = u64::try_from(st.st_size) else {
        return Ok(false);
    };
    if file_size < 16 {
        return Ok(false);
    }

    let mut offset = 0_u64;
    let mut saw_ftyp = false;
    let mut saw_moov = false;
    let mut box_count = 0_usize;

    while offset < file_size {
        box_count += 1;
        if box_count > 4096 || file_size - offset < 8 {
            return Ok(false);
        }
        let mut header = [0_u8; 8];
        if read_exact_at(&mut file, offset, &mut header).is_err() {
            return Ok(false);
        }
        let size32 = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
        let box_type = &header[4..8];

        let box_size = if size32 == 0 {
            file_size - offset
        } else if size32 == 1 {
            if file_size - offset < 16 {
                return Ok(false);
            }
            let mut ext = [0_u8; 8];
            if read_exact_at(&mut file, offset + 8, &mut ext).is_err() {
                return Ok(false);
            }
            let ext_size = u64::from_be_bytes(ext);
            if ext_size < 16 {
                return Ok(false);
            }
            ext_size
        } else {
            let s = u64::from(size32);
            if s < 8 {
                return Ok(false);
            }
            s
        };

        if offset
            .checked_add(box_size)
            .is_none_or(|end| end > file_size)
        {
            return Ok(false);
        }

        if box_type == b"ftyp" {
            saw_ftyp = true;
        } else if box_type == b"moov" {
            saw_moov = true;
        }

        offset += box_size;
    }

    Ok(offset == file_size && saw_ftyp && saw_moov)
}

fn read_exact_at(file: &mut File, offset: u64, buf: &mut [u8]) -> io::Result<()> {
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(buf)
}

/// Check whether a [`StorageError`] represents a transient volume/capacity/writability fault
/// (`ENOSPC`, `EROFS`, `EIO`, `EDQUOT`, `EACCES`, or `Unavailable`) where recoverable staging
/// directories must NOT be moved into `quarantine/`.
pub fn is_transient_storage_error(err: &StorageError) -> bool {
    match err {
        StorageError::Unavailable { .. } => true,
        StorageError::Io { source, .. } => matches!(
            source.raw_os_error(),
            Some(libc::ENOSPC | libc::EROFS | libc::EIO | libc::EDQUOT | libc::EACCES)
        ),
        _ => false,
    }
}

/// Quarantine a failed or split-brain session directory under `/data/.miro-ego/quarantine/`,
/// verifying that `.miro-ego` exists on the mounted volume so `/data` is never created on ext4.
pub fn quarantine_session_directory(
    paths: &DataVolumePaths,
    src_dir: &Path,
    base_name: &str,
) -> StorageResult<String> {
    if !paths.hidden_meta_dir().is_dir() {
        return Err(StorageError::Unavailable {
            message: "recording volume metadata directory does not exist".to_owned(),
        });
    }
    move_to_quarantine(&paths.quarantine_dir(), src_dir, base_name)
}

pub(crate) fn cleanup_empty_date_parent(recordings_root: &Path, final_root: &Path) {
    if let Some(parent) = final_root.parent() {
        if parent != recordings_root {
            let _ = fs::remove_dir(parent);
            let _ = fsync_directory(recordings_root);
        }
    }
}

fn load_committed_terminal_session_ids(paths: &DataVolumePaths) -> HashSet<String> {
    let mut out = HashSet::new();
    let receipts_file = paths.receipts_dir().join("coordinator-receipts.json");
    let Ok(val) = read_bounded_json_value(&receipts_file, 1024 * 1024, 16) else {
        return out;
    };
    if let Some(items) = val.get("unsuccessful").and_then(Value::as_array) {
        for item in items {
            let rs = item.get("outcome").and_then(|o| o.get("recording_state"));
            let state = rs.and_then(|r| r.get("state")).and_then(Value::as_str);
            if matches!(state, Some("failed" | "abandoned")) {
                if let Some(sid) = rs.and_then(|r| r.get("session_id")).and_then(Value::as_str) {
                    out.insert(sid.to_owned());
                }
            }
        }
    }
    out
}

fn partial_matches_committed_unsuccessful(
    partial_path: &Path,
    partial_name: &str,
    committed_ids: &HashSet<String>,
) -> bool {
    if committed_ids.is_empty() {
        return false;
    }
    if let Some(stem) = partial_name.strip_suffix(".partial") {
        if committed_ids.contains(stem) {
            return true;
        }
    }
    for marker in ["capture.json", "recording.json"] {
        if let Ok(v) =
            read_bounded_json_value(&partial_path.join(marker), MAX_CONTROL_JSON_BYTES, 16)
        {
            if let Some(sid) = v.get("session_id").and_then(Value::as_str) {
                if committed_ids.contains(sid) {
                    return true;
                }
            }
        }
    }
    false
}

fn collect_unrecoverable_tail_files(
    partial_path: &Path,
    kept_video_names: &HashSet<String>,
) -> StorageResult<Vec<(PathBuf, String)>> {
    let mut stray_files: Vec<(PathBuf, String)> = Vec::new();

    let video_dir = partial_path.join("video");
    if video_dir.is_dir() {
        for entry in fs::read_dir(&video_dir)
            .map_err(|err| StorageError::io("recovery_readdir", "readdir staging video", err))?
            .flatten()
        {
            if let Ok(name) = entry.file_name().into_string() {
                if !kept_video_names.contains(&name) {
                    stray_files.push((entry.path(), format!("video_{name}")));
                }
            }
        }
    }

    // Move any `.tmp` or partial audio files out of the recovered session root.
    for entry in fs::read_dir(partial_path)
        .map_err(|err| StorageError::io("recovery_readdir", "readdir staging root", err))?
        .flatten()
    {
        if let Ok(name) = entry.file_name().into_string() {
            if name.contains(".tmp") || name == "audio" {
                stray_files.push((entry.path(), name));
            }
        }
    }
    Ok(stray_files)
}

fn count_unrecoverable_tail_files(
    partial_path: &Path,
    kept_video_names: &HashSet<String>,
) -> StorageResult<usize> {
    Ok(collect_unrecoverable_tail_files(partial_path, kept_video_names)?.len())
}

fn quarantine_unrecoverable_tail_files(
    quarantine_root: &Path,
    partial_path: &Path,
    partial_name: &str,
    kept_video_names: &HashSet<String>,
) -> StorageResult<usize> {
    let stray_files = collect_unrecoverable_tail_files(partial_path, kept_video_names)?;
    if stray_files.is_empty() {
        return Ok(0);
    }

    fs::create_dir_all(quarantine_root)
        .map_err(|err| StorageError::io("quarantine_dir_create", "create quarantine root", err))?;
    let tail_dir_name =
        allocate_unique_child_dir(quarantine_root, &format!("{partial_name}-tail"))?;
    let tail_dir = quarantine_root.join(tail_dir_name);

    let mut moved = 0_usize;
    for (src, flat_name) in stray_files {
        let dst = tail_dir.join(flat_name);
        fs::rename(&src, &dst).map_err(|err| {
            StorageError::io(
                "quarantine_tail_move_failed",
                format!("move unrecoverable tail file {}", src.display()),
                err,
            )
        })?;
        moved += 1;
    }
    let _ = fsync_directory(&tail_dir);
    let _ = fsync_directory(quarantine_root);
    Ok(moved)
}

fn move_to_quarantine(
    quarantine_root: &Path,
    src_dir: &Path,
    base_name: &str,
) -> StorageResult<String> {
    fs::create_dir_all(quarantine_root).map_err(|err| {
        StorageError::io("quarantine_dir_create", "create quarantine directory", err)
    })?;
    let old_c = path_cstring(src_dir)?;
    for ordinal in 1..=MAX_COLLISION_ORDINALS {
        let candidate = if ordinal == 1 {
            base_name.to_owned()
        } else {
            format!("{base_name}_{ordinal:02}")
        };
        let dst = quarantine_root.join(&candidate);
        let new_c = path_cstring(&dst)?;
        match renameat2_noreplace(&old_c, &new_c) {
            Ok(()) => {
                let _ = fsync_directory(quarantine_root);
                return Ok(candidate);
            }
            Err(err) if matches!(err.raw_os_error(), Some(libc::EEXIST | libc::ENOTEMPTY)) => {
                continue;
            }
            Err(err) if matches!(err.raw_os_error(), Some(libc::ENOSYS | libc::EINVAL)) => {
                if fs::symlink_metadata(&dst).is_ok() {
                    continue;
                }
                fs::rename(src_dir, &dst).map_err(|e| {
                    StorageError::io("quarantine_move_failed", "move partial to quarantine", e)
                })?;
                let _ = fsync_directory(quarantine_root);
                return Ok(candidate);
            }
            Err(err) => {
                return Err(StorageError::io(
                    "quarantine_move_failed",
                    "renameat2 partial to quarantine",
                    err,
                ));
            }
        }
    }
    Err(StorageError::conflict(
        "quarantine_collision_limit",
        "exceeded quarantine directory collision limit",
    ))
}

fn allocate_unique_child_dir(parent: &Path, base_name: &str) -> StorageResult<String> {
    for ordinal in 1..=MAX_COLLISION_ORDINALS {
        let candidate = if ordinal == 1 {
            base_name.to_owned()
        } else {
            format!("{base_name}_{ordinal:02}")
        };
        let path = parent.join(&candidate);
        match fs::create_dir(&path) {
            Ok(()) => return Ok(candidate),
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(err) => {
                return Err(StorageError::io(
                    "quarantine_tail_create",
                    "create quarantine tail directory",
                    err,
                ));
            }
        }
    }
    Err(StorageError::conflict(
        "quarantine_collision_limit",
        "exceeded quarantine tail collision limit",
    ))
}

/// Stream-scan an NDJSON file in bounded 64 KiB chunks and truncate in-place via `ftruncate`
/// at the last valid newline-terminated JSON line (zero 2x-RAM or 2x-disk rewrite).
pub fn sanitize_ndjson_file(path: &Path) -> StorageResult<u64> {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() || !meta.is_file() {
                return Err(StorageError::integrity(
                    "recovery_ndjson_invalid",
                    format!("ndjson target is not a regular file: {}", path.display()),
                ));
            }
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            write_bytes_atomic(path, b"", 0o640, true).map_err(|e| {
                StorageError::io("recovery_ndjson_create", "create empty ndjson file", e)
            })?;
            return Ok(0);
        }
        Err(err) => {
            return Err(StorageError::io(
                "recovery_ndjson_stat",
                "stat partial ndjson file",
                err,
            ));
        }
    }

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|err| StorageError::io("recovery_ndjson_open", "open partial ndjson file", err))?;
    let st = fstat(file.as_raw_fd())
        .map_err(|err| StorageError::io("recovery_ndjson_stat", "fstat partial ndjson", err))?;
    if (st.st_mode & libc::S_IFMT) != libc::S_IFREG {
        return Err(StorageError::integrity(
            "recovery_ndjson_invalid",
            "ndjson descriptor is not a regular file",
        ));
    }
    #[allow(clippy::useless_conversion)]
    let nlink = u64::from(st.st_nlink);
    if nlink != 1 {
        return Err(StorageError::integrity(
            "recovery_ndjson_invalid",
            "ndjson file must not have hardlinks",
        ));
    }
    let file_size = u64::try_from(st.st_size).map_err(|_| {
        StorageError::integrity("recovery_ndjson_invalid", "negative ndjson file size")
    })?;
    if file_size == 0 {
        return Ok(0);
    }

    const CHUNK_BYTES: usize = 64 * 1024;
    const MAX_NDJSON_LINE_BYTES: usize = 2 * 1024 * 1024;

    let mut chunk = vec![0_u8; CHUNK_BYTES];
    let mut line_buf: Vec<u8> = Vec::with_capacity(4096);
    let mut read_offset = 0_u64;
    let mut line_start_offset = 0_u64;
    let mut write_offset = 0_u64;
    let mut valid_count = 0_u64;
    let mut needs_truncate = false;

    'scan: while read_offset < file_size {
        let to_read = usize::try_from((file_size - read_offset).min(CHUNK_BYTES as u64))
            .unwrap_or(CHUNK_BYTES);
        let off = libc::off_t::try_from(read_offset).map_err(|_| {
            StorageError::integrity("recovery_ndjson_invalid", "ndjson offset overflow")
        })?;
        let n = pread_retry(file.as_raw_fd(), &mut chunk[..to_read], off).map_err(|err| {
            StorageError::io("recovery_ndjson_read", "pread partial ndjson file", err)
        })?;
        if n == 0 {
            break;
        }

        let mut slice = &chunk[..n];
        let mut consumed_in_chunk = 0_u64;
        while !slice.is_empty() {
            if let Some(nl_idx) = slice.iter().position(|&b| b == b'\n') {
                let piece = &slice[..=nl_idx];
                if line_buf.len().saturating_add(piece.len()) > MAX_NDJSON_LINE_BYTES {
                    needs_truncate = true;
                    line_buf.clear();
                    break 'scan;
                }
                line_buf.extend_from_slice(piece);
                consumed_in_chunk += piece.len() as u64;
                slice = &slice[nl_idx + 1..];

                let trimmed = &line_buf[..line_buf.len() - 1];
                if trimmed.is_empty() {
                    needs_truncate = true;
                    line_buf.clear();
                    line_start_offset = read_offset + consumed_in_chunk;
                    continue;
                }
                if serde_json::from_slice::<Value>(trimmed).is_ok() {
                    if write_offset < line_start_offset {
                        pwrite_all_fd(file.as_raw_fd(), &line_buf, write_offset).map_err(
                            |err| {
                                StorageError::io(
                                    "recovery_ndjson_compact",
                                    "compact sanitized ndjson in-place",
                                    err,
                                )
                            },
                        )?;
                    }
                    write_offset += line_buf.len() as u64;
                    valid_count += 1;
                    line_buf.clear();
                    line_start_offset = read_offset + consumed_in_chunk;
                } else {
                    needs_truncate = true;
                    line_buf.clear();
                    break 'scan;
                }
            } else {
                if line_buf.len().saturating_add(slice.len()) > MAX_NDJSON_LINE_BYTES {
                    needs_truncate = true;
                    line_buf.clear();
                    break 'scan;
                }
                line_buf.extend_from_slice(slice);
                consumed_in_chunk += slice.len() as u64;
                break;
            }
        }
        read_offset += consumed_in_chunk;
    }

    if !line_buf.is_empty() {
        needs_truncate = true;
    }

    if needs_truncate || write_offset < file_size {
        ftruncate_ndjson_fd(file.as_raw_fd(), write_offset).map_err(|err| {
            StorageError::io(
                "recovery_ndjson_truncate",
                "ftruncate sanitized ndjson file",
                err,
            )
        })?;
        file.sync_all().map_err(|err| {
            StorageError::io("recovery_ndjson_sync", "fsync sanitized ndjson file", err)
        })?;
    }

    Ok(valid_count)
}

fn pwrite_all_fd(fd: libc::c_int, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
    while !buf.is_empty() {
        let off = libc::off_t::try_from(offset)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "pwrite offset overflow"))?;
        // SAFETY: `fd` is a valid open file descriptor and `buf` points to valid memory.
        let written = unsafe { libc::pwrite(fd, buf.as_ptr().cast(), buf.len(), off) };
        if written < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "pwrite returned zero bytes",
            ));
        }
        let n = written as usize;
        buf = &buf[n..];
        offset += n as u64;
    }
    Ok(())
}

fn ftruncate_ndjson_fd(fd: libc::c_int, new_size: u64) -> io::Result<()> {
    let off = libc::off_t::try_from(new_size)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "ftruncate size overflow"))?;
    loop {
        // SAFETY: `fd` is a valid open regular file descriptor.
        if unsafe { libc::ftruncate(fd, off) } == 0 {
            return Ok(());
        }
        let err = io::Error::last_os_error();
        if err.kind() != io::ErrorKind::Interrupted {
            return Err(err);
        }
    }
}

struct PartialMetadata {
    session_id: String,
    take_id: String,
    take_sequence: u64,
    continuation_of: Option<String>,
    volume_id: String,
    capture_mode: String,
    display_name: String,
    device_id: String,
    device_label: String,
    hardware_fingerprint: String,
    platform: String,
    software_version: String,
    commit: String,
    started_at: String,
    timezone: String,
    camera_width: u32,
    camera_height: u32,
    sensor_fps: f64,
    frame_decimation: u32,
    segment_frames: u64,
    encoding: RecordingEncoding,
}

fn load_partial_metadata(
    partial_path: &Path,
    paths: &DataVolumePaths,
    defaults: &RecoveryDefaults,
) -> PartialMetadata {
    let capture_meta = read_bounded_json_value(
        &partial_path.join("capture.json"),
        MAX_CONTROL_JSON_BYTES,
        16,
    )
    .ok();
    let rec_meta = read_bounded_json_value(
        &partial_path.join("recording.json"),
        MAX_CONTROL_JSON_BYTES,
        16,
    )
    .ok();
    let vol_meta = read_bounded_json_value(&paths.volume_metadata_file(), 8 * 1024, 8).ok();

    let pick_str = |key: &str| -> Option<String> {
        capture_meta
            .as_ref()
            .and_then(|v| v.get(key))
            .and_then(Value::as_str)
            .or_else(|| {
                rec_meta
                    .as_ref()
                    .and_then(|v| v.get(key))
                    .and_then(Value::as_str)
            })
            .map(str::to_owned)
    };

    let session_id = pick_str("session_id")
        .filter(|s| is_valid_uuid_v7(s))
        .unwrap_or_else(new_uuid_v7);
    let take_id = pick_str("take_id")
        .filter(|s| is_valid_uuid_v7(s))
        .unwrap_or_else(|| session_id.clone());
    let continuation_of = pick_str("continuation_of").filter(|s| is_valid_uuid_v7(s));
    let take_sequence = capture_meta
        .as_ref()
        .and_then(|v| v.get("take_sequence"))
        .and_then(Value::as_u64)
        .filter(|&s| (continuation_of.is_some() && s >= 2) || (continuation_of.is_none() && s == 1))
        .unwrap_or(if continuation_of.is_some() { 2 } else { 1 });

    let volume_id = pick_str("volume_id")
        .or_else(|| {
            rec_meta
                .as_ref()
                .and_then(|v| v.get("storage"))
                .and_then(|s| s.get("volume_id"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .or_else(|| {
            vol_meta
                .as_ref()
                .and_then(|v| v.get("volume_id"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .filter(|s| is_valid_uuid_v4(s))
        .unwrap_or_else(new_uuid_v4);

    let capture_mode = pick_str("capture_mode")
        .filter(|m| m == "production" || m == "calibration")
        .unwrap_or_else(|| "production".to_owned());
    let display_name = pick_str("display_name")
        .filter(|s| {
            let count = s.chars().count();
            (1..=160).contains(&count)
        })
        .unwrap_or_else(|| format!("Recovered {session_id}"));
    let started_at = pick_str("started_at")
        .or_else(|| pick_str("updated_at"))
        .filter(|s| parse_rfc3339_utc_nanos(s).is_some())
        .unwrap_or_else(now_rfc3339_utc);

    let segment_frames = capture_meta
        .as_ref()
        .and_then(|v| v.get("segment_frames"))
        .and_then(Value::as_u64)
        .filter(|&n| n > 0)
        .unwrap_or(defaults.segment_frames);

    PartialMetadata {
        session_id,
        take_id,
        take_sequence,
        continuation_of,
        volume_id,
        capture_mode,
        display_name,
        device_id: defaults.device_id.clone(),
        device_label: defaults.device_label.clone(),
        hardware_fingerprint: defaults.hardware_fingerprint.clone(),
        platform: defaults.platform.clone(),
        software_version: defaults.software_version.clone(),
        commit: defaults.commit.clone(),
        started_at,
        timezone: defaults.timezone.clone(),
        camera_width: defaults.camera_width,
        camera_height: defaults.camera_height,
        sensor_fps: defaults.sensor_fps,
        frame_decimation: defaults.frame_decimation,
        segment_frames,
        encoding: defaults.encoding.clone(),
    }
}

fn path_cstring(path: &Path) -> StorageResult<CString> {
    CString::new(path.as_os_str().as_bytes())
        .map_err(|_| StorageError::invalid_input("invalid_path", "path contains NUL byte"))
}
