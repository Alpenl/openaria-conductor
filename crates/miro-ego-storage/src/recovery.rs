//! Crash prefix recovery, quarantine isolation, and collision-safe atomic publication
//! for interrupted captures under `/data/.miro-ego/staging/`.

use crate::catalog::{BinarySessionCatalog, DEVICE_SESSION_SCHEMA};
use crate::util::{
    UNKNOWN_DATE_DIR_NAME, now_rfc3339_utc, parse_rfc3339_utc_nanos,
    readable_date_time_from_rfc3339, sha256_hex, sweep_stale_atomic_temps,
};
use crate::{RecoveryManager, RecoveryReport, StorageError, StorageResult};
use miro_ego_core::artifact_verify::hash_file;
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
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const RECOVERY_CONTROL_FILES: [&str; 3] = ["recording.json", "capture.json", "checkpoint.json"];
pub const AUDIO_RECOVERY_CONTROL_FILES: [&str; 3] = [
    "audio/checkpoint.json",
    "audio/checkpoint.ndjson",
    "audio/checkpoint.tmp",
];
pub const AUDIO_CAPTURE_CLOCK_REL_PATH: &str = "audio/capture-clock.json";
const AUDIO_CHECKPOINT_SCHEMA: &str = "miro-ego.audio-checkpoint.v1";
const AUDIO_CLOCK_SCHEMA: &str = "miro-ego.audio-clock.v1";
const MAX_CONTROL_JSON_BYTES: usize = 1024 * 1024;
const MAX_AUDIO_JOURNAL_BYTES: usize = 16 * 1024 * 1024;
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
        sweep_interrupted_deletions(&self.paths);

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

        let meta = load_partial_metadata(partial_path, &self.paths, &self.defaults);
        let segment_frames = meta.segment_frames.max(1);
        let effective_fps = (meta.sensor_fps / f64::from(meta.frame_decimation.max(1))).max(1.0);
        let mut planned_segment_frames: Vec<u64> = Vec::with_capacity(valid_pairs.len());
        let mut total_video_frames = 0_u64;
        for pair in &valid_pairs {
            let seg_len = pair.sample_count.unwrap_or(segment_frames).max(1);
            planned_segment_frames.push(seg_len);
            total_video_frames = total_video_frames.saturating_add(seg_len);
        }
        let duration_seconds = (total_video_frames as f64) / effective_fps;
        let max_recovered_frames = total_video_frames;
        let max_recovered_segments = valid_pairs.len() as u64;

        let frames_path = partial_path.join("frames.ndjson");
        let imu_path = partial_path.join("imu.ndjson");
        let frame_bounds = sanitize_frames_ndjson_for_recovery(
            &frames_path,
            max_recovered_frames,
            max_recovered_segments,
            duration_seconds,
        )?;
        let frames_count = frame_bounds.kept_count;
        let frame_interval_ns = ((1_000_000_000.0 / effective_fps).round() as u64).max(1);
        let imu_count = sanitize_imu_ndjson_for_recovery(
            &imu_path,
            &frame_bounds,
            frame_interval_ns,
            duration_seconds,
        )?;
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

        let mut video_segments_json = Vec::with_capacity(valid_pairs.len());
        let mut running_frame = 0_u64;
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
            let seg_len = planned_segment_frames[idx];
            let start_frame = running_frame;
            let raw_end_frame = start_frame.saturating_add(seg_len);
            let end_frame = if idx + 1 == valid_pairs.len()
                && frames_count > start_frame
                && frames_count < raw_end_frame
                && pair.sample_count.is_none()
            {
                frames_count
            } else {
                raw_end_frame
            };
            running_frame = end_frame;
            let start_t = (start_frame as f64) / effective_fps;
            let end_t = ((end_frame as f64) / effective_fps).max(start_t + (1.0 / effective_fps));
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

        let audio_recovery = recover_durable_audio_segments(
            partial_path,
            &meta.audio_requested_mode,
            duration_seconds,
        )?;

        let kept_video_names: HashSet<String> = valid_pairs
            .iter()
            .flat_map(|pair| [pair.left_name.clone(), pair.right_name.clone()])
            .collect();
        let planned_discarded_tail_files = count_unrecoverable_tail_files(
            partial_path,
            &kept_video_names,
            &audio_recovery.kept_wav_names,
        )?;

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
            "audio": audio_recovery.manifest_audio,
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

        // Quarantine any unrecoverable tail / temporary / unpaired files while keeping
        // control markers (`capture.json`, `recording.json`, `audio/checkpoint.*`) intact
        // until catalog admission succeeds (so a transient rollback never loses provenance).
        let (mut discarded_tail_files, tail_dir_opt) = quarantine_unrecoverable_tail_files(
            &self.paths.quarantine_dir(),
            partial_path,
            partial_name,
            &kept_video_names,
            &audio_recovery.kept_wav_names,
        )?;

        let relative_dir = publish_staging_directory(&self.paths, partial_path, &meta.started_at)?;

        // --- Post-publish section: `partial_path` no longer exists; operate strictly on `final_root`! ---
        // Admit into catalog BEFORE removing control markers so any rollback on transient error
        // preserves `capture.json`, `recording.json`, and `audio/checkpoint.*` inside `staging/`.
        let final_root = self.paths.recordings_dir().join(&relative_dir);
        if let Some(catalog) = &self.catalog {
            let admit_res: StorageResult<()> = (|| {
                let mut indexed = crate::catalog::parse_indexed_session_from_manifest(
                    &relative_dir,
                    &manifest_value,
                    &manifest_sha256,
                )?;
                let build_sha256 = sha256_hex(
                    format!("miro-ego:{}:{}", meta.software_version, meta.commit).as_bytes(),
                );
                indexed.verification = Some(crate::catalog::GatewayVerification {
                    actor: "gateway".to_owned(),
                    validator: crate::catalog::GatewayValidatorInfo {
                        name: "miro-ego".to_owned(),
                        version: meta.software_version.clone(),
                        build_sha256,
                    },
                    manifest_sha256: manifest_sha256.clone(),
                    verified_at: now.clone(),
                    verdict: "usable".to_owned(),
                    diagnostics: Vec::new(),
                });
                catalog.admit_indexed_session(indexed)
            })();

            if let Err(post_err) = admit_res {
                if is_transient_storage_error(&post_err) {
                    // Roll back published directory back into `staging/` with all control markers
                    // intact so recovery can retry cleanly once disk space / writability returns.
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
        }

        // Move any audio checkpoint files into the quarantine tail directory (if present) before
        // sweeping control markers so checkpoint files are isolated out of `/data/recordings/`.
        discarded_tail_files += quarantine_post_admit_audio_checkpoints(
            &self.paths.quarantine_dir(),
            &final_root,
            partial_name,
            tail_dir_opt.as_deref(),
        );
        if self.catalog.is_some() {
            let _ = remove_control_markers_with_logs(&final_root, &[]);
        } else {
            remove_control_markers_with_logs(&final_root, &[])?;
        }
        let _ = fsync_directory(&final_root);

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

/// Remove staging control markers (`recording.json`, `capture.json`, `checkpoint.json`,
/// `audio/checkpoint.*`, and undeclared `audio/capture-clock.json`) after publish.
pub fn remove_control_markers(session_root: &Path) -> StorageResult<()> {
    remove_control_markers_with_logs(session_root, &[])
}

/// Remove staging control markers after publish while preserving `audio/capture-clock.json`
/// if explicitly declared in `manifest.logs`.
pub fn remove_control_markers_with_logs(
    session_root: &Path,
    declared_log_paths: &[String],
) -> StorageResult<()> {
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

    for marker in AUDIO_RECOVERY_CONTROL_FILES {
        let path = session_root.join(marker);
        remove_control_entry_if_present(&path, marker)?;
    }

    let keep_capture_clock = declared_log_paths
        .iter()
        .any(|p| p == AUDIO_CAPTURE_CLOCK_REL_PATH);
    if !keep_capture_clock {
        let clock_path = session_root.join(AUDIO_CAPTURE_CLOCK_REL_PATH);
        remove_control_entry_if_present(&clock_path, AUDIO_CAPTURE_CLOCK_REL_PATH)?;
    }

    let _ = sweep_stale_atomic_temps(session_root);
    let audio_dir = session_root.join("audio");
    let _ = sweep_stale_atomic_temps(&audio_dir);
    if audio_dir.is_dir() {
        // Remove empty `audio/` directory if no audio/log artifacts remain; non-empty fails harmlessly.
        let _ = fs::remove_dir(&audio_dir);
    }
    Ok(())
}

fn remove_control_entry_if_present(path: &Path, label: &str) -> StorageResult<()> {
    let meta = match fs::symlink_metadata(path) {
        Ok(m) => m,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(err) => {
            return Err(StorageError::io(
                "remove_control_marker_failed",
                format!("stat staging control file {label}"),
                err,
            ));
        }
    };
    let res = if meta.is_dir() && !meta.file_type().is_symlink() {
        fs::remove_dir(path)
    } else {
        fs::remove_file(path)
    };
    match res {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(StorageError::io(
            "remove_control_marker_failed",
            format!("remove staging control file {label}"),
            err,
        )),
    }
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
    sample_count: Option<u64>,
}

/// Remove any half-unlinked session directories inside `/data/.miro-ego/deleting/` left behind
/// if power failed after atomic trash staging during `deleteSessions`.
pub fn sweep_interrupted_deletions(paths: &DataVolumePaths) {
    let deleting_dir = paths.deleting_dir();
    let Ok(del_meta) = fs::symlink_metadata(&deleting_dir) else {
        return;
    };
    if del_meta.file_type().is_symlink() || !del_meta.is_dir() {
        return;
    }
    let Ok(meta_stat) = fs::symlink_metadata(paths.hidden_meta_dir()) else {
        return;
    };
    if del_meta.dev() != meta_stat.dev() {
        return;
    }
    let Ok(entries) = fs::read_dir(&deleting_dir) else {
        return;
    };
    let mut removed_any = false;
    for entry in entries.flatten() {
        let p = entry.path();
        let Ok(st) = fs::symlink_metadata(&p) else {
            continue;
        };
        if st.dev() != del_meta.dev() || st.file_type().is_symlink() {
            continue;
        }
        if st.is_dir() {
            if fs::remove_dir_all(&p).is_ok() {
                removed_any = true;
            }
        } else if st.is_file() && fs::remove_file(&p).is_ok() {
            removed_any = true;
        }
    }
    if removed_any {
        let _ = fsync_directory(&deleting_dir);
    }
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
        let left_info = inspect_mp4_segment(&left_path).ok().flatten();
        let right_info = inspect_mp4_segment(&right_path).ok().flatten();
        let (Some(l_samples), Some(r_samples)) = (left_info, right_info) else {
            break;
        };
        if l_samples != r_samples {
            break;
        }
        pairs.push(VerifiedStereoPair {
            left_name,
            right_name,
            sample_count: l_samples,
        });
    }
    Ok(pairs)
}

/// Fast structural ISO-BMFF (MP4) box walk verifying `ftyp` at offset 0, non-empty `mdat`,
/// finalized `moov`, and (when `trak/mdia/minf/stbl/stsz` is present) non-zero `stsz` sample count.
pub fn validate_mp4_boxes(path: &Path) -> StorageResult<bool> {
    Ok(inspect_mp4_segment(path)?.is_some())
}

fn inspect_mp4_segment(path: &Path) -> StorageResult<Option<Option<u64>>> {
    let mut file = match open_readonly_safe(path) {
        Ok(f) => f,
        Err(_) => return Ok(None),
    };
    let st = fstat(file.as_raw_fd())
        .map_err(|err| StorageError::io("mp4_stat_failed", "fstat mp4 segment", err))?;
    if (st.st_mode & libc::S_IFMT) != libc::S_IFREG {
        return Ok(None);
    }
    let Ok(file_size) = u64::try_from(st.st_size) else {
        return Ok(None);
    };
    if file_size < 32 {
        return Ok(None);
    }

    let mut offset = 0_u64;
    let mut saw_ftyp_at_zero = false;
    let mut saw_non_empty_mdat = false;
    let mut saw_moov = false;
    let mut stsz_samples: Option<u64> = None;
    let mut saw_trak = false;
    let mut box_count = 0_usize;

    while offset < file_size {
        box_count += 1;
        if box_count > 4096 || file_size - offset < 8 {
            return Ok(None);
        }
        let Some((box_type, box_size, header_len)) =
            read_mp4_box_header(&mut file, offset, file_size)
        else {
            return Ok(None);
        };

        if offset == 0 {
            if &box_type != b"ftyp" || box_size < 16 {
                return Ok(None);
            }
            saw_ftyp_at_zero = true;
        }

        if &box_type == b"mdat" && box_size > header_len {
            saw_non_empty_mdat = true;
        } else if &box_type == b"moov" && box_size > header_len {
            saw_moov = true;
            let moov_start = offset + header_len;
            let moov_end = offset + box_size;
            match parse_moov_stsz_samples(&mut file, moov_start, moov_end) {
                MoovStszOutcome::Invalid => return Ok(None),
                MoovStszOutcome::NoTrak => {}
                MoovStszOutcome::Samples(count) => {
                    saw_trak = true;
                    stsz_samples = Some(count);
                }
            }
        }

        offset += box_size;
    }

    if offset != file_size || !saw_ftyp_at_zero || !saw_non_empty_mdat || !saw_moov {
        return Ok(None);
    }
    if saw_trak && stsz_samples.unwrap_or(0) == 0 {
        return Ok(None);
    }
    Ok(Some(stsz_samples))
}

enum MoovStszOutcome {
    NoTrak,
    Samples(u64),
    Invalid,
}

fn read_mp4_box_header(
    file: &mut File,
    offset: u64,
    limit_end: u64,
) -> Option<([u8; 4], u64, u64)> {
    if limit_end.saturating_sub(offset) < 8 {
        return None;
    }
    let mut header = [0_u8; 8];
    read_exact_at(file, offset, &mut header).ok()?;
    let size32 = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
    let box_type = [header[4], header[5], header[6], header[7]];
    let (box_size, header_len) = if size32 == 0 {
        return None;
    } else if size32 == 1 {
        if limit_end.saturating_sub(offset) < 16 {
            return None;
        }
        let mut ext = [0_u8; 8];
        read_exact_at(file, offset + 8, &mut ext).ok()?;
        let ext_size = u64::from_be_bytes(ext);
        if ext_size < 16 {
            return None;
        }
        (ext_size, 16_u64)
    } else {
        let s = u64::from(size32);
        if s < 8 {
            return None;
        }
        (s, 8_u64)
    };
    if offset
        .checked_add(box_size)
        .is_none_or(|end| end > limit_end)
    {
        return None;
    }
    Some((box_type, box_size, header_len))
}

fn parse_moov_stsz_samples(file: &mut File, start: u64, end: u64) -> MoovStszOutcome {
    let mut cursor = start;
    let mut saw_trak = false;
    let mut found_samples: Option<u64> = None;
    let mut guard = 0_usize;
    while cursor < end {
        guard += 1;
        if guard > 1024 {
            return MoovStszOutcome::Invalid;
        }
        let Some((btype, bsize, hlen)) = read_mp4_box_header(file, cursor, end) else {
            return MoovStszOutcome::Invalid;
        };
        if &btype == b"trak" {
            saw_trak = true;
            if let Some(samples) = find_stsz_in_container(file, cursor + hlen, cursor + bsize, 0) {
                if samples == 0 {
                    return MoovStszOutcome::Invalid;
                }
                found_samples = Some(samples);
            } else {
                return MoovStszOutcome::Invalid;
            }
        }
        cursor += bsize;
    }
    if !saw_trak {
        MoovStszOutcome::NoTrak
    } else if let Some(n) = found_samples {
        MoovStszOutcome::Samples(n)
    } else {
        MoovStszOutcome::Invalid
    }
}

fn find_stsz_in_container(file: &mut File, start: u64, end: u64, depth: usize) -> Option<u64> {
    if depth > 6 {
        return None;
    }
    let mut cursor = start;
    let mut guard = 0_usize;
    while cursor < end {
        guard += 1;
        if guard > 1024 {
            return None;
        }
        let (btype, bsize, hlen) = read_mp4_box_header(file, cursor, end)?;
        let payload_start = cursor + hlen;
        let payload_end = cursor + bsize;
        if matches!(&btype, b"mdia" | b"minf" | b"stbl") {
            if let Some(n) = find_stsz_in_container(file, payload_start, payload_end, depth + 1) {
                return Some(n);
            }
        } else if &btype == b"stsz" {
            let payload_len = payload_end.saturating_sub(payload_start);
            if payload_len < 12 {
                return None;
            }
            let mut stsz_hdr = [0_u8; 12];
            read_exact_at(file, payload_start, &mut stsz_hdr).ok()?;
            let sample_size =
                u32::from_be_bytes([stsz_hdr[4], stsz_hdr[5], stsz_hdr[6], stsz_hdr[7]]);
            let sample_count = u64::from(u32::from_be_bytes([
                stsz_hdr[8],
                stsz_hdr[9],
                stsz_hdr[10],
                stsz_hdr[11],
            ]));
            if sample_count == 0 {
                return None;
            }
            if sample_size == 0
                && payload_len < 12_u64.saturating_add(sample_count.saturating_mul(4))
            {
                return None;
            }
            return Some(sample_count);
        }
        cursor += bsize;
    }
    None
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

struct RecoveredAudioResult {
    manifest_audio: Value,
    kept_wav_names: HashSet<String>,
}

struct CheckpointedAudioEntry {
    index: u64,
    path: String,
    start_sample: u64,
    end_sample: u64,
    start_time_seconds: Option<f64>,
    end_time_seconds: Option<f64>,
    bytes: Option<u64>,
    sha256: Option<String>,
}

struct ValidatedWavHeader {
    sample_rate: u32,
    channels: u16,
    pcm_payload_bytes: u64,
    sample_frames: u64,
}

fn build_not_recorded_audio_json(requested_mode: &str) -> Value {
    let reason = if requested_mode == "disabled" {
        "user_disabled"
    } else {
        "recording_interrupted"
    };
    json!({
        "state": "not_recorded",
        "requested_mode": requested_mode,
        "resolved_mode": "disabled",
        "reason": reason,
    })
}

fn recover_durable_audio_segments(
    partial_path: &Path,
    requested_mode: &str,
    video_duration_seconds: f64,
) -> StorageResult<RecoveredAudioResult> {
    let audio_dir = partial_path.join("audio");
    if requested_mode == "disabled" || !audio_dir.is_dir() {
        return Ok(RecoveredAudioResult {
            manifest_audio: build_not_recorded_audio_json(requested_mode),
            kept_wav_names: HashSet::new(),
        });
    }

    let Ok(partial_meta) = fs::symlink_metadata(partial_path) else {
        return Ok(RecoveredAudioResult {
            manifest_audio: build_not_recorded_audio_json(requested_mode),
            kept_wav_names: HashSet::new(),
        });
    };
    let Ok(audio_meta) = fs::symlink_metadata(&audio_dir) else {
        return Ok(RecoveredAudioResult {
            manifest_audio: build_not_recorded_audio_json(requested_mode),
            kept_wav_names: HashSet::new(),
        });
    };
    if audio_meta.file_type().is_symlink()
        || !audio_meta.is_dir()
        || audio_meta.dev() != partial_meta.dev()
    {
        return Ok(RecoveredAudioResult {
            manifest_audio: build_not_recorded_audio_json(requested_mode),
            kept_wav_names: HashSet::new(),
        });
    }

    let Some(entries) = load_audio_checkpoint_entries(&audio_dir, partial_meta.dev()) else {
        return Ok(RecoveredAudioResult {
            manifest_audio: build_not_recorded_audio_json(requested_mode),
            kept_wav_names: HashSet::new(),
        });
    };

    let mut kept_wav_names = HashSet::new();
    let mut segments_json = Vec::new();
    let mut expected_sample = 0_u64;
    let mut common_rate: Option<u32> = None;
    let mut common_channels: Option<u16> = None;
    let mut first_start_time: Option<f64> = None;
    let mut last_end_time: Option<f64> = None;

    for (expected_idx, entry) in entries.iter().enumerate() {
        let idx = expected_idx as u64;
        let expected_leaf = format!("audio_{idx:05}.wav");
        let expected_rel = format!("audio/{expected_leaf}");
        if entry.index != idx
            || entry.path != expected_rel
            || entry.start_sample != expected_sample
            || entry.end_sample <= entry.start_sample
        {
            break;
        }

        let wav_path = audio_dir.join(&expected_leaf);
        let Some(wav_hdr) = validate_durable_wav_segment(&wav_path, partial_meta.dev()) else {
            break;
        };
        let expected_frames = entry.end_sample - entry.start_sample;
        if wav_hdr.sample_frames != expected_frames {
            break;
        }
        if let Some(rate) = common_rate {
            if wav_hdr.sample_rate != rate {
                break;
            }
        } else {
            common_rate = Some(wav_hdr.sample_rate);
        }
        if let Some(ch) = common_channels {
            if wav_hdr.channels != ch {
                break;
            }
        } else {
            common_channels = Some(wav_hdr.channels);
        }

        let computed_start = (entry.start_sample as f64) / f64::from(wav_hdr.sample_rate);
        let computed_end = (entry.end_sample as f64) / f64::from(wav_hdr.sample_rate);
        let start_t = entry
            .start_time_seconds
            .filter(|t| t.is_finite() && *t >= 0.0)
            .unwrap_or(computed_start);
        let end_t = entry
            .end_time_seconds
            .filter(|t| t.is_finite() && *t > start_t)
            .unwrap_or(computed_end);
        if end_t <= start_t || end_t <= 0.0 {
            break;
        }
        // Stop before any segment that begins completely after the recovered video prefix.
        if idx > 0 && video_duration_seconds > 0.0 && start_t >= video_duration_seconds {
            break;
        }

        let Ok(digest) = hash_file(&wav_path) else {
            break;
        };
        if let Some(expected_bytes) = entry.bytes {
            if digest.identity.size != expected_bytes {
                break;
            }
        }
        if let Some(expected_sha) = &entry.sha256 {
            if &digest.sha256 != expected_sha {
                break;
            }
        }

        if first_start_time.is_none() {
            first_start_time = Some(start_t);
        }
        last_end_time = Some(end_t);
        expected_sample = entry.end_sample;
        kept_wav_names.insert(expected_leaf);
        segments_json.push(json!({
            "index": idx,
            "start_sample": entry.start_sample,
            "end_sample": entry.end_sample,
            "start_time_seconds": start_t,
            "end_time_seconds": end_t,
            "pcm_payload_bytes": wav_hdr.pcm_payload_bytes,
            "wav_header_bytes": 44_u64,
            "artifact": {
                "artifact_id": digest.sha256,
                "role": "audio.wav",
                "path": expected_rel,
                "media_type": "audio/wav",
                "bytes": digest.identity.size,
                "sha256": digest.sha256,
            }
        }));
    }

    let (Some(sample_rate), Some(channels), Some(start_t), Some(end_t)) = (
        common_rate,
        common_channels,
        first_start_time,
        last_end_time,
    ) else {
        return Ok(RecoveredAudioResult {
            manifest_audio: build_not_recorded_audio_json(requested_mode),
            kept_wav_names: HashSet::new(),
        });
    };
    if segments_json.is_empty() || expected_sample == 0 {
        return Ok(RecoveredAudioResult {
            manifest_audio: build_not_recorded_audio_json(requested_mode),
            kept_wav_names: HashSet::new(),
        });
    }

    let recorded_req_mode = if requested_mode == "enabled" {
        "enabled"
    } else {
        "device_default"
    };
    let mut audio_obj = json!({
        "state": "recorded",
        "requested_mode": recorded_req_mode,
        "resolved_mode": "enabled",
        "codec": "pcm_s16le",
        "container": "wav",
        "sample_format": "S16_LE",
        "sample_rate": sample_rate,
        "channels": channels,
        "sample_count": expected_sample,
        "sync": {
            "time_base": "host_monotonic",
            "start_time_seconds": start_t,
            "end_time_seconds": end_t,
            "video_time_reference": "session_time_seconds",
        },
        "segments": segments_json,
    });
    if let Some(clock_val) = load_optional_audio_capture_clock(partial_path, partial_meta.dev()) {
        if let Some(map) = audio_obj.as_object_mut() {
            map.insert("capture_clock".to_owned(), clock_val);
        }
    }

    Ok(RecoveredAudioResult {
        manifest_audio: audio_obj,
        kept_wav_names,
    })
}

fn load_optional_audio_capture_clock(partial_path: &Path, expected_dev: u64) -> Option<Value> {
    let clock_path = partial_path.join(AUDIO_CAPTURE_CLOCK_REL_PATH);
    let meta = fs::symlink_metadata(&clock_path).ok()?;
    if meta.file_type().is_symlink() || !meta.is_file() || meta.dev() != expected_dev {
        return None;
    }
    let val = read_bounded_json_value(&clock_path, MAX_CONTROL_JSON_BYTES, 16).ok()?;
    if val.get("schema").and_then(Value::as_str) == Some(AUDIO_CLOCK_SCHEMA) {
        Some(val)
    } else {
        None
    }
}

fn load_audio_checkpoint_entries(
    audio_dir: &Path,
    expected_dev: u64,
) -> Option<Vec<CheckpointedAudioEntry>> {
    let ckpt_path = audio_dir.join("checkpoint.json");
    let ckpt_meta = fs::symlink_metadata(&ckpt_path).ok()?;
    if ckpt_meta.file_type().is_symlink() || !ckpt_meta.is_file() || ckpt_meta.dev() != expected_dev
    {
        return None;
    }
    let head = read_bounded_json_value(&ckpt_path, MAX_CONTROL_JSON_BYTES, 16).ok()?;
    if head.get("schema").and_then(Value::as_str) != Some(AUDIO_CHECKPOINT_SCHEMA) {
        return None;
    }

    // Mode 1: Incremental journal (`checkpoint.json` header + `checkpoint.ndjson` verified prefix).
    if let (Some(journal_bytes), Some(expected_journal_sha)) = (
        head.get("journal_bytes").and_then(Value::as_u64),
        head.get("journal_sha256").and_then(Value::as_str),
    ) {
        let prefix_len = usize::try_from(journal_bytes).ok()?;
        if prefix_len > MAX_AUDIO_JOURNAL_BYTES {
            return None;
        }
        let mut parsed = Vec::new();
        if prefix_len > 0 {
            let journal_path = audio_dir.join("checkpoint.ndjson");
            let mut jfile = open_readonly_safe(&journal_path).ok()?;
            let jst = fstat(jfile.as_raw_fd()).ok()?;
            if (jst.st_mode & libc::S_IFMT) != libc::S_IFREG || jst.st_dev != expected_dev {
                return None;
            }
            let jsize = u64::try_from(jst.st_size).ok()?;
            if jsize < journal_bytes {
                return None;
            }
            let mut prefix = vec![0_u8; prefix_len];
            jfile.read_exact(&mut prefix).ok()?;
            if sha256_hex(&prefix) != expected_journal_sha {
                return None;
            }
            for (expected_seq, line) in prefix
                .split(|&b| b == b'\n')
                .filter(|l| !l.is_empty())
                .enumerate()
            {
                let row: Value = serde_json::from_slice(line).ok()?;
                if row.get("sequence").and_then(Value::as_u64) != Some(expected_seq as u64) {
                    return None;
                }
                let segs = row.get("segments").and_then(Value::as_array)?;
                for raw_seg in segs {
                    parsed.push(parse_checkpoint_segment_value(raw_seg)?);
                }
            }
            if let Some(expected_records) = head.get("journal_records").and_then(Value::as_u64) {
                let actual_records = prefix
                    .split(|&b| b == b'\n')
                    .filter(|l| !l.is_empty())
                    .count() as u64;
                if actual_records != expected_records {
                    return None;
                }
            }
        }
        if let Some(expected_seg_count) = head.get("segment_count").and_then(Value::as_u64) {
            if parsed.len() as u64 != expected_seg_count {
                return None;
            }
        }
        return Some(parsed);
    }

    // Mode 2: Standalone inline `segments` array inside `checkpoint.json`.
    let segs = head.get("segments").and_then(Value::as_array)?;
    let mut parsed = Vec::with_capacity(segs.len());
    for raw_seg in segs {
        parsed.push(parse_checkpoint_segment_value(raw_seg)?);
    }
    Some(parsed)
}

fn parse_checkpoint_segment_value(raw: &Value) -> Option<CheckpointedAudioEntry> {
    let index = raw.get("index")?.as_u64()?;
    let path = raw
        .get("path")
        .or_else(|| raw.get("relative_path"))?
        .as_str()?
        .to_owned();
    let start_sample = raw.get("start_sample")?.as_u64()?;
    let end_sample = raw.get("end_sample")?.as_u64()?;
    let start_time_seconds = raw.get("start_time_seconds").and_then(Value::as_f64);
    let end_time_seconds = raw.get("end_time_seconds").and_then(Value::as_f64);
    let bytes = raw.get("bytes").and_then(Value::as_u64);
    let sha256 = raw
        .get("sha256")
        .and_then(Value::as_str)
        .filter(|s| miro_ego_core::ids::is_valid_sha256(s))
        .map(str::to_owned);
    Some(CheckpointedAudioEntry {
        index,
        path,
        start_sample,
        end_sample,
        start_time_seconds,
        end_time_seconds,
        bytes,
        sha256,
    })
}

fn validate_durable_wav_segment(path: &Path, expected_dev: u64) -> Option<ValidatedWavHeader> {
    let mut file = open_readonly_safe(path).ok()?;
    let st = fstat(file.as_raw_fd()).ok()?;
    if (st.st_mode & libc::S_IFMT) != libc::S_IFREG || st.st_dev != expected_dev {
        return None;
    }
    #[allow(clippy::useless_conversion)]
    let nlink = u64::from(st.st_nlink);
    if nlink != 1 {
        return None;
    }
    let file_size = u64::try_from(st.st_size).ok()?;
    if file_size <= 44 {
        return None;
    }

    let mut hdr = [0_u8; 44];
    file.read_exact(&mut hdr).ok()?;
    if &hdr[0..4] != b"RIFF" || &hdr[8..12] != b"WAVE" || &hdr[12..16] != b"fmt " {
        return None;
    }
    let riff_size = u64::from(u32::from_le_bytes([hdr[4], hdr[5], hdr[6], hdr[7]]));
    let fmt_chunk_size = u32::from_le_bytes([hdr[16], hdr[17], hdr[18], hdr[19]]);
    let audio_format = u16::from_le_bytes([hdr[20], hdr[21]]);
    let channels = u16::from_le_bytes([hdr[22], hdr[23]]);
    let sample_rate = u32::from_le_bytes([hdr[24], hdr[25], hdr[26], hdr[27]]);
    let byte_rate = u32::from_le_bytes([hdr[28], hdr[29], hdr[30], hdr[31]]);
    let block_align = u16::from_le_bytes([hdr[32], hdr[33]]);
    let bits_per_sample = u16::from_le_bytes([hdr[34], hdr[35]]);
    if fmt_chunk_size != 16
        || audio_format != 1
        || !(1..=8).contains(&channels)
        || !(8_000..=384_000).contains(&sample_rate)
        || bits_per_sample != 16
        || &hdr[36..40] != b"data"
    {
        return None;
    }
    let frame_bytes = u64::from(channels) * 2;
    if u64::from(block_align) != frame_bytes
        || u64::from(byte_rate) != u64::from(sample_rate) * frame_bytes
    {
        return None;
    }
    let data_size = u64::from(u32::from_le_bytes([hdr[40], hdr[41], hdr[42], hdr[43]]));
    if data_size == 0
        || riff_size.checked_add(8) != Some(file_size)
        || data_size.checked_add(44) != Some(file_size)
        || data_size % frame_bytes != 0
    {
        return None;
    }
    Some(ValidatedWavHeader {
        sample_rate,
        channels,
        pcm_payload_bytes: data_size,
        sample_frames: data_size / frame_bytes,
    })
}

fn is_audio_checkpoint_control_name(name: &str) -> bool {
    matches!(
        name,
        "checkpoint.json" | "checkpoint.ndjson" | "capture-clock.json"
    )
}

fn collect_unrecoverable_tail_files(
    partial_path: &Path,
    kept_video_names: &HashSet<String>,
    kept_audio_names: &HashSet<String>,
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

    // Move any `.tmp` files at session root and any uncommitted/truncated audio tail files,
    // while keeping `audio/checkpoint.json` & `audio/checkpoint.ndjson` until catalog admit succeeds!
    for entry in fs::read_dir(partial_path)
        .map_err(|err| StorageError::io("recovery_readdir", "readdir staging root", err))?
        .flatten()
    {
        if let Ok(name) = entry.file_name().into_string() {
            if name.contains(".tmp") {
                stray_files.push((entry.path(), name));
            } else if name == "audio" {
                let audio_path = entry.path();
                if audio_path.is_dir() {
                    for aentry in fs::read_dir(&audio_path)
                        .map_err(|err| {
                            StorageError::io("recovery_readdir", "readdir staging audio", err)
                        })?
                        .flatten()
                    {
                        if let Ok(aname) = aentry.file_name().into_string() {
                            if !kept_audio_names.contains(&aname)
                                && !is_audio_checkpoint_control_name(&aname)
                            {
                                stray_files.push((aentry.path(), format!("audio_{aname}")));
                            }
                        }
                    }
                } else {
                    stray_files.push((audio_path, name));
                }
            }
        }
    }
    Ok(stray_files)
}

fn count_unrecoverable_tail_files(
    partial_path: &Path,
    kept_video_names: &HashSet<String>,
    kept_audio_names: &HashSet<String>,
) -> StorageResult<usize> {
    let mut count =
        collect_unrecoverable_tail_files(partial_path, kept_video_names, kept_audio_names)?.len();
    let audio_dir = partial_path.join("audio");
    if audio_dir.is_dir() {
        for ckpt_name in ["checkpoint.json", "checkpoint.ndjson"] {
            if fs::symlink_metadata(audio_dir.join(ckpt_name)).is_ok() {
                count += 1;
            }
        }
    }
    Ok(count)
}

fn quarantine_unrecoverable_tail_files(
    quarantine_root: &Path,
    partial_path: &Path,
    partial_name: &str,
    kept_video_names: &HashSet<String>,
    kept_audio_names: &HashSet<String>,
) -> StorageResult<(usize, Option<PathBuf>)> {
    let stray_files =
        collect_unrecoverable_tail_files(partial_path, kept_video_names, kept_audio_names)?;
    if stray_files.is_empty() {
        return Ok((0, None));
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
    Ok((moved, Some(tail_dir)))
}

fn quarantine_post_admit_audio_checkpoints(
    quarantine_root: &Path,
    final_root: &Path,
    partial_name: &str,
    existing_tail_dir: Option<&Path>,
) -> usize {
    let audio_dir = final_root.join("audio");
    if !audio_dir.is_dir() {
        return 0;
    }
    let mut ckpt_files = Vec::new();
    for ckpt_name in ["checkpoint.json", "checkpoint.ndjson"] {
        let p = audio_dir.join(ckpt_name);
        if fs::symlink_metadata(&p).is_ok() {
            ckpt_files.push((p, format!("audio_{ckpt_name}")));
        }
    }
    if ckpt_files.is_empty() {
        return 0;
    }
    let tail_dir = match existing_tail_dir {
        Some(dir) => dir.to_path_buf(),
        None => {
            if fs::create_dir_all(quarantine_root).is_err() {
                return 0;
            }
            let Ok(name) =
                allocate_unique_child_dir(quarantine_root, &format!("{partial_name}-tail"))
            else {
                return 0;
            };
            quarantine_root.join(name)
        }
    };
    let mut moved = 0_usize;
    for (src, flat_name) in ckpt_files {
        if fs::rename(&src, tail_dir.join(flat_name)).is_ok() {
            moved += 1;
        }
    }
    let _ = fsync_directory(&tail_dir);
    let _ = fsync_directory(quarantine_root);
    moved
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

#[derive(Default)]
struct RecoveredFrameBounds {
    kept_count: u64,
    last_kept_monotonic_ns: Option<u64>,
    first_discarded_monotonic_ns: Option<u64>,
}

fn extract_ndjson_monotonic_ns(val: &Value) -> Option<u64> {
    val.get("host_monotonic_ns")
        .or_else(|| val.get("monotonic_ns"))
        .or_else(|| val.get("timestamp_ns"))
        .and_then(Value::as_u64)
}

fn sanitize_frames_ndjson_for_recovery(
    path: &Path,
    max_frames: u64,
    max_segments: u64,
    max_duration_seconds: f64,
) -> StorageResult<RecoveredFrameBounds> {
    let mut bounds = RecoveredFrameBounds::default();
    let kept = sanitize_ndjson_with_filter(path, |kept_so_far, val| {
        let frame_idx_ok = val
            .get("frame")
            .or_else(|| val.get("frame_index"))
            .and_then(Value::as_u64)
            .is_none_or(|f| f < max_frames);
        let seg_idx_ok = val
            .get("segment_index")
            .and_then(Value::as_u64)
            .is_none_or(|s| s < max_segments);
        let time_ok = val
            .get("session_time_seconds")
            .or_else(|| val.get("time_seconds"))
            .and_then(Value::as_f64)
            .is_none_or(|t| t <= max_duration_seconds + 1e-6);

        let keep = kept_so_far < max_frames && frame_idx_ok && seg_idx_ok && time_ok;
        if keep {
            if let Some(ns) = extract_ndjson_monotonic_ns(val) {
                bounds.last_kept_monotonic_ns = Some(
                    bounds
                        .last_kept_monotonic_ns
                        .map_or(ns, |prev| prev.max(ns)),
                );
            }
        } else if bounds.first_discarded_monotonic_ns.is_none() {
            bounds.first_discarded_monotonic_ns = extract_ndjson_monotonic_ns(val);
        }
        keep
    })?;
    bounds.kept_count = kept;
    Ok(bounds)
}

fn sanitize_imu_ndjson_for_recovery(
    path: &Path,
    frame_bounds: &RecoveredFrameBounds,
    frame_interval_ns: u64,
    max_duration_seconds: f64,
) -> StorageResult<u64> {
    let max_time_secs = max_duration_seconds + (frame_interval_ns as f64 / 1_000_000_000.0);
    sanitize_ndjson_with_filter(path, |_kept_so_far, val| {
        if let Some(ns) = extract_ndjson_monotonic_ns(val) {
            if let Some(cutoff_ns) = frame_bounds.first_discarded_monotonic_ns {
                if ns >= cutoff_ns {
                    return false;
                }
            } else if let Some(last_ns) = frame_bounds.last_kept_monotonic_ns {
                if ns > last_ns.saturating_add(frame_interval_ns) {
                    return false;
                }
            }
        }
        if let Some(t) = val
            .get("session_time_seconds")
            .or_else(|| val.get("time_seconds"))
            .and_then(Value::as_f64)
        {
            if t > max_time_secs + 1e-6 {
                return false;
            }
        }
        true
    })
}

/// Stream-scan an NDJSON file in bounded 64 KiB chunks and truncate in-place via `ftruncate`
/// at the last valid newline-terminated JSON line (zero 2x-RAM or 2x-disk rewrite).
pub fn sanitize_ndjson_file(path: &Path) -> StorageResult<u64> {
    sanitize_ndjson_with_filter(path, |_, _| true)
}

fn sanitize_ndjson_with_filter(
    path: &Path,
    mut keep_record: impl FnMut(u64, &Value) -> bool,
) -> StorageResult<u64> {
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
    let mut prefix_end = 0_u64;
    let mut valid_count = 0_u64;
    let mut needs_truncate = false;
    let mut saw_skip = false;
    let mut tmp_writer: Option<(PathBuf, BufWriter<File>)> = None;

    let open_compaction_tmp = |prefix_bytes: u64| -> StorageResult<(PathBuf, BufWriter<File>)> {
        let fname = path.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
            StorageError::invalid_input("invalid_path", "invalid ndjson filename")
        })?;
        let tmp_path = path.with_file_name(format!("{fname}.tmp"));
        let tmp_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o640)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&tmp_path)
            .map_err(|err| {
                StorageError::io(
                    "recovery_ndjson_compact",
                    "create ndjson compaction tmp",
                    err,
                )
            })?;
        let mut writer = BufWriter::with_capacity(CHUNK_BYTES, tmp_file);
        let mut copy_off = 0_u64;
        let mut copy_buf = vec![0_u8; CHUNK_BYTES];
        while copy_off < prefix_bytes {
            let want = usize::try_from((prefix_bytes - copy_off).min(CHUNK_BYTES as u64))
                .unwrap_or(CHUNK_BYTES);
            let off = libc::off_t::try_from(copy_off).map_err(|_| {
                StorageError::integrity("recovery_ndjson_invalid", "ndjson offset overflow")
            })?;
            let read =
                pread_retry(file.as_raw_fd(), &mut copy_buf[..want], off).map_err(|err| {
                    StorageError::io("recovery_ndjson_read", "copy ndjson prefix to tmp", err)
                })?;
            if read == 0 {
                break;
            }
            if let Err(err) = writer.write_all(&copy_buf[..read]) {
                drop(writer);
                let _ = fs::remove_file(&tmp_path);
                return Err(StorageError::io(
                    "recovery_ndjson_compact",
                    "write ndjson prefix to tmp",
                    err,
                ));
            }
            copy_off += read as u64;
        }
        Ok((tmp_path, writer))
    };

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
                    saw_skip = true;
                    line_buf.clear();
                    continue;
                }
                if let Ok(parsed_val) = serde_json::from_slice::<Value>(trimmed) {
                    if !keep_record(valid_count, &parsed_val) {
                        needs_truncate = true;
                        saw_skip = true;
                        line_buf.clear();
                        continue;
                    }
                    if !saw_skip {
                        prefix_end = read_offset + consumed_in_chunk;
                    } else {
                        if tmp_writer.is_none() {
                            tmp_writer = Some(open_compaction_tmp(prefix_end)?);
                        }
                        if let Some((ref tmp_path, ref mut writer)) = tmp_writer
                            && let Err(err) = writer.write_all(&line_buf)
                        {
                            let cleanup_path = tmp_path.clone();
                            drop(tmp_writer.take());
                            let _ = fs::remove_file(&cleanup_path);
                            return Err(StorageError::io(
                                "recovery_ndjson_compact",
                                "write sanitized ndjson record to tmp",
                                err,
                            ));
                        }
                    }
                    valid_count += 1;
                    line_buf.clear();
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

    if let Some((tmp_path, mut writer)) = tmp_writer {
        if let Err(err) = writer.flush() {
            drop(writer);
            let _ = fs::remove_file(&tmp_path);
            return Err(StorageError::io(
                "recovery_ndjson_compact",
                "flush sanitized ndjson tmp",
                err,
            ));
        }
        let tmp_file = writer.into_inner().map_err(|err| {
            let _ = fs::remove_file(&tmp_path);
            StorageError::io(
                "recovery_ndjson_compact",
                "unwrap sanitized ndjson tmp writer",
                err.into_error(),
            )
        })?;
        if let Err(err) = tmp_file.sync_all() {
            drop(tmp_file);
            let _ = fs::remove_file(&tmp_path);
            return Err(StorageError::io(
                "recovery_ndjson_sync",
                "fsync sanitized ndjson tmp",
                err,
            ));
        }
        drop(tmp_file);
        drop(file);
        if let Err(err) = fs::rename(&tmp_path, path) {
            let _ = fs::remove_file(&tmp_path);
            return Err(StorageError::io(
                "recovery_ndjson_compact",
                "rename sanitized ndjson tmp over target",
                err,
            ));
        }
        if let Some(parent) = path.parent() {
            let _ = fsync_directory(parent);
        }
    } else if needs_truncate || prefix_end < file_size {
        ftruncate_ndjson_fd(file.as_raw_fd(), prefix_end).map_err(|err| {
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
    audio_requested_mode: String,
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
    let manifest_meta = read_bounded_json_value(
        &partial_path.join("manifest.json"),
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
            .or_else(|| {
                manifest_meta
                    .as_ref()
                    .and_then(|v| v.get(key))
                    .and_then(Value::as_str)
            })
            .map(str::to_owned)
    };

    let stem_uuid = partial_path
        .file_name()
        .and_then(|os| os.to_str())
        .and_then(|name| name.strip_suffix(".partial"))
        .filter(|stem| is_valid_uuid_v7(stem))
        .map(str::to_owned);

    let session_id = pick_str("session_id")
        .filter(|s| is_valid_uuid_v7(s))
        .or(stem_uuid)
        .unwrap_or_else(new_uuid_v7);

    let manifest_take = manifest_meta.as_ref().and_then(|m| m.get("take"));
    let take_id = pick_str("take_id")
        .or_else(|| {
            manifest_take
                .and_then(|t| t.get("take_id"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .filter(|s| is_valid_uuid_v7(s))
        .unwrap_or_else(|| session_id.clone());

    let continuation_of = pick_str("continuation_of")
        .or_else(|| {
            manifest_take
                .and_then(|t| t.get("continuation_of"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .filter(|s| is_valid_uuid_v7(s));

    let take_sequence = capture_meta
        .as_ref()
        .and_then(|v| v.get("take_sequence"))
        .and_then(Value::as_u64)
        .or_else(|| {
            manifest_take
                .and_then(|t| t.get("sequence"))
                .and_then(Value::as_u64)
        })
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
        .or_else(|| {
            manifest_meta
                .as_ref()
                .and_then(|m| m.get("time"))
                .and_then(|t| t.get("started_at"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .or_else(|| pick_str("updated_at"))
        .filter(|s| parse_rfc3339_utc_nanos(s).is_some())
        .unwrap_or_else(now_rfc3339_utc);

    let segment_frames = capture_meta
        .as_ref()
        .and_then(|v| v.get("segment_frames"))
        .and_then(Value::as_u64)
        .filter(|&n| n > 0)
        .unwrap_or(defaults.segment_frames);

    let cap_device = capture_meta.as_ref().and_then(|v| v.get("device"));
    let rec_device = rec_meta.as_ref().and_then(|v| v.get("device"));
    let man_device = manifest_meta.as_ref().and_then(|v| v.get("device"));
    let pick_dev_str = |key: &str| -> Option<String> {
        cap_device
            .and_then(|d| d.get(key))
            .and_then(Value::as_str)
            .or_else(|| rec_device.and_then(|d| d.get(key)).and_then(Value::as_str))
            .or_else(|| man_device.and_then(|d| d.get(key)).and_then(Value::as_str))
            .map(str::to_owned)
    };

    let device_id = pick_dev_str("device_id")
        .filter(|s| is_valid_uuid_v4(s))
        .unwrap_or_else(|| defaults.device_id.clone());
    let device_label = pick_dev_str("device_label")
        .filter(|s| is_valid_device_label(s))
        .unwrap_or_else(|| defaults.device_label.clone());
    let hardware_fingerprint = pick_dev_str("hardware_fingerprint")
        .filter(|s| {
            s.strip_prefix("sha256:")
                .is_some_and(miro_ego_core::ids::is_valid_sha256)
        })
        .unwrap_or_else(|| defaults.hardware_fingerprint.clone());
    let platform = pick_dev_str("platform")
        .filter(|s| !s.is_empty() && s.len() <= 128)
        .unwrap_or_else(|| defaults.platform.clone());
    let software_version = pick_dev_str("software_version")
        .filter(|s| !s.is_empty() && s.len() <= 64)
        .unwrap_or_else(|| defaults.software_version.clone());
    let commit = pick_dev_str("commit")
        .filter(|s| {
            (40..=64).contains(&s.len())
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        })
        .unwrap_or_else(|| defaults.commit.clone());

    let timezone = pick_str("timezone")
        .or_else(|| {
            manifest_meta
                .as_ref()
                .and_then(|m| m.get("time"))
                .and_then(|t| t.get("timezone"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .filter(|tz| tz.contains('/') && !tz.starts_with('/') && !tz.ends_with('/'))
        .unwrap_or_else(|| defaults.timezone.clone());

    let cap_camera = capture_meta.as_ref().and_then(|v| v.get("camera"));
    let man_camera = manifest_meta.as_ref().and_then(|v| v.get("camera"));
    let camera_width = cap_camera
        .and_then(|c| c.get("width"))
        .or_else(|| capture_meta.as_ref().and_then(|v| v.get("camera_width")))
        .or_else(|| man_camera.and_then(|c| c.get("width")))
        .and_then(Value::as_u64)
        .and_then(|v| u32::try_from(v).ok())
        .filter(|&w| w > 0)
        .unwrap_or(defaults.camera_width);
    let camera_height = cap_camera
        .and_then(|c| c.get("height"))
        .or_else(|| capture_meta.as_ref().and_then(|v| v.get("camera_height")))
        .or_else(|| man_camera.and_then(|c| c.get("height")))
        .and_then(Value::as_u64)
        .and_then(|v| u32::try_from(v).ok())
        .filter(|&h| h > 0)
        .unwrap_or(defaults.camera_height);
    let sensor_fps = cap_camera
        .and_then(|c| c.get("sensor_fps"))
        .or_else(|| capture_meta.as_ref().and_then(|v| v.get("sensor_fps")))
        .or_else(|| man_camera.and_then(|c| c.get("sensor_fps")))
        .and_then(Value::as_f64)
        .filter(|f| f.is_finite() && *f > 0.0)
        .unwrap_or(defaults.sensor_fps);
    let frame_decimation = cap_camera
        .and_then(|c| c.get("frame_decimation"))
        .or_else(|| {
            capture_meta
                .as_ref()
                .and_then(|v| v.get("frame_decimation"))
        })
        .or_else(|| man_camera.and_then(|c| c.get("frame_decimation")))
        .and_then(Value::as_u64)
        .and_then(|v| u32::try_from(v).ok())
        .filter(|&d| d > 0)
        .unwrap_or(defaults.frame_decimation);

    let encoding = capture_meta
        .as_ref()
        .and_then(|v| v.get("encoding"))
        .and_then(parse_recording_encoding_value)
        .or_else(|| {
            manifest_meta
                .as_ref()
                .and_then(|m| m.get("video"))
                .and_then(|vid| vid.get("encoding"))
                .and_then(parse_recording_encoding_value)
        })
        .unwrap_or_else(|| defaults.encoding.clone());

    let audio_requested_mode = pick_str("audio_requested_mode")
        .or_else(|| {
            manifest_meta
                .as_ref()
                .and_then(|m| m.get("audio"))
                .and_then(|a| a.get("requested_mode"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .filter(|m| matches!(m.as_str(), "device_default" | "disabled" | "enabled"))
        .unwrap_or_else(|| "device_default".to_owned());

    PartialMetadata {
        session_id,
        take_id,
        take_sequence,
        continuation_of,
        volume_id,
        capture_mode,
        display_name,
        device_id,
        device_label,
        hardware_fingerprint,
        platform,
        software_version,
        commit,
        started_at,
        timezone,
        camera_width,
        camera_height,
        sensor_fps,
        frame_decimation,
        segment_frames,
        encoding,
        audio_requested_mode,
    }
}

fn is_valid_device_label(label: &str) -> bool {
    let Some(hex) = label.strip_prefix("Miro-Ego-") else {
        return false;
    };
    hex.len() == 8
        && hex
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'A'..=b'F').contains(&b))
}

fn parse_recording_encoding_value(val: &Value) -> Option<RecordingEncoding> {
    if let Ok(enc) = serde_json::from_value::<RecordingEncoding>(val.clone()) {
        if enc.validate().is_ok() {
            return Some(enc);
        }
    }
    // Also support manifest-shaped `video.encoding` (where VBR/Fixqp fields may be `null`).
    let obj = val.as_object()?;
    let codec_str = obj.get("codec")?.as_str()?;
    let rc_str = obj.get("rate_control")?.as_str()?;
    let mut base = RecordingEncoding {
        codec: serde_json::from_value(Value::String(codec_str.to_owned())).ok()?,
        rate_control: serde_json::from_value(Value::String(rc_str.to_owned())).ok()?,
        ..RecordingEncoding::default()
    };
    if let Some(v) = obj
        .get("bitrate_kbps")
        .and_then(Value::as_u64)
        .and_then(|n| u32::try_from(n).ok())
    {
        base.bitrate_kbps = v;
    }
    if let Some(v) = obj
        .get("min_qp")
        .and_then(Value::as_u64)
        .and_then(|n| u8::try_from(n).ok())
    {
        base.min_qp = v;
    }
    if let Some(v) = obj
        .get("max_qp")
        .and_then(Value::as_u64)
        .and_then(|n| u8::try_from(n).ok())
    {
        base.max_qp = v;
    }
    if let Some(v) = obj
        .get("intra_qp")
        .and_then(Value::as_u64)
        .and_then(|n| u8::try_from(n).ok())
    {
        base.intra_qp = v;
    }
    if let Some(v) = obj
        .get("initial_qp")
        .and_then(Value::as_u64)
        .and_then(|n| u8::try_from(n).ok())
    {
        base.initial_qp = v;
    }
    if let Some(v) = obj
        .get("gop_frames")
        .and_then(Value::as_u64)
        .and_then(|n| u32::try_from(n).ok())
    {
        base.gop_frames = v;
    }
    if let Some(v) = obj
        .get("vbv_ms")
        .and_then(Value::as_u64)
        .and_then(|n| u32::try_from(n).ok())
    {
        base.vbv_ms = v;
    }
    base.validate().ok()?;
    Some(base)
}

fn path_cstring(path: &Path) -> StorageResult<CString> {
    CString::new(path.as_os_str().as_bytes())
        .map_err(|_| StorageError::invalid_input("invalid_path", "path contains NUL byte"))
}
