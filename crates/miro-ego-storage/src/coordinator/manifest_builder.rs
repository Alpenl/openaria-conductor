//! Zero-reread manifest construction, provenance log attachment (`clock-calibration.json` &
//! `camera-calibration.json`), collision-safe directory publish, and catalog admission.

use super::fsm::{CaptureCoordinator, SessionMetadata};
use super::types::{
    CapturedArtifact, CapturedAudioOutcome, CapturedAudioSegment, CapturedSessionArtifacts,
};
use crate::VolumeManager;
use crate::catalog::{DEVICE_SESSION_SCHEMA, IndexedSession, parse_indexed_session_from_manifest};
use crate::recovery::{
    build_manifest_encoding_json, cleanup_empty_date_parent, is_transient_storage_error,
    publish_staging_directory, quarantine_session_directory, remove_control_markers_with_logs,
};
use crate::util::{now_rfc3339_utc, sha256_hex};
use crate::{StorageError, StorageResult};
use miro_ego_core::artifact_verify::{
    ExpectedArtifactIdentity, finalize_artifact_with_digest, verify_device_session_artifacts,
};
use miro_ego_core::config::VideoCodec;
use miro_ego_core::ids::new_uuid_v7;
use miro_ego_core::paths::DataVolumePaths;
use miro_ego_core::sys::fs::{fsync_directory, read_bounded_json, write_bytes_atomic};
use serde_json::{Value, json};
use std::fs;
use std::path::Path;

pub(super) struct SealedPublishOutput {
    pub manifest_id: String,
    pub manifest_sha256: String,
    pub sealed_at: String,
}

impl CaptureCoordinator {
    pub(super) fn update_verification_completed(&self, completed: u64) {
        if let Ok(mut guard) = self.state.lock()
            && let Some(ctx) = guard.active.as_mut()
            && let Some(ver) = ctx.verification_progress.as_mut()
        {
            ver.completed = completed.min(ver.total);
        }
    }

    pub(super) fn seal_and_publish(
        &self,
        ctx: &SessionMetadata,
        captured: &CapturedSessionArtifacts,
    ) -> StorageResult<SealedPublishOutput> {
        if captured.segments.is_empty() {
            return Err(StorageError::integrity(
                "empty_recording",
                "cannot seal recording with zero stereo segments",
            ));
        }

        let staging_root = &ctx.staging_dir;
        let combined_logs = merge_session_logs(&captured.logs, &ctx.extra_logs);
        let declared_log_paths: Vec<String> =
            combined_logs.iter().map(|l| l.path.clone()).collect();
        let mut verified_count = 0_u64;
        let mut expected_identities: Vec<ExpectedArtifactIdentity> = Vec::new();
        let mut segments_json = Vec::with_capacity(captured.segments.len());

        for seg in &captured.segments {
            let left_digest = finalize_artifact_with_digest(
                &staging_root.join(&seg.left_path),
                Some(seg.left_bytes),
                seg.left_sha256.as_deref(),
            )
            .map_err(|e| StorageError::integrity("seal_artifact_invalid", e.message))?;
            let right_digest = finalize_artifact_with_digest(
                &staging_root.join(&seg.right_path),
                Some(seg.right_bytes),
                seg.right_sha256.as_deref(),
            )
            .map_err(|e| StorageError::integrity("seal_artifact_invalid", e.message))?;

            expected_identities.push(ExpectedArtifactIdentity {
                path: seg.left_path.clone(),
                identity: left_digest.identity,
            });
            expected_identities.push(ExpectedArtifactIdentity {
                path: seg.right_path.clone(),
                identity: right_digest.identity,
            });
            verified_count = verified_count.saturating_add(2);
            self.update_verification_completed(verified_count);

            segments_json.push(json!({
                "index": seg.index,
                "start_frame": seg.start_frame,
                "end_frame": seg.end_frame,
                "start_time_seconds": seg.start_time_seconds,
                "end_time_seconds": seg.end_time_seconds,
                "artifacts": {
                    "left": {
                        "artifact_id": left_digest.sha256,
                        "role": "video.left",
                        "path": seg.left_path,
                        "media_type": "video/mp4",
                        "bytes": left_digest.identity.size,
                        "sha256": left_digest.sha256,
                    },
                    "right": {
                        "artifact_id": right_digest.sha256,
                        "role": "video.right",
                        "path": seg.right_path,
                        "media_type": "video/mp4",
                        "bytes": right_digest.identity.size,
                        "sha256": right_digest.sha256,
                    }
                }
            }));
        }

        let frames_digest = finalize_artifact_with_digest(
            &staging_root.join(&captured.frames_artifact.path),
            Some(captured.frames_artifact.bytes),
            captured.frames_artifact.sha256.as_deref(),
        )
        .map_err(|e| StorageError::integrity("seal_artifact_invalid", e.message))?;
        expected_identities.push(ExpectedArtifactIdentity {
            path: captured.frames_artifact.path.clone(),
            identity: frames_digest.identity,
        });
        verified_count = verified_count.saturating_add(1);
        self.update_verification_completed(verified_count);

        let imu_digest = finalize_artifact_with_digest(
            &staging_root.join(&captured.imu_artifact.path),
            Some(captured.imu_artifact.bytes),
            captured.imu_artifact.sha256.as_deref(),
        )
        .map_err(|e| StorageError::integrity("seal_artifact_invalid", e.message))?;
        expected_identities.push(ExpectedArtifactIdentity {
            path: captured.imu_artifact.path.clone(),
            identity: imu_digest.identity,
        });
        verified_count = verified_count.saturating_add(1);
        self.update_verification_completed(verified_count);

        let audio_json = match &captured.audio {
            CapturedAudioOutcome::Recorded {
                requested_mode,
                sample_rate,
                channels,
                sample_count,
                start_time_seconds,
                end_time_seconds,
                segments,
            } => self.finalize_recorded_audio_json(
                staging_root,
                requested_mode,
                *sample_rate,
                *channels,
                *sample_count,
                *start_time_seconds,
                *end_time_seconds,
                segments,
                None,
                &mut expected_identities,
                &mut verified_count,
            )?,
            CapturedAudioOutcome::RecordedWithClock {
                requested_mode,
                sample_rate,
                channels,
                sample_count,
                start_time_seconds,
                end_time_seconds,
                segments,
                capture_clock,
            } => self.finalize_recorded_audio_json(
                staging_root,
                requested_mode,
                *sample_rate,
                *channels,
                *sample_count,
                *start_time_seconds,
                *end_time_seconds,
                segments,
                capture_clock.as_ref(),
                &mut expected_identities,
                &mut verified_count,
            )?,
            CapturedAudioOutcome::NotRecorded {
                requested_mode,
                reason,
            } => {
                clean_leftover_not_recorded_audio_files(
                    self.volume.paths(),
                    staging_root,
                    &ctx.session_id,
                    &declared_log_paths,
                );
                json!({
                    "state": "not_recorded",
                    "requested_mode": requested_mode,
                    "resolved_mode": "disabled",
                    "reason": reason,
                })
            }
        };

        let mut logs_json = Vec::with_capacity(combined_logs.len());
        for log_art in &combined_logs {
            let ldig = finalize_artifact_with_digest(
                &staging_root.join(&log_art.path),
                Some(log_art.bytes),
                log_art.sha256.as_deref(),
            )
            .map_err(|e| StorageError::integrity("seal_artifact_invalid", e.message))?;
            expected_identities.push(ExpectedArtifactIdentity {
                path: log_art.path.clone(),
                identity: ldig.identity,
            });
            verified_count = verified_count.saturating_add(1);
            self.update_verification_completed(verified_count);
            logs_json.push(json!({
                "artifact_id": ldig.sha256,
                "role": log_art.role,
                "path": log_art.path,
                "media_type": log_art.media_type,
                "bytes": ldig.identity.size,
                "sha256": ldig.sha256,
            }));
        }

        let sealed_at = now_rfc3339_utc();
        let manifest_id = new_uuid_v7();
        let profile_str = match self.config.encoding.codec {
            VideoCodec::H264 => "high",
            VideoCodec::Hevc => "main",
        };
        let manifest_value = json!({
            "schema": DEVICE_SESSION_SCHEMA,
            "manifest_id": manifest_id,
            "sealed": true,
            "sealed_at": sealed_at,
            "session_id": ctx.session_id,
            "volume_id": ctx.admission.volume_id,
            "capture_mode": ctx.mode.as_str(),
            "display_name": ctx.display_name,
            "device": {
                "device_id": self.config.device_id,
                "device_label": self.config.device_label,
                "hardware_fingerprint": self.config.hardware_fingerprint,
                "platform": self.config.platform,
                "software_version": self.config.software_version,
                "commit": self.config.commit,
            },
            "time": {
                "started_at": ctx.started_at,
                "ended_at": sealed_at,
                "timezone": self.config.timezone,
                "duration_seconds": captured.duration_seconds.max(0.0),
                "duration_clock": "host_monotonic",
            },
            "take": {
                "take_id": ctx.take_id,
                "sequence": ctx.take_sequence,
                "continuation_of": ctx.continuation_of,
            },
            "camera": {
                "width": self.config.camera_width,
                "height": self.config.camera_height,
                "eye_width": (self.config.camera_width / 2).max(1),
                "sensor_fps": self.config.sensor_fps,
                "frame_decimation": self.config.frame_decimation,
                "effective_fps": captured.effective_fps.max(0.0),
                "coordinate_frame": "opencv_optical",
            },
            "video": {
                "layout": "split-eyes",
                "codec": self.config.encoding.codec.as_str(),
                "container": "mp4",
                "segments": segments_json,
                "encoding": build_manifest_encoding_json(&self.config.encoding, profile_str),
            },
            "imu": {
                "artifact": {
                    "artifact_id": imu_digest.sha256,
                    "role": "imu.samples",
                    "path": captured.imu_artifact.path,
                    "media_type": captured.imu_artifact.media_type,
                    "bytes": imu_digest.identity.size,
                    "sha256": imu_digest.sha256,
                },
                "sample_count": captured.imu_sample_count,
                "units": "raw_int16",
                "coordinate_frame": "raw_device_axes",
            },
            "frames": {
                "artifact": {
                    "artifact_id": frames_digest.sha256,
                    "role": "frames.index",
                    "path": captured.frames_artifact.path,
                    "media_type": captured.frames_artifact.media_type,
                    "bytes": frames_digest.identity.size,
                    "sha256": frames_digest.sha256,
                },
                "count": captured.frames_count,
            },
            "audio": audio_json,
            "logs": logs_json,
            "integrity": {
                "verified_at": sealed_at,
                "dropped_frames": captured.dropped_frames,
                "drop_events": captured.drop_events,
                "fatal_errors": [],
            },
        });

        let mut manifest_bytes =
            serde_json::to_vec_pretty(&manifest_value).map_err(|err| StorageError::Catalog {
                message: format!("serialize session manifest failed: {err}"),
            })?;
        manifest_bytes.push(b'\n');
        let manifest_sha256 = sha256_hex(&manifest_bytes);

        verify_device_session_artifacts(
            staging_root,
            &manifest_bytes,
            &ctx.session_id,
            &expected_identities,
        )
        .map_err(|e| StorageError::integrity("seal_verify_failed", e.message))?;

        let manifest_path = staging_root.join("manifest.json");
        write_bytes_atomic(&manifest_path, &manifest_bytes, 0o640, true).map_err(|err| {
            StorageError::io("manifest_write_failed", "write sealed manifest.json", err)
        })?;
        let _ = fsync_directory(staging_root);

        let relative_dir =
            match publish_staging_directory(self.volume.paths(), staging_root, &ctx.started_at) {
                Ok(rel) => rel,
                Err(pub_err) => {
                    let _ = fs::remove_file(&manifest_path);
                    let _ = fsync_directory(staging_root);
                    return Err(pub_err);
                }
            };
        let final_root = self.volume.paths().recordings_dir().join(&relative_dir);

        let verify_published_res: StorageResult<IndexedSession> = (|| {
            remove_control_markers_with_logs(&final_root, &declared_log_paths)?;
            if combined_logs.is_empty() {
                let _ = fs::remove_dir(final_root.join("logs"));
            }
            let _ = fsync_directory(&final_root);

            verify_device_session_artifacts(
                &final_root,
                &manifest_bytes,
                &ctx.session_id,
                &expected_identities,
            )
            .map_err(|e| StorageError::integrity("post_publish_verify_failed", e.message))?;

            let mut indexed = parse_indexed_session_from_manifest(
                &relative_dir,
                &manifest_value,
                &manifest_sha256,
            )?;
            let build_sha256 = sha256_hex(
                format!(
                    "miro-ego:{}:{}",
                    self.config.software_version, self.config.commit
                )
                .as_bytes(),
            );
            indexed.verification = Some(crate::catalog::GatewayVerification {
                actor: "gateway".to_owned(),
                validator: crate::catalog::GatewayValidatorInfo {
                    name: "miro-ego".to_owned(),
                    version: self.config.software_version.clone(),
                    build_sha256,
                },
                manifest_sha256: manifest_sha256.clone(),
                verified_at: sealed_at.clone(),
                verdict: "usable".to_owned(),
                diagnostics: Vec::new(),
            });
            Ok(indexed)
        })();

        let indexed = match verify_published_res {
            Ok(idx) => idx,
            Err(post_err) => {
                if !is_transient_storage_error(&post_err) {
                    let q_base = format!("{}.postpublish", ctx.session_id);
                    if quarantine_session_directory(self.volume.paths(), &final_root, &q_base)
                        .is_ok()
                    {
                        cleanup_empty_date_parent(
                            &self.volume.paths().recordings_dir(),
                            &final_root,
                        );
                    }
                }
                return Err(post_err);
            }
        };

        if let Err(admit_err) = self.catalog.admit_indexed_session(indexed.clone()) {
            if is_transient_storage_error(&admit_err) {
                self.catalog.admit_in_memory(indexed)?;
            } else {
                let q_base = format!("{}.postpublish", ctx.session_id);
                if quarantine_session_directory(self.volume.paths(), &final_root, &q_base).is_ok() {
                    cleanup_empty_date_parent(&self.volume.paths().recordings_dir(), &final_root);
                }
                return Err(admit_err);
            }
        }

        Ok(SealedPublishOutput {
            manifest_id,
            manifest_sha256,
            sealed_at,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn finalize_recorded_audio_json(
        &self,
        staging_root: &Path,
        requested_mode: &str,
        sample_rate: u32,
        channels: u16,
        sample_count: u64,
        start_time_seconds: f64,
        end_time_seconds: f64,
        segments: &[CapturedAudioSegment],
        capture_clock: Option<&Value>,
        expected_identities: &mut Vec<ExpectedArtifactIdentity>,
        verified_count: &mut u64,
    ) -> StorageResult<Value> {
        let mut audio_segs_json = Vec::with_capacity(segments.len());
        for aseg in segments {
            let adig = finalize_artifact_with_digest(
                &staging_root.join(&aseg.artifact.path),
                Some(aseg.artifact.bytes),
                aseg.artifact.sha256.as_deref(),
            )
            .map_err(|e| StorageError::integrity("seal_artifact_invalid", e.message))?;
            expected_identities.push(ExpectedArtifactIdentity {
                path: aseg.artifact.path.clone(),
                identity: adig.identity,
            });
            *verified_count = verified_count.saturating_add(1);
            self.update_verification_completed(*verified_count);
            audio_segs_json.push(json!({
                "index": aseg.index,
                "start_sample": aseg.start_sample,
                "end_sample": aseg.end_sample,
                "start_time_seconds": aseg.start_time_seconds,
                "end_time_seconds": aseg.end_time_seconds,
                "pcm_payload_bytes": aseg.pcm_payload_bytes,
                "wav_header_bytes": aseg.wav_header_bytes,
                "artifact": {
                    "artifact_id": adig.sha256,
                    "role": "audio.wav",
                    "path": aseg.artifact.path,
                    "media_type": aseg.artifact.media_type,
                    "bytes": adig.identity.size,
                    "sha256": adig.sha256,
                }
            }));
        }

        let disk_clock = if capture_clock.is_none() {
            read_bounded_json::<Value>(
                &staging_root.join("audio/capture-clock.json"),
                64 * 1024,
                16,
            )
            .ok()
            .filter(|v| v.get("schema").and_then(Value::as_str) == Some("miro-ego.audio-clock.v1"))
        } else {
            None
        };

        let mut obj = json!({
            "state": "recorded",
            "requested_mode": requested_mode,
            "resolved_mode": "enabled",
            "codec": "pcm_s16le",
            "container": "wav",
            "sample_format": "S16_LE",
            "sample_rate": sample_rate,
            "channels": channels,
            "sample_count": sample_count,
            "sync": {
                "time_base": "host_monotonic",
                "start_time_seconds": start_time_seconds,
                "end_time_seconds": end_time_seconds,
                "video_time_reference": "session_time_seconds",
            },
            "segments": audio_segs_json,
        });
        if let Some(clk) = capture_clock.or(disk_clock.as_ref())
            && clk.get("schema").and_then(Value::as_str) == Some("miro-ego.audio-clock.v1")
            && let Some(map) = obj.as_object_mut()
        {
            map.insert("capture_clock".to_owned(), clk.clone());
        }
        Ok(obj)
    }
}

pub(super) fn merge_session_logs(
    captured_logs: &[CapturedArtifact],
    extra_logs: &[CapturedArtifact],
) -> Vec<CapturedArtifact> {
    let mut out = captured_logs.to_vec();
    for extra in extra_logs {
        if !out.iter().any(|existing| existing.path == extra.path) {
            out.push(extra.clone());
        }
    }
    out
}

pub(super) fn count_captured_artifacts(
    captured: &CapturedSessionArtifacts,
    extra_logs: &[CapturedArtifact],
) -> u64 {
    let audio_count = match &captured.audio {
        CapturedAudioOutcome::Recorded { segments, .. }
        | CapturedAudioOutcome::RecordedWithClock { segments, .. } => segments.len() as u64,
        CapturedAudioOutcome::NotRecorded { .. } => 0,
    };
    let merged_logs = merge_session_logs(&captured.logs, extra_logs);
    (captured.segments.len() as u64)
        .saturating_mul(2)
        .saturating_add(2)
        .saturating_add(audio_count)
        .saturating_add(merged_logs.len() as u64)
}

pub(super) fn sum_captured_artifact_bytes(
    captured: &CapturedSessionArtifacts,
    extra_logs: &[CapturedArtifact],
) -> u64 {
    let mut total = captured
        .frames_artifact
        .bytes
        .saturating_add(captured.imu_artifact.bytes);
    for seg in &captured.segments {
        total = total
            .saturating_add(seg.left_bytes)
            .saturating_add(seg.right_bytes);
    }
    match &captured.audio {
        CapturedAudioOutcome::Recorded { segments, .. }
        | CapturedAudioOutcome::RecordedWithClock { segments, .. } => {
            for aseg in segments {
                total = total.saturating_add(aseg.artifact.bytes);
            }
        }
        CapturedAudioOutcome::NotRecorded { .. } => {}
    }
    for log in merge_session_logs(&captured.logs, extra_logs) {
        total = total.saturating_add(log.bytes);
    }
    total
}

fn clean_leftover_not_recorded_audio_files(
    paths: &DataVolumePaths,
    staging_root: &Path,
    session_id: &str,
    declared_log_paths: &[String],
) {
    let audio_dir = staging_root.join("audio");
    if !audio_dir.is_dir() {
        return;
    }
    let has_declared_audio_log = declared_log_paths
        .iter()
        .any(|p| p == "audio" || p.starts_with("audio/"));
    if !has_declared_audio_log {
        let q_name = format!("{session_id}.not-recorded-audio");
        if quarantine_session_directory(paths, &audio_dir, &q_name).is_ok() {
            let _ = fsync_directory(staging_root);
            return;
        }
    }

    let Ok(entries) = fs::read_dir(&audio_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        let rel = format!("audio/{name}");
        if declared_log_paths.iter().any(|p| p == &rel) {
            continue;
        }
        let path = entry.path();
        if let Ok(meta) = fs::symlink_metadata(&path) {
            if meta.is_dir() && !meta.file_type().is_symlink() {
                let _ = quarantine_session_directory(
                    paths,
                    &path,
                    &format!("{session_id}.audio-{name}"),
                )
                .or_else(|_| fs::remove_dir(&path).map(|()| String::new()));
            } else {
                let _ = fs::remove_file(&path);
            }
        }
    }
    let _ = fs::remove_dir(&audio_dir);
    let _ = fsync_directory(staging_root);
}
