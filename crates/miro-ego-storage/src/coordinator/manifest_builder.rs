//! Zero-reread manifest construction, provenance log attachment (`clock-calibration.json` &
//! `camera-calibration.json`), collision-safe directory publish, and catalog admission.

use super::fsm::{CaptureCoordinator, SessionMetadata};
use super::types::{
    CapturedArtifact, CapturedAudioOutcome, CapturedAudioSegment, CapturedSessionArtifacts,
};
use crate::VolumeManager;
use crate::catalog::{
    DEVICE_SESSION_SCHEMA, DEVICE_SESSION_V2_SCHEMA, IndexedSession,
    parse_indexed_session_from_manifest,
};
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
        let staging_root = &ctx.staging_dir;
        let combined_logs = merge_session_logs(&captured.logs, &ctx.extra_logs);
        let declared_log_paths: Vec<String> =
            combined_logs.iter().map(|l| l.path.clone()).collect();
        let mut verified_count = 0_u64;
        let mut expected_identities: Vec<ExpectedArtifactIdentity> = Vec::new();
        let sealed_at = now_rfc3339_utc();
        let manifest_id = new_uuid_v7();
        let profile_str = match self.config.encoding.codec {
            VideoCodec::H264 => "high",
            VideoCodec::Hevc => "main",
        };
        let encoding_json = build_manifest_encoding_json(&self.config.encoding, profile_str);

        let manifest_value = if let Some(synced) = &captured.synced_fmp4 {
            if synced.sealed.frames == 0
                || synced.sealed.fragments == 0
                || synced.sealed.duration_ticks == 0
                || synced.sealed.bytes == 0
            {
                return Err(StorageError::integrity(
                    "empty_recording",
                    "cannot seal empty synced fmp4 recording",
                ));
            }
            let media_digest = finalize_artifact_with_digest(
                &staging_root.join(&synced.sealed.path),
                Some(synced.sealed.bytes),
                Some(synced.sealed.sha256.as_str()),
            )
            .map_err(|e| StorageError::integrity("seal_artifact_invalid", e.message))?;
            expected_identities.push(ExpectedArtifactIdentity {
                path: synced.sealed.path.clone(),
                identity: media_digest.identity,
            });
            verified_count = verified_count.saturating_add(1);
            self.update_verification_completed(verified_count);

            let has_audio = matches!(
                &captured.audio,
                CapturedAudioOutcome::Recorded { .. }
                    | CapturedAudioOutcome::RecordedWithClock { .. }
            ) && synced.sealed.audio_samples > 0;
            let meta_track_id = if has_audio { 4_u64 } else { 3_u64 };
            let timescale = synced.sealed.timescale.max(1);
            let duration_clock = if has_audio {
                "audio_hardware_samples"
            } else {
                "host_monotonic"
            };
            let duration_seconds = if has_audio {
                (synced.sealed.duration_ticks as f64) / (timescale as f64)
            } else {
                captured.duration_seconds.max(0.0)
            };

            let audio_json = match (&captured.audio, has_audio) {
                (
                    CapturedAudioOutcome::Recorded {
                        requested_mode,
                        sample_rate,
                        channels,
                        ..
                    }
                    | CapturedAudioOutcome::RecordedWithClock {
                        requested_mode,
                        sample_rate,
                        channels,
                        ..
                    },
                    true,
                ) => {
                    let sr = synced.audio_sample_rate.unwrap_or(*sample_rate);
                    let ch = synced.audio_channels.unwrap_or(*channels);
                    let mut obj = serde_json::Map::new();
                    obj.insert("state".to_owned(), json!("recorded"));
                    obj.insert("requested_mode".to_owned(), json!(requested_mode));
                    obj.insert("resolved_mode".to_owned(), json!("enabled"));
                    obj.insert("codec".to_owned(), json!("flac"));
                    obj.insert("container".to_owned(), json!("fmp4"));
                    obj.insert("track_id".to_owned(), json!(3));
                    obj.insert("sample_format".to_owned(), json!("S16_LE"));
                    obj.insert("sample_rate".to_owned(), json!(sr));
                    obj.insert("channels".to_owned(), json!(ch));
                    obj.insert(
                        "sample_count".to_owned(),
                        json!(synced.sealed.audio_samples),
                    );
                    obj.insert(
                        "sync".to_owned(),
                        json!({
                            "time_base": "audio_hardware_samples",
                            "timescale": timescale,
                            "boundary_gap_samples": 0,
                        }),
                    );
                    if let Some(cc) = synced
                        .audio_capture_clock
                        .as_ref()
                        .or_else(|| captured.audio.capture_clock())
                    {
                        obj.insert("capture_clock".to_owned(), cc.clone());
                    }
                    Value::Object(obj)
                }
                (
                    CapturedAudioOutcome::NotRecorded {
                        requested_mode,
                        reason,
                    },
                    _,
                ) => {
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
                (
                    CapturedAudioOutcome::Recorded { requested_mode, .. }
                    | CapturedAudioOutcome::RecordedWithClock { requested_mode, .. },
                    false,
                ) => {
                    json!({
                        "state": "not_recorded",
                        "requested_mode": requested_mode,
                        "resolved_mode": "disabled",
                        "reason": "recording_interrupted",
                    })
                }
            };

            let logs_json = self.finalize_logs_json(
                staging_root,
                &combined_logs,
                &mut expected_identities,
                &mut verified_count,
            )?;

            json!({
                "schema": DEVICE_SESSION_V2_SCHEMA,
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
                    "duration_seconds": duration_seconds,
                    "duration_clock": duration_clock,
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
                "media": {
                    "container": "fmp4",
                    "layout": "synced-fmp4",
                    "timescale": timescale,
                    "duration_ticks": synced.sealed.duration_ticks,
                    "fragment_count": synced.sealed.fragments,
                    "artifact": {
                        "artifact_id": media_digest.sha256,
                        "role": "media.session",
                        "path": synced.sealed.path,
                        "media_type": "video/mp4",
                        "bytes": media_digest.identity.size,
                        "sha256": media_digest.sha256,
                    },
                },
                "video": {
                    "layout": "split-eyes",
                    "codec": self.config.encoding.codec.as_str(),
                    "container": "fmp4",
                    "frame_count": synced.sealed.frames,
                    "tracks": {
                        "left_track_id": 1,
                        "right_track_id": 2,
                    },
                    "encoding": encoding_json,
                },
                "imu": {
                    "embedded_in": "media.session",
                    "track_id": meta_track_id,
                    "mime_type": "application/x-miro-ego-sync+json",
                    "sample_count": synced.sealed.imu_samples.max(captured.imu_sample_count),
                    "units": "raw_int16",
                    "coordinate_frame": "raw_device_axes",
                },
                "frames": {
                    "embedded_in": "media.session",
                    "track_id": meta_track_id,
                    "mime_type": "application/x-miro-ego-sync+json",
                    "count": synced.sealed.frames,
                },
                "audio": audio_json,
                "logs": logs_json,
                "integrity": {
                    "verified_at": sealed_at,
                    "dropped_frames": captured.dropped_frames,
                    "drop_events": captured.drop_events,
                    "fatal_errors": [],
                },
            })
        } else {
            if captured.segments.is_empty() {
                return Err(StorageError::integrity(
                    "empty_recording",
                    "cannot seal recording with zero stereo segments",
                ));
            }

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

            let logs_json = self.finalize_logs_json(
                staging_root,
                &combined_logs,
                &mut expected_identities,
                &mut verified_count,
            )?;

            json!({
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
                    "encoding": encoding_json,
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
            })
        };

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
            let _ = remove_control_markers_with_logs(&final_root, &declared_log_paths);
            if captured.synced_fmp4.is_some() {
                let _ = fs::remove_dir(final_root.join("video"));
            }
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

    fn finalize_logs_json(
        &self,
        staging_root: &Path,
        combined_logs: &[CapturedArtifact],
        expected_identities: &mut Vec<ExpectedArtifactIdentity>,
        verified_count: &mut u64,
    ) -> StorageResult<Vec<Value>> {
        let mut logs_json = Vec::with_capacity(combined_logs.len());
        for log_art in combined_logs {
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
            *verified_count = verified_count.saturating_add(1);
            self.update_verification_completed(*verified_count);
            logs_json.push(json!({
                "artifact_id": ldig.sha256,
                "role": log_art.role,
                "path": log_art.path,
                "media_type": log_art.media_type,
                "bytes": ldig.identity.size,
                "sha256": ldig.sha256,
            }));
        }
        Ok(logs_json)
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
    let merged_logs = merge_session_logs(&captured.logs, extra_logs);
    if captured.synced_fmp4.is_some() {
        return 1_u64.saturating_add(merged_logs.len() as u64);
    }
    let audio_count = match &captured.audio {
        CapturedAudioOutcome::Recorded { segments, .. }
        | CapturedAudioOutcome::RecordedWithClock { segments, .. } => segments.len() as u64,
        CapturedAudioOutcome::NotRecorded { .. } => 0,
    };
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
    if let Some(synced) = &captured.synced_fmp4 {
        let mut total = synced.sealed.bytes;
        for log in merge_session_logs(&captured.logs, extra_logs) {
            total = total.saturating_add(log.bytes);
        }
        return total;
    }
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
