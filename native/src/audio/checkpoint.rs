use crate::audio::{
    AudioError, AudioProgress, AudioSegment, DEFAULT_PERIOD_FRAMES, QUEUE_BLOCKS, clock,
};
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::sync::atomic::Ordering;

#[derive(serde::Serialize)]
struct CheckpointSegment<'a> {
    end_sample: u64,
    index: u64,
    path: &'a str,
    start_sample: u64,
}

#[derive(Default)]
pub(crate) struct CheckpointJournal {
    file: Option<File>,
    digest: Sha256,
    pub(crate) bytes: u64,
    records: u64,
    anchors: usize,
    segments: usize,
    failed: bool,
}

#[derive(serde::Serialize)]
struct CheckpointDelta<'a> {
    sequence: u64,
    anchors: &'a [clock::Anchor],
    segments: Vec<CheckpointSegment<'a>>,
}

#[derive(serde::Serialize)]
struct CheckpointHeader {
    schema: &'static str,
    journal_bytes: u64,
    journal_records: u64,
    journal_sha256: String,
    anchor_count: usize,
    segment_count: usize,
    last_anchor: Option<clock::Anchor>,
    sample_count: u64,
    thread_started_monotonic_ns: u64,
    period_frames: u64,
    buffer_frames: u64,
    queue_capacity_frames: u64,
    queue_peak_frames: u64,
    max_write_ns: u64,
}

pub(crate) fn commit_checkpoint(
    root: &Path,
    progress: &AudioProgress,
    records: &[AudioSegment],
    journal: &mut CheckpointJournal,
) -> Result<(), AudioError> {
    if journal.failed {
        return Err(AudioError::new(
            "write_failed",
            "audio checkpoint writer previously failed",
        ));
    }
    // Copy only the new observations while holding the capture lock. Neither
    // JSON encoding nor filesystem operations hold that lock or revisit history.
    let guard = progress
        .clock_checkpoint
        .lock()
        .map_err(|_| AudioError::new("audio_failed", "audio checkpoint mutex poisoned"))?;
    let Some(clock) = guard.as_ref() else {
        return Ok(());
    };
    let total_anchors = clock
        .base_index
        .checked_add(clock.anchors.len())
        .ok_or_else(|| AudioError::new("audio_failed", "audio checkpoint anchor count overflow"))?;
    if journal.anchors < clock.base_index || journal.anchors > total_anchors {
        return Err(AudioError::new(
            "audio_failed",
            "audio checkpoint anchors regressed",
        ));
    }
    let local_offset = journal.anchors - clock.base_index;
    let anchors = clock.anchors[local_offset..].to_vec();
    let mut head = CheckpointHeader {
        schema: "openaria.audio-checkpoint.v3",
        journal_bytes: 0,
        journal_records: journal.records + 1,
        journal_sha256: String::new(),
        anchor_count: total_anchors,
        segment_count: records.len(),
        last_anchor: clock.last_anchor,
        sample_count: records.last().map_or(0, |record| record.end_sample),
        thread_started_monotonic_ns: clock.started,
        period_frames: clock.period_frames,
        buffer_frames: clock.buffer_frames,
        queue_capacity_frames: QUEUE_BLOCKS as u64 * DEFAULT_PERIOD_FRAMES,
        queue_peak_frames: progress.queue_peak_frames.load(Ordering::Relaxed),
        max_write_ns: progress.max_write_ns.load(Ordering::Relaxed),
    };
    // Release the guard, not just its borrowed ClockCheckpoint reference.
    drop(guard);
    let delta = CheckpointDelta {
        sequence: journal.records,
        anchors: &anchors,
        segments: records
            .get(journal.segments..)
            .ok_or_else(|| AudioError::new("audio_failed", "audio checkpoint segments regressed"))?
            .iter()
            .map(|record| CheckpointSegment {
                index: record.index,
                path: &record.relative_path,
                start_sample: record.start_sample,
                end_sample: record.end_sample,
            })
            .collect(),
    };
    let mut bytes = serde_json::to_vec(&delta).map_err(|e| {
        AudioError::io(
            "write_failed",
            "encode audio checkpoint",
            io::Error::other(e),
        )
    })?;
    bytes.push(b'\n');
    let directory = root.join("audio");
    journal.failed = true; // Any failed append/publication is terminal; never reuse an unpublished tail.
    let file = match journal.file.as_mut() {
        Some(file) => file,
        None => {
            let opened = OpenOptions::new()
                .append(true)
                .create_new(true)
                .open(directory.join("checkpoint.ndjson"))
                .map_err(|e| {
                    AudioError::io("write_failed", "create audio checkpoint journal", e)
                })?;
            journal.file.insert(opened)
        }
    };
    file.write_all(&bytes)
        .and_then(|_| file.sync_all())
        .map_err(|e| AudioError::io("write_failed", "sync audio checkpoint journal", e))?;
    let mut digest = journal.digest.clone();
    digest.update(&bytes);
    head.journal_bytes = journal.bytes + bytes.len() as u64;
    head.journal_sha256 = format!("{:x}", digest.clone().finalize());
    let encoded = serde_json::to_vec(&head).map_err(|e| {
        AudioError::io(
            "write_failed",
            "encode audio checkpoint header",
            io::Error::other(e),
        )
    })?;
    let temporary = directory.join("checkpoint.tmp");
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(|e| AudioError::io("write_failed", "create audio checkpoint header", e))?;
    let commit_result = (|| -> io::Result<()> {
        output.write_all(&encoded)?;
        output.sync_all()?;
        drop(output);
        std::fs::rename(&temporary, directory.join("checkpoint.json"))?;
        File::open(&directory)?.sync_all()?;
        Ok(())
    })();
    if let Err(error) = commit_result {
        let _ = std::fs::remove_file(&temporary);
        return Err(AudioError::io(
            "write_failed",
            "commit audio checkpoint header",
            error,
        ));
    }
    journal.bytes = head.journal_bytes;
    journal.records = head.journal_records;
    journal.anchors = head.anchor_count;
    journal.segments = head.segment_count;
    journal.digest = digest;
    journal.failed = false;

    // Drain only the already-published prefix so capture-side anchor memory stays bounded
    // while preserving any newer anchors appended during disk I/O.
    if let Ok(mut guard) = progress.clock_checkpoint.lock() {
        if let Some(clock) = guard.as_mut() {
            if journal.anchors > clock.base_index {
                let drain_count = (journal.anchors - clock.base_index).min(clock.anchors.len());
                clock.anchors.drain(..drain_count);
                clock.base_index += drain_count;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audio::wav_writer::SegmentWriter;
    use crate::audio::{ClockCheckpoint, recorder::RecorderConfig};
    use std::path::PathBuf;

    fn anchor(sample: u64, time: u64) -> clock::Anchor {
        [sample, time, time, time, time]
    }

    fn tempfile_dir() -> PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "rp-ylx-audio-ckpt-test-{}-{}",
            std::process::id(),
            unique
        ));
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    fn expanded_checkpoint(root: &Path) -> serde_json::Value {
        let mut head: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.join("audio/checkpoint.json")).unwrap())
                .unwrap();
        let journal = std::fs::read(root.join("audio/checkpoint.ndjson")).unwrap();
        let prefix = &journal[..head["journal_bytes"].as_u64().unwrap() as usize];
        assert_eq!(
            format!("{:x}", Sha256::digest(prefix)),
            head["journal_sha256"]
        );
        let mut anchors = Vec::new();
        let mut segments = Vec::new();
        for (i, line) in prefix
            .split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
            .enumerate()
        {
            let row: serde_json::Value = serde_json::from_slice(line).unwrap();
            assert_eq!(row["sequence"], i);
            anchors.extend(row["anchors"].as_array().unwrap().clone());
            segments.extend(row["segments"].as_array().unwrap().clone());
        }
        assert_eq!(
            anchors.len(),
            head["anchor_count"].as_u64().unwrap() as usize
        );
        assert_eq!(
            segments.len(),
            head["segment_count"].as_u64().unwrap() as usize
        );
        if !head["last_anchor"].is_null() && anchors.last() != Some(&head["last_anchor"]) {
            anchors.push(head["last_anchor"].clone());
        }
        for key in [
            "journal_bytes",
            "journal_records",
            "journal_sha256",
            "anchor_count",
            "segment_count",
            "last_anchor",
        ] {
            head.as_object_mut().unwrap().remove(key);
        }
        head["schema"] = serde_json::json!("openaria.audio-checkpoint.v2");
        head["anchors"] = serde_json::json!(anchors);
        head["segments"] = serde_json::json!(segments);
        head
    }

    #[test]
    fn incremental_checkpoint_io_and_failed_publication_preserve_recoverable_prefix() {
        let root = tempfile_dir();
        std::fs::create_dir(root.join("audio")).unwrap();
        let progress = AudioProgress::default();
        let mut journal = CheckpointJournal::default();
        let mut records = Vec::new();
        let mut previous_bytes = 0;
        for index in 0..20_u64 {
            *progress.clock_checkpoint.lock().unwrap() = Some(ClockCheckpoint {
                started: 1_000_000_000,
                period_frames: 1024,
                buffer_frames: 8192,
                base_index: 0,
                anchors: (0..(index + 1) * 50)
                    .map(|n| anchor(n * 4800, 1_000_000_000 + n * 100_000_000))
                    .collect(),
                last_anchor: Some(anchor(
                    (index + 1) * 240000,
                    1_000_000_000 + (index + 1) * 5_000_000_000,
                )),
            });
            records.push(AudioSegment {
                index,
                relative_path: format!("audio/audio_{index:05}.wav"),
                start_sample: index * 240000,
                end_sample: (index + 1) * 240000,
                start_time_seconds: (index * 5) as f64,
                end_time_seconds: ((index + 1) * 5) as f64,
            });
            commit_checkpoint(&root, &progress, &records, &mut journal).unwrap();
            assert!(
                journal.bytes - previous_bytes < 10000,
                "a commit rewrote prior observations"
            );
            assert!(
                std::fs::metadata(root.join("audio/checkpoint.json"))
                    .unwrap()
                    .len()
                    < 1024
            );
            previous_bytes = journal.bytes;
        }
        let head = std::fs::read(root.join("audio/checkpoint.json")).unwrap();
        std::fs::create_dir(root.join("audio/checkpoint.tmp")).unwrap();
        // The delta reaches disk, but publication fails. An old header must
        // still select precisely its old prefix, never the uncommitted tail.
        commit_checkpoint(&root, &progress, &records, &mut journal).unwrap_err();
        assert_eq!(
            std::fs::read(root.join("audio/checkpoint.json")).unwrap(),
            head
        );
        assert!(
            std::fs::metadata(root.join("audio/checkpoint.ndjson"))
                .unwrap()
                .len()
                > previous_bytes
        );
        assert!(commit_checkpoint(&root, &progress, &records, &mut journal).is_err());
        let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .to_owned();
        let result = std::process::Command::new(
            std::env::var("PYO3_PYTHON").unwrap_or_else(|_| "python3".to_owned()),
        )
        .current_dir(&repo)
        .env("PYTHONPATH", repo.join("src"))
        .arg("-c")
        .arg("import json,sys; from pathlib import Path; from rp_ylx.recording.audio_journal import AudioJournalReader; p=Path(sys.argv[1]); h=json.loads((p/'audio/checkpoint.json').read_bytes()); v=AudioJournalReader(p).read(h); assert len(v['anchors']) == 1001; assert len(v['segments']) == 20; assert v['sample_count'] == 4800000; assert v['anchors'][-1][0] == 4800000")
        .arg(&root)
        .output()
        .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn long_recording_checkpoint_publication_is_bounded() {
        let root = tempfile_dir();
        let config = RecorderConfig {
            session_root: root.clone(),
            device: "hw:0,0".to_owned(),
            sample_rate_hz: 10,
            channels: 2,
            segment_seconds: 0.3,
        };
        let progress = std::sync::Arc::new(AudioProgress::default());
        *progress.clock_checkpoint.lock().unwrap() = Some(ClockCheckpoint {
            started: 1_000_000_000,
            period_frames: 1,
            buffer_frames: 8,
            base_index: 0,
            anchors: (0..100_000).map(|i| anchor(i, 1_000_000_000 + i)).collect(),
            last_anchor: Some(anchor(100_000, 1_000_100_000)),
        });
        let mut writer = SegmentWriter::with_progress(&config, progress.clone()).unwrap();
        let mut written = 0;
        writer.write_frames(&[7; 20], 5, &mut written).unwrap();
        drop(writer);
        let bytes = std::fs::metadata(root.join("audio/checkpoint.json"))
            .unwrap()
            .len();
        assert!(
            bytes <= 4096,
            "commit header rewrites historical anchors: {bytes} bytes"
        );
        assert_eq!(progress.durable_samples.load(Ordering::Acquire), 3);
        let guard = progress.clock_checkpoint.lock().unwrap();
        let clock = guard.as_ref().unwrap();
        assert!(clock.anchors.is_empty());
        assert_eq!(clock.base_index, 100_000);
        drop(guard);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn checkpoint_journal_preserves_all_clock_anchors_and_segment_fields() {
        let root = tempfile_dir();
        std::fs::create_dir(root.join("audio")).unwrap();
        let progress = AudioProgress::default();
        progress.queue_peak_frames.store(123, Ordering::Relaxed);
        progress.max_write_ns.store(456, Ordering::Relaxed);
        let anchors = vec![anchor(0, 1_000_000_000), anchor(u64::MAX - 1, u64::MAX - 1)];
        let records = vec![AudioSegment {
            index: 3,
            relative_path: "audio/引号\"换行\n反斜线\\.wav".to_owned(),
            start_sample: 0,
            end_sample: u64::MAX,
            start_time_seconds: 0.0,
            end_time_seconds: 5.0,
        }];
        let mut journal = CheckpointJournal::default();
        for last_anchor in [None, Some(anchors[1]), Some(anchor(u64::MAX, u64::MAX))] {
            *progress.clock_checkpoint.lock().unwrap() = Some(ClockCheckpoint {
                started: 1_000_000_000,
                period_frames: 1024,
                buffer_frames: 8192,
                base_index: 0,
                anchors: anchors.clone(),
                last_anchor,
            });
            commit_checkpoint(&root, &progress, &records, &mut journal).unwrap();
            let mut expected_anchors = anchors.clone();
            if let Some(anchor) = last_anchor {
                if expected_anchors.last() != Some(&anchor) {
                    expected_anchors.push(anchor);
                }
            }
            let expected = serde_json::json!({
                "schema": "openaria.audio-checkpoint.v2",
                "sample_count": u64::MAX,
                "thread_started_monotonic_ns": 1_000_000_000_u64,
                "period_frames": 1024, "buffer_frames": 8192,
                "anchors": expected_anchors,
                "queue_capacity_frames": QUEUE_BLOCKS as u64 * DEFAULT_PERIOD_FRAMES,
                "queue_peak_frames": 123, "max_write_ns": 456,
                "segments": [{"index": 3, "path": records[0].relative_path,
                    "start_sample": 0, "end_sample": u64::MAX}],
            });
            let saved = expanded_checkpoint(&root);
            assert_eq!(saved, expected);
            let guard = progress.clock_checkpoint.lock().unwrap();
            let clock = guard.as_ref().unwrap();
            assert!(clock.anchors.is_empty());
            assert_eq!(clock.base_index, anchors.len());
            assert!(!root.join("audio/checkpoint.tmp").exists());
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn checkpoint_streaming_write_and_flush_failures_preserve_previous_checkpoint() {
        for anchor_count in [1, 10_000] {
            let root = tempfile_dir();
            std::fs::create_dir(root.join("audio")).unwrap();
            std::fs::write(root.join("audio/checkpoint.json"), b"previous checkpoint").unwrap();
            std::os::unix::fs::symlink("/dev/full", root.join("audio/checkpoint.tmp")).unwrap();
            let progress = AudioProgress::default();
            *progress.clock_checkpoint.lock().unwrap() = Some(ClockCheckpoint {
                started: 1_000_000_000,
                period_frames: 1024,
                buffer_frames: 8192,
                base_index: 0,
                anchors: (0..anchor_count)
                    .map(|i| anchor(i, 1_000_000_000 + i))
                    .collect(),
                last_anchor: None,
            });
            let error = commit_checkpoint(&root, &progress, &[], &mut CheckpointJournal::default())
                .unwrap_err();
            assert_eq!(error.code, "write_failed");
            assert!(error.message.contains("audio checkpoint"));
            assert_eq!(
                std::fs::read(root.join("audio/checkpoint.json")).unwrap(),
                b"previous checkpoint"
            );
            assert_eq!(progress.durable_samples.load(Ordering::Acquire), 0);
            let guard = progress.clock_checkpoint.lock().unwrap();
            assert_eq!(guard.as_ref().unwrap().anchors.len(), anchor_count as usize);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}
