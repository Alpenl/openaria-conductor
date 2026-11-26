//! Voice feedback controller with 0..=100 volume scaling, atomic settings persistence,
//! and trait-abstracted `aplay` audio playback.

use crate::{HwError, HwResult, VoiceCue, VoiceFeedback};
use miro_ego_core::sys::fs::{read_bounded_bytes, read_bounded_json, write_bytes_atomic};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub const VOICE_STATUS_SCHEMA: &str = "miro-ego.voice.v1";
pub const VOICE_SETTINGS_SCHEMA: &str = "miro-ego.voice-settings.v1";
pub const DEFAULT_VOICE_DEVICE: &str = "plughw:CARD=duplexaudio,DEV=0";
pub const MAX_VOICE_SETTINGS_BYTES: usize = 8 * 1024;
pub const MAX_WAV_ASSET_BYTES: usize = 4 * 1024 * 1024;
pub const DEFAULT_PLAYBACK_TIMEOUT: Duration = Duration::from_secs(5);

/// Public voice status matching OpenAPI `VoiceStatus` (`miro-ego.voice.v1`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VoiceStatus {
    pub schema: String,
    pub enabled: bool,
    pub volume: u8,
}

/// Persisted voice configuration stored atomically on the system disk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VoiceSettings {
    pub schema: String,
    pub enabled: bool,
    pub volume: u8,
}

impl Default for VoiceSettings {
    fn default() -> Self {
        Self {
            schema: VOICE_SETTINGS_SCHEMA.to_owned(),
            enabled: false,
            volume: 80,
        }
    }
}

impl VoiceSettings {
    pub fn validate(&self) -> HwResult<()> {
        if self.schema != VOICE_SETTINGS_SCHEMA {
            return Err(HwError::Io {
                message: format!(
                    "voice settings schema must be {VOICE_SETTINGS_SCHEMA}, got {}",
                    self.schema
                ),
            });
        }
        if self.volume > 100 {
            return Err(HwError::Io {
                message: format!("voice volume must be in 0..=100, got {}", self.volume),
            });
        }
        Ok(())
    }

    pub fn to_status(&self) -> VoiceStatus {
        VoiceStatus {
            schema: VOICE_STATUS_SCHEMA.to_owned(),
            enabled: self.enabled,
            volume: self.volume,
        }
    }
}

/// Trait-abstracted WAV audio playback backend (`aplay` in production, mock in tests).
pub trait AudioPlayer: Send + Sync {
    fn play_wav(&self, device: &str, wav_bytes: &[u8], timeout: Duration) -> HwResult<()>;
}

/// Real ALSA `aplay` backend piping scaled PCM16 WAV bytes on stdin with a bounded timeout.
#[derive(Debug, Clone)]
pub struct AplayAudioPlayer {
    aplay_bin: PathBuf,
}

impl Default for AplayAudioPlayer {
    fn default() -> Self {
        Self {
            aplay_bin: PathBuf::from("/usr/bin/aplay"),
        }
    }
}

impl AplayAudioPlayer {
    pub fn with_binary(path: impl Into<PathBuf>) -> Self {
        Self {
            aplay_bin: path.into(),
        }
    }
}

impl AudioPlayer for AplayAudioPlayer {
    fn play_wav(&self, device: &str, wav_bytes: &[u8], timeout: Duration) -> HwResult<()> {
        let mut child = Command::new(&self.aplay_bin)
            .args(["-q", "-D", device, "-"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|err| {
                if err.kind() == std::io::ErrorKind::NotFound {
                    HwError::Unavailable {
                        message: format!("aplay binary unavailable: {}", self.aplay_bin.display()),
                    }
                } else {
                    HwError::Io {
                        message: format!("failed to spawn aplay: {err}"),
                    }
                }
            })?;

        if let Some(mut stdin) = child.stdin.take() {
            stdin.write_all(wav_bytes).map_err(|err| {
                let _ = child.kill();
                let _ = child.wait();
                HwError::Io {
                    message: format!("failed writing WAV bytes to aplay stdin: {err}"),
                }
            })?;
        }

        let deadline = Instant::now() + timeout;
        loop {
            match child.try_wait() {
                Ok(Some(status)) => {
                    if status.success() {
                        return Ok(());
                    }
                    return Err(HwError::Io {
                        message: format!("aplay exited with status {:?}", status.code()),
                    });
                }
                Ok(None) => {
                    if Instant::now() >= deadline {
                        let _ = child.kill();
                        let _ = child.wait();
                        return Err(HwError::Io {
                            message: "aplay playback timed out".to_owned(),
                        });
                    }
                    std::thread::sleep(Duration::from_millis(20));
                }
                Err(err) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(HwError::Io {
                        message: format!("failed waiting on aplay: {err}"),
                    });
                }
            }
        }
    }
}

/// Map a [`VoiceCue`] to its bundled `.wav` filename stem.
pub fn cue_asset_filename(cue: VoiceCue) -> &'static str {
    match cue {
        VoiceCue::DeviceReady => "device_ready.wav",
        VoiceCue::RecordingStarted => "recording_started.wav",
        VoiceCue::RecordingStopped => "recording_stopped.wav",
        VoiceCue::StorageLow => "storage_low.wav",
        VoiceCue::ErrorAlert => "error_alert.wav",
    }
}

/// Scale PCM16 little-endian WAV audio samples by `volume` (0..=100) without touching
/// the ALSA mixer or capture gain.
pub fn scale_pcm16_wav(wav_bytes: &[u8], volume: u8) -> HwResult<Vec<u8>> {
    if volume > 100 {
        return Err(HwError::Io {
            message: format!("invalid voice volume: {volume}"),
        });
    }
    if wav_bytes.len() < 44 || &wav_bytes[0..4] != b"RIFF" || &wav_bytes[8..12] != b"WAVE" {
        return Err(HwError::Io {
            message: "voice asset is not a valid RIFF/WAVE file".to_owned(),
        });
    }

    let mut out = wav_bytes.to_vec();
    let mut offset = 12_usize;
    let mut fmt_valid = false;
    let mut found_data = false;

    while offset + 8 <= out.len() {
        let chunk_id = [
            out[offset],
            out[offset + 1],
            out[offset + 2],
            out[offset + 3],
        ];
        let chunk_size = u32::from_le_bytes([
            out[offset + 4],
            out[offset + 5],
            out[offset + 6],
            out[offset + 7],
        ]) as usize;
        let data_start = offset + 8;
        let Some(data_end) = data_start.checked_add(chunk_size) else {
            return Err(HwError::Io {
                message: "corrupt WAV chunk length".to_owned(),
            });
        };
        if data_end > out.len() {
            return Err(HwError::Io {
                message: "truncated WAV chunk payload".to_owned(),
            });
        }

        if &chunk_id == b"fmt " {
            if chunk_size < 16 {
                return Err(HwError::Io {
                    message: "WAV fmt chunk too short".to_owned(),
                });
            }
            let audio_format = u16::from_le_bytes([out[data_start], out[data_start + 1]]);
            let bits_per_sample = u16::from_le_bytes([out[data_start + 14], out[data_start + 15]]);
            if audio_format != 1 || bits_per_sample != 16 {
                return Err(HwError::Io {
                    message: "voice asset must be uncompressed PCM16".to_owned(),
                });
            }
            fmt_valid = true;
        } else if &chunk_id == b"data" {
            if !fmt_valid {
                return Err(HwError::Io {
                    message: "WAV data chunk appeared before valid PCM16 fmt chunk".to_owned(),
                });
            }
            if chunk_size % 2 != 0 {
                return Err(HwError::Io {
                    message: "PCM16 WAV data chunk has odd byte length".to_owned(),
                });
            }
            if volume != 100 {
                let scale = i32::from(volume);
                for pair in out[data_start..data_end].chunks_exact_mut(2) {
                    let sample = i16::from_le_bytes([pair[0], pair[1]]);
                    let scaled = ((i32::from(sample) * scale) / 100)
                        .clamp(i32::from(i16::MIN), i32::from(i16::MAX))
                        as i16;
                    let bytes = scaled.to_le_bytes();
                    pair[0] = bytes[0];
                    pair[1] = bytes[1];
                }
            }
            found_data = true;
        }

        // RIFF chunks are word-aligned (pad byte when chunk_size is odd).
        let padded = chunk_size + (chunk_size & 1);
        offset = match data_start.checked_add(padded) {
            Some(next) => next,
            None => break,
        };
    }

    if !fmt_valid || !found_data {
        return Err(HwError::Io {
            message: "WAV file missing required PCM16 fmt/data chunks".to_owned(),
        });
    }
    Ok(out)
}

/// Thread-safe voice feedback controller with atomic settings persistence and trait-abstracted audio output.
pub struct VoiceController {
    audio_device: String,
    assets_dir: PathBuf,
    settings_path: PathBuf,
    playback_timeout: Duration,
    player: Arc<dyn AudioPlayer>,
    settings: Mutex<VoiceSettings>,
}

impl VoiceController {
    pub fn open(
        audio_device: impl Into<String>,
        assets_dir: impl Into<PathBuf>,
        settings_path: impl Into<PathBuf>,
        player: Arc<dyn AudioPlayer>,
    ) -> HwResult<Self> {
        let settings_path = settings_path.into();
        let settings = match fs::symlink_metadata(&settings_path) {
            Ok(_) => {
                let loaded: VoiceSettings =
                    read_bounded_json(&settings_path, MAX_VOICE_SETTINGS_BYTES, 8).map_err(
                        |err| HwError::Io {
                            message: format!(
                                "failed to read voice settings {}: {err}",
                                settings_path.display()
                            ),
                        },
                    )?;
                loaded.validate()?;
                loaded
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => VoiceSettings::default(),
            Err(err) => {
                return Err(HwError::Io {
                    message: format!(
                        "failed to inspect voice settings {}: {err}",
                        settings_path.display()
                    ),
                });
            }
        };

        Ok(Self {
            audio_device: audio_device.into(),
            assets_dir: assets_dir.into(),
            settings_path,
            playback_timeout: DEFAULT_PLAYBACK_TIMEOUT,
            player,
            settings: Mutex::new(settings),
        })
    }

    pub fn with_playback_timeout(mut self, timeout: Duration) -> Self {
        self.playback_timeout = timeout;
        self
    }

    pub fn status(&self) -> HwResult<VoiceStatus> {
        let guard = self.settings.lock().map_err(|_| HwError::Io {
            message: "voice settings mutex poisoned".to_owned(),
        })?;
        Ok(guard.to_status())
    }

    /// Update voice volume (0..=100) and atomically persist settings to disk.
    pub fn set_volume(&self, volume: u8) -> HwResult<VoiceStatus> {
        if volume > 100 {
            return Err(HwError::Io {
                message: format!("voice volume must be in 0..=100, got {volume}"),
            });
        }
        let mut guard = self.settings.lock().map_err(|_| HwError::Io {
            message: "voice settings mutex poisoned".to_owned(),
        })?;
        let mut next = guard.clone();
        next.volume = volume;
        persist_voice_settings(&self.settings_path, &next)?;
        *guard = next;
        Ok(guard.to_status())
    }

    /// Enable or disable voice feedback and atomically persist settings to disk.
    pub fn set_enabled(&self, enabled: bool) -> HwResult<VoiceStatus> {
        let mut guard = self.settings.lock().map_err(|_| HwError::Io {
            message: "voice settings mutex poisoned".to_owned(),
        })?;
        let mut next = guard.clone();
        next.enabled = enabled;
        persist_voice_settings(&self.settings_path, &next)?;
        *guard = next;
        Ok(guard.to_status())
    }
}

impl VoiceFeedback for VoiceController {
    fn play_cue(&self, cue: VoiceCue) -> HwResult<()> {
        let (enabled, volume) = {
            let guard = self.settings.lock().map_err(|_| HwError::Io {
                message: "voice settings mutex poisoned".to_owned(),
            })?;
            (guard.enabled, guard.volume)
        };
        if !enabled || volume == 0 {
            return Ok(());
        }
        let asset_path = self.assets_dir.join(cue_asset_filename(cue));
        let raw_wav = read_bounded_bytes(&asset_path, MAX_WAV_ASSET_BYTES).map_err(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                HwError::Unavailable {
                    message: format!("voice asset missing: {}", asset_path.display()),
                }
            } else {
                HwError::Io {
                    message: format!("failed to read voice asset {}: {err}", asset_path.display()),
                }
            }
        })?;
        let scaled = scale_pcm16_wav(&raw_wav, volume)?;
        self.player
            .play_wav(&self.audio_device, &scaled, self.playback_timeout)
    }
}

fn persist_voice_settings(path: &Path, settings: &VoiceSettings) -> HwResult<()> {
    settings.validate()?;
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|err| HwError::Io {
                message: format!(
                    "failed to create voice settings dir {}: {err}",
                    parent.display()
                ),
            })?;
        }
    }
    let mut bytes = serde_json::to_vec_pretty(settings).map_err(|err| HwError::Io {
        message: format!("failed to serialize voice settings: {err}"),
    })?;
    bytes.push(b'\n');
    write_bytes_atomic(path, &bytes, 0o640, true).map_err(|err| HwError::Io {
        message: format!("failed to write voice settings {}: {err}", path.display()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn make_pcm16_wav(samples: &[i16]) -> Vec<u8> {
        let data_len = (samples.len() * 2) as u32;
        let riff_len = 36 + data_len;
        let mut buf = Vec::with_capacity(44 + data_len as usize);
        buf.extend_from_slice(b"RIFF");
        buf.extend_from_slice(&riff_len.to_le_bytes());
        buf.extend_from_slice(b"WAVE");
        buf.extend_from_slice(b"fmt ");
        buf.extend_from_slice(&16u32.to_le_bytes());
        buf.extend_from_slice(&1u16.to_le_bytes()); // PCM
        buf.extend_from_slice(&1u16.to_le_bytes()); // mono
        buf.extend_from_slice(&16000u32.to_le_bytes()); // sample rate
        buf.extend_from_slice(&32000u32.to_le_bytes()); // byte rate
        buf.extend_from_slice(&2u16.to_le_bytes()); // block align
        buf.extend_from_slice(&16u16.to_le_bytes()); // bits per sample
        buf.extend_from_slice(b"data");
        buf.extend_from_slice(&data_len.to_le_bytes());
        for &s in samples {
            buf.extend_from_slice(&s.to_le_bytes());
        }
        buf
    }

    struct RecordingPlayer {
        played: Mutex<Vec<(String, Vec<u8>)>>,
    }

    impl AudioPlayer for RecordingPlayer {
        fn play_wav(&self, device: &str, wav_bytes: &[u8], _timeout: Duration) -> HwResult<()> {
            self.played
                .lock()
                .unwrap()
                .push((device.to_owned(), wav_bytes.to_vec()));
            Ok(())
        }
    }

    #[test]
    fn persists_volume_atomically_scales_pcm16_and_skips_when_muted() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "miro-ego-voice-test-{}-{unique}",
            std::process::id()
        ));
        let assets = dir.join("assets");
        fs::create_dir_all(&assets).unwrap();
        let wav = make_pcm16_wav(&[1000, -2000, 10000, -10000]);
        fs::write(assets.join("recording_started.wav"), &wav).unwrap();

        let settings_path = dir.join("voice.json");
        let player = Arc::new(RecordingPlayer {
            played: Mutex::new(Vec::new()),
        });
        let ctrl = VoiceController::open(
            DEFAULT_VOICE_DEVICE,
            &assets,
            &settings_path,
            player.clone(),
        )
        .unwrap();

        // Disabled by default -> play_cue is a no-op.
        ctrl.play_cue(VoiceCue::RecordingStarted).unwrap();
        assert!(player.played.lock().unwrap().is_empty());

        // Reject out-of-range volume > 100.
        assert!(ctrl.set_volume(101).is_err());

        // Enable + set volume 50% -> persists and plays scaled samples.
        ctrl.set_enabled(true).unwrap();
        let status = ctrl.set_volume(50).unwrap();
        assert_eq!(status.schema, VOICE_STATUS_SCHEMA);
        assert!(status.enabled);
        assert_eq!(status.volume, 50);

        ctrl.play_cue(VoiceCue::RecordingStarted).unwrap();
        {
            let played = player.played.lock().unwrap();
            assert_eq!(played.len(), 1);
            assert_eq!(played[0].0, DEFAULT_VOICE_DEVICE);
            let data = &played[0].1[44..];
            let samples: Vec<i16> = data
                .chunks_exact(2)
                .map(|c| i16::from_le_bytes([c[0], c[1]]))
                .collect();
            assert_eq!(samples, vec![500, -1000, 5000, -5000]);
        }

        // Reload from disk verifies persistence.
        let reloaded = VoiceController::open(
            DEFAULT_VOICE_DEVICE,
            &assets,
            &settings_path,
            player.clone(),
        )
        .unwrap();
        assert_eq!(reloaded.status().unwrap(), status);

        // Volume 0 mutes without invoking player.
        reloaded.set_volume(0).unwrap();
        reloaded.play_cue(VoiceCue::RecordingStarted).unwrap();
        assert_eq!(player.played.lock().unwrap().len(), 1);

        fs::remove_dir_all(dir).unwrap();
    }
}
