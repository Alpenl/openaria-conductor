//! On-device peripheral control traits: GPIO recording button, LED status indicator,
//! and voice feedback cues for Miro-Ego.

use miro_ego_core::CoreError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum HwError {
    #[error("peripheral unavailable: {message}")]
    Unavailable { message: String },

    #[error("peripheral I/O failure: {message}")]
    Io { message: String },

    #[error(transparent)]
    Core(#[from] CoreError),
}

pub type HwResult<T> = Result<T, HwError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ButtonEvent {
    ShortPress,
    DoublePress,
    LongPress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndicatorState {
    Idle,
    Ready,
    Recording,
    Hotspot,
    Updating,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VoiceCue {
    DeviceReady,
    RecordingStarted,
    RecordingStopped,
    StorageLow,
    ErrorAlert,
}

pub trait ButtonInput: Send + Sync {
    fn poll_event(&self, timeout_ms: u64) -> HwResult<Option<ButtonEvent>>;
}

pub trait IndicatorController: Send + Sync {
    fn set_state(&self, state: IndicatorState) -> HwResult<()>;
}

pub trait VoiceFeedback: Send + Sync {
    fn play_cue(&self, cue: VoiceCue) -> HwResult<()>;
}
