//! On-device peripheral control implementations and traits: 40-pin GPIO recording button,
//! sysfs LED status indicator, voice feedback cues, and RDK X5 thermal/board inspection.

pub mod button;
pub mod indicator;
pub mod thermal;
pub mod voice;

use miro_ego_core::CoreError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub use button::{
    BUTTON_CONFIG_SCHEMA, ButtonClassifier, ButtonConfig, ButtonMonitor, CaptureToggleAction,
    CaptureToggleClient, DEFAULT_BUTTON_CONFIG_PATH, GpioButtonInput, GpioPin,
    LocalHttpCaptureClient, RDK_X5_GPIO_PINS, SysfsGpioPin, rdk_x5_board_pin_to_soc_gpio,
};
pub use indicator::{
    DEFAULT_LEDS_SYSFS_ROOT, LedIo, LedPattern, SysfsLedController, SysfsLedIo, pattern_for_state,
};
pub use thermal::{
    DEFAULT_BOARD_MODEL_PATH, DEFAULT_THERMAL_ROOT, MAX_VALID_TEMP_C, MIN_VALID_TEMP_C,
    RDK_X5_BOARD_MODEL, ThermalZoneReading, is_rdk_x5_model, read_board_model,
    read_board_model_from, read_soc_temperature_c, read_soc_temperature_c_in,
    read_thermal_zones_in,
};
pub use voice::{
    AplayAudioPlayer, AudioPlayer, DEFAULT_VOICE_DEVICE, VOICE_SETTINGS_SCHEMA,
    VOICE_STATUS_SCHEMA, VoiceController, VoiceSettings, VoiceStatus, cue_asset_filename,
    scale_pcm16_wav,
};

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

    fn disarm(&self) {}
}

pub trait IndicatorController: Send + Sync {
    fn set_state(&self, state: IndicatorState) -> HwResult<()>;
}

pub trait VoiceFeedback: Send + Sync {
    fn play_cue(&self, cue: VoiceCue) -> HwResult<()>;
}
