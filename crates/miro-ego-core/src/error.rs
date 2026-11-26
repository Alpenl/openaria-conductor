use std::io;
use thiserror::Error;

/// Canonical error type for `miro-ego-core`.
#[derive(Debug, Error)]
pub enum CoreError {
    #[error("{context}: {source}")]
    Io {
        code: &'static str,
        context: String,
        #[source]
        source: io::Error,
    },

    #[error("{code}: {message}")]
    InvalidInput { code: &'static str, message: String },

    #[error("config invalid: {message}")]
    InvalidConfig { code: &'static str, message: String },

    #[error("lock unavailable: {message}")]
    LockBusy { message: String },

    #[error("mutex poisoned: {resource}")]
    Poisoned { resource: &'static str },
}

impl CoreError {
    pub fn io(code: &'static str, context: impl Into<String>, source: io::Error) -> Self {
        Self::Io {
            code,
            context: context.into(),
            source,
        }
    }

    pub fn invalid_input(code: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidInput {
            code,
            message: message.into(),
        }
    }

    pub fn invalid_config(code: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidConfig {
            code,
            message: message.into(),
        }
    }

    pub fn lock_busy(message: impl Into<String>) -> Self {
        Self::LockBusy {
            message: message.into(),
        }
    }

    pub fn poisoned(resource: &'static str) -> Self {
        Self::Poisoned { resource }
    }

    pub fn code(&self) -> &'static str {
        match self {
            Self::Io { code, .. }
            | Self::InvalidInput { code, .. }
            | Self::InvalidConfig { code, .. } => code,
            Self::LockBusy { .. } => "lock_busy",
            Self::Poisoned { .. } => "state_poisoned",
        }
    }
}

pub type CoreResult<T> = Result<T, CoreError>;
