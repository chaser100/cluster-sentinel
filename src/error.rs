//! Error types: `thiserror` for the library boundary, `anyhow` at the binary edge.

use thiserror::Error;

/// Fallible operation result used across library modules.
pub type AppResult<T> = Result<T, AppError>;

/// Typed application errors.
#[derive(Debug, Error)]
pub enum AppError {
    /// Invalid configuration value.
    #[error("config error: {0}")]
    Config(String),

    /// Kubernetes API / client failure.
    #[error("kubernetes error: {0}")]
    Kubernetes(#[from] kube::Error),

    /// HTTP / bind failure.
    #[error("http error: {0}")]
    Http(String),

    /// Unexpected internal failure.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
