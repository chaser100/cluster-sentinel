//! ClusterSentinel library — event registry, metrics, MCP, and HTTP surfaces.

pub mod auth;
pub mod config;
pub mod error;
pub mod events;
pub mod http;
pub mod mcp;
pub mod metrics;

pub use config::Config;
pub use error::{AppError, AppResult};
