//! Mandatory Bearer authentication for streamable HTTP MCP and protected APIs.

use std::sync::Arc;

use axum::extract::Request;
use axum::http::header::{AUTHORIZATION, WWW_AUTHENTICATE};
use axum::http::{HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use tracing::warn;

use crate::metrics::Metrics;

/// Realm advertised on `401 Unauthorized` for MCP clients.
pub const MCP_AUTH_REALM: &str = "clustersentinel-mcp";

/// Shared auth middleware state.
#[derive(Clone)]
pub struct BearerAuthState {
    pub token: Arc<str>,
    pub metrics: Metrics,
}

/// Axum middleware: require `Authorization: Bearer <token>`.
pub async fn require_mcp_bearer(
    axum::extract::State(state): axum::extract::State<BearerAuthState>,
    request: Request,
    next: Next,
) -> Response {
    let auth_header = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    let authorized = auth_header
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|provided| constant_time_eq(provided.as_bytes(), state.token.as_bytes()));

    if authorized {
        return next.run(request).await;
    }

    state.metrics.mcp_auth_failures.inc();

    let reason = match auth_header {
        None => "missing_authorization",
        Some(value) if !value.starts_with("Bearer ") => "not_bearer_scheme",
        Some(_) => "token_mismatch",
    };
    warn!(
        target: "clustersentinel::mcp_auth",
        %reason,
        path = %request.uri().path(),
        "MCP Bearer auth rejected"
    );

    let mut response = StatusCode::UNAUTHORIZED.into_response();
    if let Ok(header) = HeaderValue::from_str(&format!("Bearer realm=\"{MCP_AUTH_REALM}\"")) {
        response.headers_mut().insert(WWW_AUTHENTICATE, header);
    }
    response
}

/// Constant-time equality for equal-length slices; unequal lengths return false.
#[must_use]
pub fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut diff = 0_u8;
    for (a, b) in left.iter().zip(right.iter()) {
        diff |= a ^ b;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_time_eq_matches() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
    }
}
