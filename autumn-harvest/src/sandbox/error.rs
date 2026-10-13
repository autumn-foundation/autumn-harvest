//! The failures of a sandbox run, and how each one maps to an activity
//! failure.
//!
//! A run of the code that ends badly is not a failure. A non-zero exit, a
//! signal or a timeout is an outcome, so the agent can read stderr and fix
//! its code. A [`SandboxError`] means that the code did not run to an
//! outcome at all.

use crate::failure::ActivityFailure;

/// Why a sandbox run produced no outcome.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SandboxError {
    /// The request breaks the policy: an unknown runtime, a limit over its
    /// ceiling, a bad path or an oversized input. A retry cannot fix it.
    #[error("sandbox request denied: {0}")]
    Denied(String),
    /// The operator configuration is not valid. A retry cannot fix it.
    #[error("sandbox configuration is not valid: {0}")]
    Config(String),
    /// The backend could not start, boot or reach the sandbox. Another
    /// attempt, or another worker, can succeed.
    #[error("sandbox unavailable: {0}")]
    Unavailable(String),
    /// The guest broke the wire protocol. The guest runs untrusted code, so
    /// the host does not retry it.
    #[error("sandbox protocol violation: {0}")]
    Protocol(String),
    /// The caller cancelled the run.
    #[error("sandbox run cancelled")]
    Cancelled,
}

impl SandboxError {
    /// Can a retry of the same request succeed?
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::Unavailable(_) | Self::Cancelled)
    }
}

impl From<SandboxError> for ActivityFailure {
    fn from(err: SandboxError) -> Self {
        use crate::failure::{
            ERROR_TYPE_SANDBOX_CANCELLED, ERROR_TYPE_SANDBOX_PROTOCOL_VIOLATION,
            ERROR_TYPE_SANDBOX_UNAVAILABLE,
        };
        let message = err.to_string();
        match err {
            SandboxError::Denied(_) | SandboxError::Config(_) => Self::sandbox_denied(message),
            SandboxError::Unavailable(_) => {
                Self::retryable(ERROR_TYPE_SANDBOX_UNAVAILABLE, message)
            }
            SandboxError::Protocol(_) => {
                Self::non_retryable(ERROR_TYPE_SANDBOX_PROTOCOL_VIOLATION, message)
            }
            SandboxError::Cancelled => Self::retryable(ERROR_TYPE_SANDBOX_CANCELLED, message),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::failure::{
        ERROR_TYPE_SANDBOX_CANCELLED, ERROR_TYPE_SANDBOX_DENIED,
        ERROR_TYPE_SANDBOX_PROTOCOL_VIOLATION, ERROR_TYPE_SANDBOX_UNAVAILABLE,
    };

    #[test]
    fn a_denial_and_a_config_error_are_not_retryable() {
        for err in [
            SandboxError::Denied("x".into()),
            SandboxError::Config("x".into()),
        ] {
            assert!(!err.is_retryable());
            let failure = ActivityFailure::from(err);
            assert_eq!(failure.error_type, ERROR_TYPE_SANDBOX_DENIED);
            assert!(failure.non_retryable);
        }
    }

    #[test]
    fn an_unavailable_backend_is_retryable() {
        let err = SandboxError::Unavailable("boot timed out".into());
        assert!(err.is_retryable());
        let failure = ActivityFailure::from(err);
        assert_eq!(failure.error_type, ERROR_TYPE_SANDBOX_UNAVAILABLE);
        assert!(!failure.non_retryable);
        assert!(failure.message.contains("boot timed out"));
    }

    #[test]
    fn a_protocol_violation_is_not_retryable() {
        let err = SandboxError::Protocol("frame too large".into());
        assert!(!err.is_retryable());
        let failure = ActivityFailure::from(err);
        assert_eq!(failure.error_type, ERROR_TYPE_SANDBOX_PROTOCOL_VIOLATION);
        assert!(failure.non_retryable);
    }

    #[test]
    fn a_cancellation_is_retryable() {
        assert!(SandboxError::Cancelled.is_retryable());
        let failure = ActivityFailure::from(SandboxError::Cancelled);
        assert_eq!(failure.error_type, ERROR_TYPE_SANDBOX_CANCELLED);
        assert!(!failure.non_retryable);
    }
}
