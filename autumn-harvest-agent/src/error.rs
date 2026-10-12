//! The error type of models and tools.

use thiserror::Error;

/// The category of an [`AgentError`].
///
/// The loop reads the category of a model failure to decide on a retry. See
/// [`ErrorKind::is_retryable`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The model or tool configuration is not valid.
    Config,
    /// The provider refused the credentials.
    Authentication,
    /// The provider limited the request rate.
    RateLimited,
    /// The provider refused the request. A retry cannot fix it.
    Provider,
    /// The transport failed before an answer arrived.
    Transport,
    /// The provider answered with a timeout, a server error, or an overload.
    /// A retry can succeed.
    Unavailable,
    /// A tool failed.
    Tool,
    /// The request was too large for the provider.
    Budget,
    /// An answer could not be decoded.
    Decode,
}

impl ErrorKind {
    /// Can a retry of the same request succeed?
    #[must_use]
    pub const fn is_retryable(self) -> bool {
        matches!(
            self,
            Self::RateLimited | Self::Transport | Self::Unavailable
        )
    }
}

/// A model or tool failure: a category and a message.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("[{kind:?}] {message}")]
pub struct AgentError {
    kind: ErrorKind,
    message: String,
}

impl AgentError {
    /// An error with this category and message.
    #[must_use]
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// The category.
    #[must_use]
    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// The message, without the category.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_transient_kinds_are_retryable() {
        for kind in [
            ErrorKind::RateLimited,
            ErrorKind::Transport,
            ErrorKind::Unavailable,
        ] {
            assert!(kind.is_retryable(), "{kind:?}");
        }
        for kind in [
            ErrorKind::Config,
            ErrorKind::Authentication,
            ErrorKind::Provider,
            ErrorKind::Tool,
            ErrorKind::Budget,
            ErrorKind::Decode,
        ] {
            assert!(!kind.is_retryable(), "{kind:?}");
        }
    }

    #[test]
    fn display_names_the_kind_and_message() {
        let err = AgentError::new(ErrorKind::Tool, "boom");
        assert_eq!(err.to_string(), "[Tool] boom");
        assert_eq!(err.message(), "boom");
        assert_eq!(err.kind(), ErrorKind::Tool);
    }
}
