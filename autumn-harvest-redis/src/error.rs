//! Error type for the Redis task queue adapter.

/// Errors produced by the Redis task queue adapter.
#[derive(Debug, thiserror::Error)]
pub enum RedisAdapterError {
    /// A Redis command failed.
    #[error("redis error: {0}")]
    Redis(#[from] redis::RedisError),

    /// A task envelope could not be serialized or deserialized.
    #[error("envelope serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// A duration value was outside the supported range when converting between
    /// `std::time::Duration` and `chrono::Duration`.
    #[error("duration out of range: {0}")]
    DurationOutOfRange(String),

    /// A Redis stream entry was missing the expected `payload` field.
    #[error("malformed stream entry (id={entry_id}): {reason}")]
    MalformedEntry {
        /// The Redis stream entry ID.
        entry_id: String,
        /// Human readable reason describing why the entry could not be parsed.
        reason: String,
    },

    /// The queue name contained characters that are not allowed.
    #[error("invalid queue name '{0}'")]
    InvalidQueueName(String),
}

/// Convenience result alias used throughout the adapter.
pub type RedisAdapterResult<T> = Result<T, RedisAdapterError>;
