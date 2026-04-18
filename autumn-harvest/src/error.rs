//! Error types for the harvest engine.
//!
//! `HarvestError` is a proper `std::error::Error` (via thiserror) so it can be
//! propagated with `?` through internal engine code and wrapped in `AutumnError`
//! at the boundary where workflow/activity results leave the engine.
//!
//! Note: `AutumnError` (from autumn-web) is intentionally NOT `std::error::Error`
//! — it's an HTTP response wrapper. `HarvestError` converts to `AutumnError` via
//! the blanket `From<E: Error> for AutumnError` impl automatically.

/// The kind of timeout that fired.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TimeoutType {
    /// Worker claimed the task but didn't finish in time.
    StartToClose,
    /// Task was enqueued but no worker claimed it in time.
    ScheduleToStart,
    /// Total time from enqueue to final completion exceeded limit.
    ScheduleToClose,
    /// Activity stopped sending heartbeats.
    Heartbeat,
}

impl std::fmt::Display for TimeoutType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StartToClose => write!(f, "StartToClose"),
            Self::ScheduleToStart => write!(f, "ScheduleToStart"),
            Self::ScheduleToClose => write!(f, "ScheduleToClose"),
            Self::Heartbeat => write!(f, "Heartbeat"),
        }
    }
}

/// Errors produced by the autumn-harvest workflow engine.
#[derive(Debug, thiserror::Error)]
pub enum HarvestError {
    #[error("activity failed: {name} (attempt {attempt}): {source}")]
    ActivityFailed {
        name: String,
        attempt: u32,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("workflow failed: {name}: {reason}")]
    WorkflowFailed { name: String, reason: String },

    #[error("non-deterministic replay: {0}")]
    NonDeterministic(String),

    #[error("workflow cancelled: {0}")]
    Cancelled(String),

    #[error("timeout: {timeout_type} for {task_name}")]
    Timeout {
        timeout_type: TimeoutType,
        task_name: String,
    },

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("database error: {0}")]
    Database(String),

    #[error("task queue is full (queue: {queue}, depth: {depth})")]
    QueueFull { queue: String, depth: usize },

    #[error("workflow execution not found: {0}")]
    NotFound(String),

    #[error("invalid configuration: {0}")]
    Config(String),
}

/// Standard result type for internal harvest engine operations.
pub type HarvestResult<T> = Result<T, HarvestError>;

/// Wrap any displayable error into [`HarvestError::Database`].
///
/// Use with `.map_err(database_error)` to reduce boilerplate on diesel calls.
pub fn database_error(e: impl std::fmt::Display) -> HarvestError {
    HarvestError::Database(e.to_string())
}

#[cfg(feature = "db")]
impl From<diesel::result::Error> for HarvestError {
    fn from(value: diesel::result::Error) -> Self {
        database_error(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn harvest_error_is_std_error() {
        let e: &dyn std::error::Error = &HarvestError::NonDeterministic("test".into());
        assert!(e.to_string().contains("non-deterministic"));
    }

    #[test]
    fn harvest_error_display_includes_task_name() {
        let e = HarvestError::Timeout {
            timeout_type: TimeoutType::StartToClose,
            task_name: "send_email".into(),
        };
        assert!(e.to_string().contains("send_email"));
        assert!(e.to_string().contains("StartToClose"));
    }

    #[test]
    #[allow(clippy::unnecessary_literal_unwrap)]
    fn harvest_result_ok() -> HarvestResult<()> {
        let r: HarvestResult<i32> = Ok(42);
        assert_eq!(r?, 42);
        Ok(())
    }
}
