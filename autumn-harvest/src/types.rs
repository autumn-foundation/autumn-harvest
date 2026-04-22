//! Core identity types for the workflow engine.
//!
//! All IDs are strong newtypes — raw strings and UUIDs never flow through
//! the engine untagged.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// User-provided idempotency key for a workflow execution.
///
/// This is the business-level identifier chosen by the caller (e.g.
/// `"user-123"` or `"order-456"`). It is NOT the run ID. Reusing the same
/// `WorkflowId` for the same workflow name should resolve to the same logical
/// workflow start; explicit reruns should use a fresh key until Harvest grows a
/// dedicated restart API.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::types::WorkflowId;
///
/// let id = WorkflowId::new("user-123");
/// assert_eq!(id.as_str(), "user-123");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkflowId(String);

impl WorkflowId {
    /// Creates a new `WorkflowId` from a string-like value.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::types::WorkflowId;
    ///
    /// let id = WorkflowId::new("my-workflow-123");
    /// ```
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Returns the underlying string slice.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::types::WorkflowId;
    ///
    /// let id = WorkflowId::new("user-123");
    /// assert_eq!(id.as_str(), "user-123");
    /// ```
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for WorkflowId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Unique identifier for a single workflow execution (run).
///
/// Generated fresh for each run. Stored as UUID in Postgres.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::types::ExecutionId;
///
/// let id = ExecutionId::new();
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExecutionId(Uuid);

impl ExecutionId {
    /// Creates a new, random `ExecutionId` using a v4 UUID.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::types::ExecutionId;
    ///
    /// let id = ExecutionId::new();
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Wraps an existing `Uuid` into an `ExecutionId`.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use uuid::Uuid;
    /// use autumn_harvest::types::ExecutionId;
    ///
    /// let id = ExecutionId::from_uuid(Uuid::new_v4());
    /// ```
    #[must_use]
    pub const fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }

    /// Returns the underlying `Uuid` for database storage or serialization.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::types::ExecutionId;
    ///
    /// let id = ExecutionId::new();
    /// let uuid = id.as_uuid();
    /// ```
    #[must_use]
    pub const fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for ExecutionId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ExecutionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for ExecutionId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s).map(Self)
    }
}

/// Unique identifier for a single activity execution attempt.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::types::ActivityExecId;
///
/// let id = ActivityExecId::new();
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActivityExecId(Uuid);

impl ActivityExecId {
    /// Creates a new, random `ActivityExecId` using a v4 UUID.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::types::ActivityExecId;
    ///
    /// let id = ActivityExecId::new();
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Returns the underlying `Uuid`.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::types::ActivityExecId;
    ///
    /// let id = ActivityExecId::new();
    /// let uuid = id.as_uuid();
    /// ```
    #[must_use]
    pub const fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for ActivityExecId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ActivityExecId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for ActivityExecId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s).map(Self)
    }
}

/// Durable timer handle within a workflow.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::types::TimerId;
///
/// let id = TimerId::new("timer-1");
/// assert_eq!(id.as_str(), "timer-1");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TimerId(String);

impl TimerId {
    /// Creates a new `TimerId` from a string-like value.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::types::TimerId;
    ///
    /// let id = TimerId::new("my-timer");
    /// ```
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Returns the underlying string slice.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::types::TimerId;
    ///
    /// let id = TimerId::new("timer-1");
    /// assert_eq!(id.as_str(), "timer-1");
    /// ```
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TimerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Identifies a worker instance (hostname + PID or UUID).
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::types::WorkerId;
///
/// let id = WorkerId::new("worker-1");
/// assert_eq!(id.as_str(), "worker-1");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerId(String);

impl WorkerId {
    /// Creates a new `WorkerId` from a string-like value.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::types::WorkerId;
    ///
    /// let id = WorkerId::new("node-a-pid-1234");
    /// ```
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Returns the underlying string slice.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::types::WorkerId;
    ///
    /// let id = WorkerId::new("worker-1");
    /// assert_eq!(id.as_str(), "worker-1");
    /// ```
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_id_display_and_equality() {
        let id = WorkflowId::new("user-123");
        assert_eq!(id.as_str(), "user-123");
        assert_eq!(id, WorkflowId::new("user-123"));
        assert_ne!(id, WorkflowId::new("user-456"));
    }

    #[test]
    fn execution_id_is_random_uuid() {
        let a = ExecutionId::new();
        let b = ExecutionId::new();
        assert_ne!(a, b);
    }

    #[test]
    fn activity_exec_id_display_roundtrip() -> Result<(), uuid::Error> {
        let id = ActivityExecId::new();
        let s = id.to_string();
        let parsed: ActivityExecId = s.parse()?;
        assert_eq!(id, parsed);
        Ok(())
    }
}
