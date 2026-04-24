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

/// Shard identifier for routing workflow state to a specific database.
///
/// A process running against a sharded Harvest deployment holds one `DbPool`
/// per shard; the `ShardId` picks which pool a given workflow lives in. The
/// value is encoded into the first two bytes of each `ExecutionId` so that any
/// holder of an `ExecutionId` can recover the shard in O(1) without a lookup
/// table.
///
/// The reserved sentinel value [`ShardId::UNENCODED`] (`0xFFFF`) is emitted
/// by [`ExecutionId::new`] when the caller has not explicitly picked a shard
/// (tests, replay harnesses, ad-hoc tooling). Routing code treats the sentinel
/// as "fall back to the default shard" rather than a real placement.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::types::ShardId;
///
/// let shard = ShardId::new(0);
/// assert_eq!(shard.as_i32(), 0);
/// assert_ne!(shard, ShardId::UNENCODED);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ShardId(i32);

impl ShardId {
    /// Reserved shard number signalling "not encoded by the framework".
    ///
    /// Routing layers should treat this value as a request for the
    /// deployment's default shard rather than a literal shard index.
    pub const UNENCODED: Self = Self(0xFFFF);

    /// Create a shard identifier from a numeric value.
    ///
    /// Values in the range `0..=0xFFFE` are valid shard numbers. The value
    /// `0xFFFF` is reserved for the sentinel; constructing one is allowed but
    /// prefer [`ShardId::UNENCODED`] at call sites for clarity.
    #[must_use]
    pub const fn new(id: i32) -> Self {
        Self(id)
    }

    /// The raw integer value of the shard identifier.
    #[must_use]
    pub const fn as_i32(self) -> i32 {
        self.0
    }

    /// Returns the shard number encoded in the two high bytes of a UUID.
    #[must_use]
    pub fn from_uuid(uuid: &Uuid) -> Self {
        let bytes = uuid.as_bytes();
        let raw = u16::from_be_bytes([bytes[0], bytes[1]]);
        Self(i32::from(raw))
    }

    /// Is this the reserved sentinel value?
    #[must_use]
    pub const fn is_unencoded(self) -> bool {
        self.0 == Self::UNENCODED.0
    }
}

impl fmt::Display for ShardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_unencoded() {
            f.write_str("unencoded")
        } else {
            write!(f, "{}", self.0)
        }
    }
}

/// Unique identifier for a single workflow execution (run).
///
/// Stored as UUID in Postgres. When a workflow is started by the framework,
/// the first two bytes of the UUID carry the [`ShardId`] the workflow is
/// routed to so that any `ExecutionId` can be resolved back to its database
/// shard in O(1). Values produced outside the start path (tests, replay) use
/// the reserved sentinel [`ShardId::UNENCODED`], which routers handle by
/// falling back to the default shard.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::types::{ExecutionId, ShardId};
///
/// let shard = ShardId::new(3);
/// let id = ExecutionId::new_for_shard(shard);
/// assert_eq!(id.shard(), shard);
///
/// let sentinel = ExecutionId::new();
/// assert_eq!(sentinel.shard(), ShardId::UNENCODED);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExecutionId(Uuid);

impl ExecutionId {
    /// Creates a new `ExecutionId` with the [`ShardId::UNENCODED`] sentinel
    /// in its shard bits.
    ///
    /// Callers that know which shard a workflow should live in should use
    /// [`ExecutionId::new_for_shard`] instead. `ExecutionId::new` is retained
    /// for tests, replay harnesses, and other non-production code paths where
    /// shard routing is not meaningful.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::types::{ExecutionId, ShardId};
    ///
    /// let id = ExecutionId::new();
    /// assert_eq!(id.shard(), ShardId::UNENCODED);
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::new_with_shard_bytes(Uuid::new_v4(), ShardId::UNENCODED)
    }

    /// Create a fresh `ExecutionId` that encodes the given `ShardId` in its
    /// first two bytes.
    ///
    /// The random bits of a UUID v4 fill the remaining 14 bytes so collision
    /// probability is unaffected.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::types::{ExecutionId, ShardId};
    ///
    /// let id = ExecutionId::new_for_shard(ShardId::new(2));
    /// assert_eq!(id.shard(), ShardId::new(2));
    /// ```
    #[must_use]
    pub fn new_for_shard(shard: ShardId) -> Self {
        Self::new_with_shard_bytes(Uuid::new_v4(), shard)
    }

    fn new_with_shard_bytes(uuid: Uuid, shard: ShardId) -> Self {
        let raw = u16::try_from(shard.as_i32() & 0xFFFF).unwrap_or(0xFFFF);
        let [hi, lo] = raw.to_be_bytes();
        let mut bytes = *uuid.as_bytes();
        bytes[0] = hi;
        bytes[1] = lo;
        Self(Uuid::from_bytes(bytes))
    }

    /// Extracts the encoded [`ShardId`] from the first two bytes of the UUID.
    ///
    /// Returns [`ShardId::UNENCODED`] for ids produced by [`ExecutionId::new`]
    /// or sourced from pre-sharding data.
    #[must_use]
    pub fn shard(&self) -> ShardId {
        ShardId::from_uuid(&self.0)
    }

    /// Wraps an existing `Uuid` into an `ExecutionId`.
    ///
    /// The shard bits are preserved as-is; callers that wish to re-home a
    /// UUID should rebuild it via [`ExecutionId::new_for_shard`].
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
    fn execution_id_new_uses_unencoded_sentinel() {
        let id = ExecutionId::new();
        assert_eq!(id.shard(), ShardId::UNENCODED);
        assert!(id.shard().is_unencoded());
    }

    #[test]
    fn execution_id_new_for_shard_round_trips() {
        for raw in [0i32, 1, 2, 7, 255, 0x1234, 0xFFFE] {
            let shard = ShardId::new(raw);
            let id = ExecutionId::new_for_shard(shard);
            assert_eq!(id.shard(), shard, "round trip failed for shard {raw}");
        }
    }

    #[test]
    fn execution_id_new_for_shard_preserves_entropy() {
        let shard = ShardId::new(5);
        let a = ExecutionId::new_for_shard(shard);
        let b = ExecutionId::new_for_shard(shard);
        assert_ne!(a, b);
        assert_eq!(a.shard(), shard);
        assert_eq!(b.shard(), shard);
    }

    #[test]
    fn shard_id_from_uuid_reads_two_high_bytes() {
        let mut bytes = [0u8; 16];
        bytes[0] = 0x01;
        bytes[1] = 0x23;
        let uuid = uuid::Uuid::from_bytes(bytes);
        assert_eq!(ShardId::from_uuid(&uuid).as_i32(), 0x0123);
    }

    #[test]
    fn shard_id_display_names_sentinel() {
        assert_eq!(ShardId::UNENCODED.to_string(), "unencoded");
        assert_eq!(ShardId::new(0).to_string(), "0");
        assert_eq!(ShardId::new(7).to_string(), "7");
    }

    #[test]
    fn execution_id_from_uuid_preserves_shard_bits() {
        let source = ExecutionId::new_for_shard(ShardId::new(9));
        let wrapped = ExecutionId::from_uuid(source.as_uuid());
        assert_eq!(wrapped.shard(), ShardId::new(9));
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

#[cfg(test)]
mod types_proptest;
