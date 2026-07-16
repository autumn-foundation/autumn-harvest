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

    /// Wraps an existing `Uuid` as an `ActivityExecId`.
    #[must_use]
    pub const fn from_uuid(id: Uuid) -> Self {
        Self(id)
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

/// Unique identifier for a single workflow update invocation.
///
/// Generated when an update request is admitted (validator passed) and embedded
/// in the `UpdateAdmitted`, `UpdateCompleted`, and `UpdateFailed` events so the
/// result can be looked up by any worker after a restart.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::types::UpdateId;
///
/// let id = UpdateId::new();
/// assert!(!id.as_uuid().is_nil());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UpdateId(Uuid);

impl UpdateId {
    /// Creates a new, random `UpdateId` using a v4 UUID.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Returns the underlying `Uuid`.
    #[must_use]
    pub const fn as_uuid(&self) -> Uuid {
        self.0
    }

    /// Wraps an existing `Uuid` as an `UpdateId`.
    #[must_use]
    pub const fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }
}

impl Default for UpdateId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for UpdateId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for UpdateId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s).map(Self)
    }
}

/// Opaque single-use token that uniquely identifies a pending external activity.
///
/// The token is embedded in the `ActivityAwaitingExternal` event when a workflow
/// calls `execute_activity_external`, and is round-tripped by external systems
/// through the management API to deliver a result (`/complete`, `/fail`) or extend
/// the deadline (`/heartbeat`).
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::types::ExternalActivityToken;
///
/// let token = ExternalActivityToken::new();
/// assert!(!token.as_uuid().is_nil());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExternalActivityToken(Uuid);

impl ExternalActivityToken {
    /// Create a fresh, random token.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// The nil token (all-zero UUID), useful as a sentinel in tests.
    #[must_use]
    pub const fn nil() -> Self {
        Self(Uuid::nil())
    }

    /// The underlying UUID.
    #[must_use]
    pub const fn as_uuid(&self) -> Uuid {
        self.0
    }

    /// Wraps an existing `Uuid` as an `ExternalActivityToken`.
    #[must_use]
    pub const fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }
}

impl Default for ExternalActivityToken {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ExternalActivityToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for ExternalActivityToken {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s).map(Self)
    }
}

/// Unique identifier for a single `signal_external_workflow` invocation.
///
/// Generated when the workflow calls `ctx.signal_external_workflow(...)` and
/// embedded in the `ExternalSignalRequested`, `ExternalSignalDelivered`, and
/// `ExternalSignalFailed` events so the request can be correlated with its
/// outcome during replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExternalSignalId(Uuid);

impl ExternalSignalId {
    /// Creates a new, random `ExternalSignalId` using a v4 UUID.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Returns the underlying `Uuid`.
    #[must_use]
    pub const fn as_uuid(&self) -> Uuid {
        self.0
    }

    /// Wraps an existing `Uuid` as an `ExternalSignalId`.
    #[must_use]
    pub const fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }
}

impl Default for ExternalSignalId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ExternalSignalId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for ExternalSignalId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s).map(Self)
    }
}

/// Unique identifier for a single `request_cancel_external_workflow` invocation.
///
/// Generated when the workflow calls `ctx.request_cancel_external_workflow(...)` and
/// embedded in the `ExternalCancelRequested`, `ExternalCancelDelivered`, and
/// `ExternalCancelFailed` events so the request can be correlated with its
/// outcome during replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExternalCancelId(Uuid);

impl ExternalCancelId {
    /// Creates a new, random `ExternalCancelId` using a v4 UUID.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Returns the underlying `Uuid`.
    #[must_use]
    pub const fn as_uuid(&self) -> Uuid {
        self.0
    }

    /// Wraps an existing `Uuid` as an `ExternalCancelId`.
    #[must_use]
    pub const fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }
}

impl Default for ExternalCancelId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ExternalCancelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for ExternalCancelId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s).map(Self)
    }
}

/// Unique identifier for a worker session (issue #606).
///
/// Recorded deterministically via the existing `MarkerRecorded` mechanism when
/// a workflow calls `ctx.create_session(...)` — the *identity* is replayed
/// like any other marker, but the *physical worker binding* it resolves to is
/// non-replayed runtime routing state, exactly like activity placement today.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionId(Uuid);

impl SessionId {
    /// Creates a new, random `SessionId` using a v4 UUID.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Returns the underlying `Uuid`.
    #[must_use]
    pub const fn as_uuid(&self) -> Uuid {
        self.0
    }

    /// Wraps an existing `Uuid` as a `SessionId`.
    #[must_use]
    pub const fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for SessionId {
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

/// Controls how a duplicate `(workflow_name, workflow_id)` start is handled.
///
/// The policy is a *caller* concern chosen at request time. It has no effect
/// on the first start of a given `workflow_id`; it only changes behaviour when
/// a prior execution already exists for the same `(workflow_name, workflow_id)`
/// pair.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::types::WorkflowIdReusePolicy;
///
/// // AllowDuplicate is the default
/// let policy = WorkflowIdReusePolicy::default();
/// assert_eq!(policy, WorkflowIdReusePolicy::AllowDuplicate);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowIdReusePolicy {
    /// Return the existing execution unconditionally. This is the default.
    ///
    /// Correct for upstream retries when the caller does not know whether the
    /// previous start succeeded — they get the same `exec_id` back and move on.
    #[default]
    AllowDuplicate,

    /// Return [`crate::error::HarvestError::AlreadyExists`] for any prior
    /// execution, including terminal ones.
    ///
    /// Use for at-most-one semantics: the second request is explicitly rejected
    /// so the caller can decide what to do.
    RejectDuplicate,

    /// Start a fresh run if the prior execution is FAILED or CANCELLED; return
    /// the existing execution unchanged if it is RUNNING or COMPLETED.
    ///
    /// Use for retry-after-failure semantics: a successful or in-progress run
    /// is not superseded, but a failed one is automatically replaced.
    AllowDuplicateFailedOnly,

    /// Cancel a RUNNING prior execution and start a fresh run; start a fresh
    /// run unconditionally if the prior execution is already terminal.
    ///
    /// The cancel and the new start are two separate transactions. A failure
    /// between them leaves the prior workflow CANCELLED with no new run started;
    /// retrying with the same policy starts a fresh run on the next attempt.
    TerminateIfRunning,
}

// ---------------------------------------------------------------------------
// StartSource
// ---------------------------------------------------------------------------

/// A bounded classifier for *how* a workflow execution was started (issue #740).
///
/// Recorded durably on every execution across all start paths so operators can
/// answer "where did this run come from?" — an API call, a schedule tick, a
/// child spawn, a completion trigger, and so on. This is distinct from the
/// `origin` column (issue #534), which classifies scheduled dispatch sub-kinds;
/// the two coexist.
///
/// The variant set is intentionally bounded (low-cardinality) so it is safe as
/// a filter dimension and, if ever surfaced, a metric label. An unrecognized
/// stored string deserializes to [`StartSource::Unknown`] rather than erroring,
/// so a value written by a newer build never breaks an older reader.
///
/// A start-source column is *never* read during replay, so recording it has no
/// effect on determinism.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::types::StartSource;
///
/// // Unknown is the default (a NULL / pre-upgrade column).
/// assert_eq!(StartSource::default(), StartSource::Unknown);
/// assert_eq!(StartSource::Api.as_str(), "api");
/// // Unrecognized strings never error — they map to Unknown.
/// assert_eq!(StartSource::from_str("not-a-real-source"), StartSource::Unknown);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartSource {
    /// Started via the plain HTTP start route or an equivalent direct API call.
    Api,
    /// Fired by a schedule tick.
    Schedule,
    /// Started by a schedule backfill run.
    Backfill,
    /// Started atomically alongside a signal (`signal_with_start`).
    SignalWithStart,
    /// Started atomically alongside an update (`update_with_start`).
    UpdateWithStart,
    /// Started by a completion trigger reacting to another workflow's terminal.
    CompletionTrigger,
    /// Started by an inbound webhook delivery.
    Webhook,
    /// Spawned as a child workflow (awaited or detached).
    Child,
    /// Started via the batch-start API.
    Batch,
    /// A continue-as-new successor run.
    ContinueAsNew,
    /// A reset-fork run.
    Reset,
    /// Started by the cross-shard outbox dispatcher.
    Outbox,
    /// Provenance is unknown (a NULL / pre-upgrade column, or an unrecognized
    /// stored value). This is the default.
    #[default]
    Unknown,
}

impl StartSource {
    /// The stable, snake_case wire string for this source.
    ///
    /// Matches the `#[serde(rename_all = "snake_case")]` representation and is
    /// what is written to the `start_source` column.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Api => "api",
            Self::Schedule => "schedule",
            Self::Backfill => "backfill",
            Self::SignalWithStart => "signal_with_start",
            Self::UpdateWithStart => "update_with_start",
            Self::CompletionTrigger => "completion_trigger",
            Self::Webhook => "webhook",
            Self::Child => "child",
            Self::Batch => "batch",
            Self::ContinueAsNew => "continue_as_new",
            Self::Reset => "reset",
            Self::Outbox => "outbox",
            Self::Unknown => "unknown",
        }
    }

    /// Parse a stored/source string into a [`StartSource`].
    ///
    /// Unrecognized input maps to [`StartSource::Unknown`] and never errors, so
    /// a value written by a newer build is tolerated by an older reader.
    #[must_use]
    pub fn from_str(s: &str) -> Self {
        match s {
            "api" => Self::Api,
            "schedule" => Self::Schedule,
            "backfill" => Self::Backfill,
            "signal_with_start" => Self::SignalWithStart,
            "update_with_start" => Self::UpdateWithStart,
            "completion_trigger" => Self::CompletionTrigger,
            "webhook" => Self::Webhook,
            "child" => Self::Child,
            "batch" => Self::Batch,
            "continue_as_new" => Self::ContinueAsNew,
            "reset" => Self::Reset,
            "outbox" => Self::Outbox,
            _ => Self::Unknown,
        }
    }
}

// ---------------------------------------------------------------------------
// IdempotencyKey
// ---------------------------------------------------------------------------

/// A stable, deterministic idempotency key for a single logical activity
/// invocation.
///
/// The key is derived from the `ActivityExecId` recorded in the
/// `ActivityScheduled` (or `LocalActivityScheduled`) event the first time the
/// activity is dispatched.  Because that event is part of the durable history,
/// the key is identical across:
///
/// - worker restarts
/// - duplicate task-queue dispatch
/// - deterministic replay
/// - every retry attempt for the same logical invocation
///
/// Two distinct activity invocations in the same workflow execution receive
/// distinct keys, even when they call the same activity with the same input.
///
/// ## Subkeys
///
/// Use [`subkey`](Self::subkey) to derive a named child key when one activity
/// must produce multiple distinct side effects (e.g. charge + notify).
/// Subkeys are stable and collision-resistant within their parent.
///
/// ## HTTP-header safety
///
/// Both base keys and subkeys contain only printable ASCII characters and are
/// safe to use as the value of an `Idempotency-Key` HTTP request header.
///
/// ## Example
///
/// ```rust
/// use autumn_harvest::types::{ActivityExecId, IdempotencyKey};
///
/// // In production the engine sets this on ActivityContext for you.
/// let id = ActivityExecId::new();
/// let key = IdempotencyKey::from_activity_exec_id(id);
///
/// // Pass the base key to a payment gateway or email provider.
/// let _charge_key: &str = key.as_str();
///
/// // Derive a subkey for a second outbound call within the same activity.
/// let notify_key = key.subkey("notify");
/// assert_ne!(key.as_str(), notify_key.as_str());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdempotencyKey {
    base: String,
}

impl IdempotencyKey {
    /// Build an `IdempotencyKey` from the stable `ActivityExecId` for this
    /// logical activity invocation.
    #[must_use]
    pub fn from_activity_exec_id(id: ActivityExecId) -> Self {
        Self {
            base: id.as_uuid().to_string(),
        }
    }

    /// The key as a string slice — safe to pass directly as an
    /// `Idempotency-Key` HTTP header value.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.base
    }

    /// Derive a stable named subkey for a secondary outbound call within the
    /// same activity.
    ///
    /// `name` should be a short, stable identifier (e.g. `"charge"`,
    /// `"notify"`, `"provision"`).  Distinct names always produce distinct
    /// subkeys.  The same `(parent_key, name)` pair always produces the same
    /// subkey, making it safe to use in retry loops.
    ///
    /// Subkeys are themselves `IdempotencyKey` values so they can be further
    /// nested if necessary.
    ///
    /// # Panics
    ///
    /// Panics if `name` is empty, contains `/`, or contains any character that
    /// is not printable ASCII (byte range `0x21`–`0x7E`).  Subkey names are
    /// programmer-provided constants; a panic surfaces the mistake immediately
    /// rather than silently producing a malformed or colliding key.
    #[must_use]
    pub fn subkey(&self, name: &str) -> Self {
        assert!(
            !name.is_empty() && name.bytes().all(|b| b > b' ' && b < b'\x7f' && b != b'/'),
            "IdempotencyKey::subkey: name must be non-empty printable ASCII without '/'; got {name:?}"
        );
        Self {
            base: format!("{}/{name}", self.base),
        }
    }
}

impl fmt::Display for IdempotencyKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.base)
    }
}

// ── ParentClosePolicy ─────────────────────────────────────────────────────────

/// Determines what happens to a detached child workflow when its parent
/// reaches a terminal state (Completed, Failed, Cancelled, Terminated, or
/// execution-timeout).
///
/// This is only relevant for children spawned via
/// [`WorkflowContext::spawn_child_workflow_detached_raw`]. Children spawned via
/// the await path (`spawn_child_workflow_raw`) pin the parent alive until the
/// child terminates — no cascade policy is needed.
///
/// The three variants mirror Temporal's `ParentClosePolicy` so that operators
/// with Temporal experience can map their mental model directly.
///
/// ## Default
///
/// When calling `spawn_child_workflow_detached_raw`, the default policy is
/// `RequestCancel`. Children that should outlive their parent unconditionally
/// must opt in to `Abandon` explicitly.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::types::ParentClosePolicy;
///
/// let policy = ParentClosePolicy::default();
/// assert_eq!(policy, ParentClosePolicy::RequestCancel);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ParentClosePolicy {
    /// Children continue independently — no cascade when the parent closes.
    ///
    /// Use for "fire-and-forget" fan-out and long-lived monitor patterns where
    /// the child must outlive the parent regardless of how the parent terminates.
    Abandon,
    /// Parent close emits a cancel signal at each child's root. Children
    /// observe `ctx.is_cancelled()` and may exit cleanly, running compensations
    /// if needed. This is the **default**.
    ///
    /// Use when children should be informed of the parent's demise and given a
    /// chance to shut down gracefully. Children that ignore cancellation will
    /// continue running; there is no hard-kill guarantee.
    #[default]
    RequestCancel,
    /// Children are force-failed by the executor without running compensations
    /// or further activities. The child's terminal event will carry the error
    /// `"ParentClosed"`.
    ///
    /// Use for "tear-down cascade" — when the parent's failure means child
    /// results are meaningless and immediate cleanup of queue tasks is desired.
    Terminate,
}

impl ParentClosePolicy {
    /// Returns the canonical lowercase string representation used in the DB.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Abandon => "abandon",
            Self::RequestCancel => "request_cancel",
            Self::Terminate => "terminate",
        }
    }
}

impl fmt::Display for ParentClosePolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for ParentClosePolicy {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "abandon" => Ok(Self::Abandon),
            "request_cancel" => Ok(Self::RequestCancel),
            "terminate" => Ok(Self::Terminate),
            other => Err(format!("unknown ParentClosePolicy: {other:?}")),
        }
    }
}

// ── BuildId ───────────────────────────────────────────────────────────────────

/// Immutable build identifier advertised by a worker process.
///
/// Operators choose a stable string for each deployable binary (e.g. a Git SHA,
/// a semantic version, or a CI job ID). Harvest uses this to ensure in-flight
/// workflow executions are only resumed by workers running a compatible build.
///
/// The empty string `""` is the **legacy sentinel**: workers that pre-date
/// build routing (or operators who have not opted in) advertise an empty
/// `BuildId` and retain the ability to claim any task regardless of the task's
/// `required_build_id`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BuildId(String);

impl BuildId {
    /// Create a new `BuildId` from any string-like value.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// The legacy sentinel used by workers that pre-date build routing.
    ///
    /// Legacy workers advertise an empty build id and are allowed to claim any
    /// task, including those with an explicit `required_build_id`. This
    /// preserves backward compatibility for operators who have not yet adopted
    /// build-aware routing.
    #[must_use]
    pub const fn legacy() -> Self {
        Self(String::new())
    }

    /// Returns `true` when this is the legacy empty-string sentinel.
    #[must_use]
    pub const fn is_legacy(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the underlying string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for BuildId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

// ── DeploymentName ────────────────────────────────────────────────────────────

/// Optional human-readable deployment name for a worker (e.g. `"prod-blue"`).
///
/// Deployment names are purely for operator observability — Harvest does not
/// use them for routing decisions. They are stored alongside `BuildId` in the
/// fleet table and surfaced in worker list/detail responses.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeploymentName(String);

impl DeploymentName {
    /// Create a new `DeploymentName`.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Returns the underlying string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DeploymentName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

// ── Priority ──────────────────────────────────────────────────────────────────

/// Task priority for within-queue ordering (issue #249).
///
/// Workers claim tasks in `priority DESC, available_at ASC` order, so
/// higher-priority tasks are claimed before lower-priority ones that arrived
/// earlier.  Same-priority tasks are FIFO by `available_at`.
///
/// The numeric values are chosen so that `Normal = 0` preserves backward
/// compatibility: pre-upgrade rows written with `priority = 0` continue to
/// behave as `Normal` without any data migration.
///
/// Priority is **queue metadata**, not workflow history.  Changing a task's
/// priority via `PATCH /tasks/{id}` does not add a `WorkflowEvent`, does not
/// affect replay determinism, and takes effect on the next claim attempt.
///
/// ## Anti-starvation
///
/// When `WorkerConfig::priority_aging_secs` is set, a task's effective
/// priority is boosted by `+1` for every `aging_secs` it has waited in
/// `PENDING` state.  This bounds the maximum starvation time for `Low`
/// priority tasks even under sustained high-priority load.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Priority {
    /// Lower than normal; used for bulk background work that should yield to
    /// all interactive traffic.  Maps to `-1` in the database so `ORDER BY
    /// priority DESC` places it behind `Normal`.
    Low,
    /// Default priority.  Pre-upgrade rows with `priority = 0` are treated as
    /// `Normal`.  Maps to `0`.
    #[default]
    Normal,
    /// Above-normal urgency; claim-ordered ahead of `Normal` and `Low` tasks.
    /// Maps to `1`.
    High,
    /// Highest urgency lane; use sparingly for incident-response runbooks and
    /// latency-SLA paths.  Maps to `2`.
    Critical,
}

impl Priority {
    /// Returns the integer value stored in the `priority` column.
    ///
    /// `ORDER BY priority DESC` in the claim query ensures `Critical (2) >
    /// High (1) > Normal (0) > Low (-1)`.
    #[must_use]
    pub const fn as_i32(self) -> i32 {
        match self {
            Self::Low => -1,
            Self::Normal => 0,
            Self::High => 1,
            Self::Critical => 2,
        }
    }

    /// Convert from a database integer back to a `Priority`.
    ///
    /// Returns `None` for values outside the declared range so callers can
    /// handle unknown values gracefully (e.g. treat as `Normal`).
    #[must_use]
    pub const fn from_i32(v: i32) -> Option<Self> {
        match v {
            -1 => Some(Self::Low),
            0 => Some(Self::Normal),
            1 => Some(Self::High),
            2 => Some(Self::Critical),
            _ => None,
        }
    }
}

impl fmt::Display for Priority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Low => f.write_str("low"),
            Self::Normal => f.write_str("normal"),
            Self::High => f.write_str("high"),
            Self::Critical => f.write_str("critical"),
        }
    }
}

impl std::str::FromStr for Priority {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "low" => Ok(Self::Low),
            "normal" => Ok(Self::Normal),
            "high" => Ok(Self::High),
            "critical" => Ok(Self::Critical),
            other => Err(format!(
                "unknown priority '{other}'; expected low | normal | high | critical"
            )),
        }
    }
}

impl<'de> serde::Deserialize<'de> for Priority {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
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

    #[test]
    fn should_display_ids_correctly() {
        assert_eq!(WorkflowId::new("wf-123").to_string(), "wf-123");
        assert_eq!(TimerId::new("timer-456").to_string(), "timer-456");
        assert_eq!(WorkerId::new("worker-789").to_string(), "worker-789");

        let token = ExternalActivityToken::new();
        assert_eq!(token.to_string(), token.as_uuid().to_string());
    }

    #[test]
    fn should_parse_uuid_ids_correctly() {
        let exec_uuid = uuid::Uuid::new_v4();
        let exec_id_str = exec_uuid.to_string();
        let parsed_exec_id: ExecutionId = exec_id_str.parse().unwrap();
        assert_eq!(parsed_exec_id.as_uuid(), exec_uuid);

        let act_uuid = uuid::Uuid::new_v4();
        let act_id_str = act_uuid.to_string();
        let parsed_act_id: ActivityExecId = act_id_str.parse().unwrap();
        assert_eq!(parsed_act_id.as_uuid(), act_uuid);

        let ext_uuid = uuid::Uuid::new_v4();
        let ext_id_str = ext_uuid.to_string();
        let parsed_ext_id: ExternalActivityToken = ext_id_str.parse().unwrap();
        assert_eq!(parsed_ext_id.as_uuid(), ext_uuid);
    }

    #[test]
    fn should_return_error_for_invalid_uuid_parse() {
        let invalid_uuid = "not-a-valid-uuid";
        assert!(invalid_uuid.parse::<ExecutionId>().is_err());
        assert!(invalid_uuid.parse::<ActivityExecId>().is_err());
        assert!(invalid_uuid.parse::<ExternalActivityToken>().is_err());
    }

    #[test]
    fn build_id_legacy_sentinel() {
        let legacy = BuildId::legacy();
        assert!(legacy.is_legacy());
        assert!(!BuildId::new("v1").is_legacy());
    }

    #[test]
    fn deployment_name_round_trip() {
        let n = DeploymentName::new("canary");
        let json = serde_json::to_string(&n).unwrap();
        let back: DeploymentName = serde_json::from_str(&json).unwrap();
        assert_eq!(n, back);
    }

    #[test]
    fn session_id_is_random_uuid() {
        let a = SessionId::new();
        let b = SessionId::new();
        assert_ne!(a, b);
    }

    #[test]
    fn session_id_display_and_from_str_round_trip() {
        let id = SessionId::new();
        let s = id.to_string();
        let parsed: SessionId = s.parse().unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn session_id_from_uuid_and_as_uuid_round_trip() {
        let uuid = uuid::Uuid::new_v4();
        let id = SessionId::from_uuid(uuid);
        assert_eq!(id.as_uuid(), uuid);
    }

    #[test]
    fn session_id_serde_round_trip() {
        let id = SessionId::new();
        let json = serde_json::to_string(&id).unwrap();
        let back: SessionId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, back);
    }

    #[test]
    fn session_id_invalid_str_fails_to_parse() {
        assert!("not-a-valid-uuid".parse::<SessionId>().is_err());
    }

    #[test]
    fn priority_round_trips_through_string_and_display() {
        for priority in [
            Priority::Low,
            Priority::Normal,
            Priority::High,
            Priority::Critical,
        ] {
            let s = priority.to_string();
            let parsed: Priority = s.parse().expect("valid priority string should parse");
            assert_eq!(parsed, priority);
        }
    }

    #[test]
    fn priority_rejects_unknown_string() {
        let err = "highest"
            .parse::<Priority>()
            .expect_err("unknown priority should fail");
        assert!(err.contains("unknown priority"));
    }

    #[test]
    fn priority_maps_to_and_from_i32() {
        assert_eq!(Priority::Low.as_i32(), -1);
        assert_eq!(Priority::Normal.as_i32(), 0);
        assert_eq!(Priority::High.as_i32(), 1);
        assert_eq!(Priority::Critical.as_i32(), 2);

        assert_eq!(Priority::from_i32(-1), Some(Priority::Low));
        assert_eq!(Priority::from_i32(0), Some(Priority::Normal));
        assert_eq!(Priority::from_i32(1), Some(Priority::High));
        assert_eq!(Priority::from_i32(2), Some(Priority::Critical));

        assert_eq!(Priority::from_i32(3), None);
        assert_eq!(Priority::from_i32(-2), None);
    }

    #[test]
    fn priority_round_trips_serde() {
        let cases = vec![
            (Priority::Low, "\"low\""),
            (Priority::Normal, "\"normal\""),
            (Priority::High, "\"high\""),
            (Priority::Critical, "\"critical\""),
        ];

        for (priority, expected_json) in cases {
            let json = serde_json::to_string(&priority).unwrap();
            assert_eq!(json, expected_json);

            let deserialized: Priority = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, priority);
        }
    }

    #[test]
    fn start_source_as_str_from_str_round_trip() {
        let all = [
            StartSource::Api,
            StartSource::Schedule,
            StartSource::Backfill,
            StartSource::SignalWithStart,
            StartSource::UpdateWithStart,
            StartSource::CompletionTrigger,
            StartSource::Webhook,
            StartSource::Child,
            StartSource::Batch,
            StartSource::ContinueAsNew,
            StartSource::Reset,
            StartSource::Outbox,
            StartSource::Unknown,
        ];
        for src in all {
            assert_eq!(
                StartSource::from_str(src.as_str()),
                src,
                "round-trip failed for {src:?}"
            );
        }
    }

    #[test]
    fn start_source_from_str_unrecognized_maps_to_unknown() {
        assert_eq!(StartSource::from_str("garbage"), StartSource::Unknown);
        assert_eq!(StartSource::from_str(""), StartSource::Unknown);
        assert_eq!(StartSource::from_str("API"), StartSource::Unknown);
        assert_eq!(StartSource::from_str("unknown"), StartSource::Unknown);
    }

    #[test]
    fn start_source_default_is_unknown() {
        assert_eq!(StartSource::default(), StartSource::Unknown);
    }

    #[test]
    fn start_source_serde_matches_as_str() {
        // serde snake_case rename must agree with as_str() for every variant.
        let cases = [
            StartSource::Api,
            StartSource::SignalWithStart,
            StartSource::CompletionTrigger,
            StartSource::ContinueAsNew,
            StartSource::Unknown,
        ];
        for src in cases {
            let json = serde_json::to_string(&src).unwrap();
            assert_eq!(json, format!("\"{}\"", src.as_str()));
            let de: StartSource = serde_json::from_str(&json).unwrap();
            assert_eq!(de, src);
        }
    }
}
