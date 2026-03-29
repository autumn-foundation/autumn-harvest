# Autumn Harvest Phase 1 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build the foundational `autumn-harvest` workspace — a Postgres-backed durable workflow engine companion to the Autumn web framework — covering core types, event sourcing, Diesel persistence layer, and proc macros for `#[workflow]` and `#[activity]`.

**Architecture:** Separate Cargo workspace at `~/autumn-harvest/` with two crates: `autumn-harvest` (core lib with Diesel models, event store, workflow/activity traits, builder) and `autumn-harvest-macros` (proc-macro crate generating companion functions). Companion function pattern mirrors Autumn's `__autumn_task_info_*` pattern exactly. Depends on `autumn-web` via a local path dependency.

**Tech Stack:** Rust 1.86+, edition 2024, Tokio, Diesel 2 + diesel-async, deadpool, Postgres, thiserror, serde/serde_json, uuid, chrono.

---

## Schema Note: UUIDs for Execution IDs

The `i64` PK convention applies to application domain tables. Workflow/activity execution IDs use UUIDs because they must be collision-free across distributed shards *without a DB roundtrip* (needed at enqueue time before the row exists). The `harvest_events` table uses `BIGSERIAL` (i64) for its PK since it's purely append-local.

---

### Task 1: Initialize workspace and git repo

**Files:**
- Create: `~/autumn-harvest/Cargo.toml`
- Create: `~/autumn-harvest/rustfmt.toml`
- Create: `~/autumn-harvest/clippy.toml`
- Create: `~/autumn-harvest/.gitignore`
- Create: `~/autumn-harvest/autumn-harvest/Cargo.toml`
- Create: `~/autumn-harvest/autumn-harvest-macros/Cargo.toml`

**Step 1: Create workspace directories**

```bash
mkdir -p ~/autumn-harvest/autumn-harvest/src
mkdir -p ~/autumn-harvest/autumn-harvest-macros/src
mkdir -p ~/autumn-harvest/autumn-harvest/migrations
cd ~/autumn-harvest && git init
```

**Step 2: Write workspace Cargo.toml**

```toml
# ~/autumn-harvest/Cargo.toml
[workspace]
members = ["autumn-harvest", "autumn-harvest-macros"]
resolver = "3"

[workspace.package]
edition = "2024"
rust-version = "1.86.0"
version = "0.1.0"
license = "MIT OR Apache-2.0"
repository = "https://github.com/madmax983/autumn-harvest"

[workspace.dependencies]
# Core
autumn-web = { path = "../autumn/autumn" }
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "2"
uuid = { version = "1", features = ["v4", "serde"] }
chrono = { version = "0.4", features = ["serde"] }
tracing = "0.1"
futures = "0.3"

# Database
diesel = { version = "2", features = ["postgres", "uuid", "chrono", "serde_json"] }
diesel-async = { version = "0.5", features = ["deadpool", "postgres"] }
deadpool = "0.12"

# Proc macro
syn = { version = "2", features = ["full"] }
quote = "1"
proc-macro2 = "1"

[workspace.lints.clippy]
pedantic = { level = "warn", priority = -1 }
nursery = { level = "warn", priority = -1 }
```

**Step 3: Write rustfmt.toml and clippy.toml**

```toml
# ~/autumn-harvest/rustfmt.toml
edition = "2024"
max_width = 100
```

```toml
# ~/autumn-harvest/clippy.toml
# msrv = "1.86.0"
```

**Step 4: Write .gitignore**

```
/target
Cargo.lock
*.env
```

**Step 5: Write autumn-harvest-macros/Cargo.toml**

```toml
[package]
name = "autumn-harvest-macros"
description = "Proc macros for autumn-harvest workflow engine"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[lib]
proc-macro = true

[dependencies]
syn = { workspace = true }
quote = { workspace = true }
proc-macro2 = { workspace = true }
```

**Step 6: Write autumn-harvest/Cargo.toml**

```toml
[package]
name = "autumn-harvest"
description = "Durable workflow orchestration engine for the Autumn web framework"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[dependencies]
autumn-web = { workspace = true }
autumn-harvest-macros = { path = "../autumn-harvest-macros" }
tokio = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }
uuid = { workspace = true }
chrono = { workspace = true }
tracing = { workspace = true }
futures = { workspace = true }
diesel = { workspace = true }
diesel-async = { workspace = true }

[dev-dependencies]
tokio = { workspace = true, features = ["test-util"] }
```

**Step 7: Create stub lib files so workspace builds**

```rust
// autumn-harvest-macros/src/lib.rs
// Stub - populated in Task 12 and 13
use proc_macro::TokenStream;

#[proc_macro_attribute]
pub fn workflow(_attr: TokenStream, item: TokenStream) -> TokenStream { item }

#[proc_macro_attribute]
pub fn activity(_attr: TokenStream, item: TokenStream) -> TokenStream { item }
```

```rust
// autumn-harvest/src/lib.rs
// Stub - populated in subsequent tasks
```

**Step 8: Verify workspace builds**

```bash
cd ~/autumn-harvest && cargo build
```

Expected: compiles (possibly with warnings about empty crates — that's fine)

**Step 9: Commit**

```bash
git add -A && git commit -m "chore: initialize autumn-harvest workspace"
```

---

### Task 2: Core identity types

**Files:**
- Create: `autumn-harvest/src/types.rs`
- Modify: `autumn-harvest/src/lib.rs`

**Step 1: Write failing test first**

In `autumn-harvest/src/types.rs`:

```rust
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
        assert_ne!(a, b); // UUIDs are random
    }

    #[test]
    fn activity_exec_id_display_roundtrip() {
        let id = ActivityExecId::new();
        let s = id.to_string();
        let parsed: ActivityExecId = s.parse().unwrap();
        assert_eq!(id, parsed);
    }
}
```

**Step 2: Run test — expect FAIL**

```bash
cd ~/autumn-harvest && cargo test -p autumn-harvest types
```

Expected: compile error (types not defined yet)

**Step 3: Implement types**

```rust
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
/// `"user-123"` or `"order-456"`). It is NOT the run ID — multiple
/// runs of the same workflow may share a `WorkflowId` in a retry scenario.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkflowId(String);

impl WorkflowId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExecutionId(Uuid);

impl ExecutionId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    #[must_use]
    pub fn as_uuid(&self) -> Uuid {
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActivityExecId(Uuid);

impl ActivityExecId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    #[must_use]
    pub fn as_uuid(&self) -> Uuid {
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TimerId(String);

impl TimerId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl fmt::Display for TimerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Identifies a worker instance (hostname + PID or UUID).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerId(String);

impl WorkerId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl fmt::Display for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
```

**Step 4: Expose in lib.rs**

```rust
pub mod types;
pub use types::{ActivityExecId, ExecutionId, TimerId, WorkerId, WorkflowId};
```

**Step 5: Run test — expect PASS**

```bash
cargo test -p autumn-harvest types
```

**Step 6: Commit**

```bash
git add -A && git commit -m "feat(types): add WorkflowId, ExecutionId, ActivityExecId, TimerId, WorkerId newtypes"
```

---

### Task 3: HarvestError and HarvestResult

**Files:**
- Create: `autumn-harvest/src/error.rs`

**Step 1: Write failing test**

```rust
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
    fn harvest_result_ok() {
        let r: HarvestResult<i32> = Ok(42);
        assert_eq!(r.unwrap(), 42);
    }
}
```

**Step 2: Run test — expect FAIL**

```bash
cargo test -p autumn-harvest error
```

**Step 3: Implement error types**

```rust
//! Error types for the harvest engine.
//!
//! `HarvestError` is a proper `std::error::Error` (via thiserror) so it can be
//! propagated with `?` through internal engine code and wrapped in `AutumnError`
//! at the boundary where workflow/activity results leave the engine.

use std::time::Duration;

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
///
/// This type implements `std::error::Error` so it propagates normally inside
/// engine internals. At the public boundary (activity/workflow return types),
/// callers use `AutumnResult<T>` from `autumn-web` — the blanket
/// `From<E: Error>` impl converts `HarvestError` to a 500 `AutumnError`.
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
    Timeout { timeout_type: TimeoutType, task_name: String },

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

/// Compute the next retry delay using exponential backoff.
///
/// `attempt` is 1-based (attempt 1 = first retry, gets `initial`).
#[must_use]
pub fn compute_retry_delay(
    initial: Duration,
    backoff_coefficient: f64,
    max_interval: Duration,
    attempt: u32,
) -> Duration {
    let secs = initial.as_secs_f64() * backoff_coefficient.powi((attempt - 1) as i32);
    Duration::from_secs_f64(secs.min(max_interval.as_secs_f64()))
}
```

**Step 4: Expose in lib.rs**

```rust
pub mod error;
pub use error::{HarvestError, HarvestResult, TimeoutType, compute_retry_delay};
```

**Step 5: Run test — expect PASS**

```bash
cargo test -p autumn-harvest error
```

**Step 6: Commit**

```bash
git add -A && git commit -m "feat(error): add HarvestError, HarvestResult, TimeoutType"
```

---

### Task 4: RetryPolicy and scheduling types

**Files:**
- Create: `autumn-harvest/src/policy.rs`

**Step 1: Write failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn exponential_backoff_doubles() {
        let policy = RetryPolicy::exponential(5, Duration::from_secs(1));
        // attempt 1: 1s, attempt 2: 2s, attempt 3: 4s
        let d1 = policy.next_delay(1);
        let d2 = policy.next_delay(2);
        let d3 = policy.next_delay(3);
        assert_eq!(d1, Some(Duration::from_secs(1)));
        assert_eq!(d2, Some(Duration::from_secs(2)));
        assert_eq!(d3, Some(Duration::from_secs(4)));
    }

    #[test]
    fn fixed_backoff_stays_constant() {
        let policy = RetryPolicy::fixed(3, Duration::from_secs(5));
        assert_eq!(policy.next_delay(1), Some(Duration::from_secs(5)));
        assert_eq!(policy.next_delay(2), Some(Duration::from_secs(5)));
    }

    #[test]
    fn no_retry_after_max_attempts() {
        let policy = RetryPolicy::exponential(3, Duration::from_secs(1));
        // attempt 3 is the last — no delay (we're done)
        assert_eq!(policy.next_delay(3), None);
    }

    #[test]
    fn exponential_caps_at_max_interval() {
        let policy = RetryPolicy {
            max_attempts: 10,
            initial_interval: Duration::from_secs(60),
            backoff_coefficient: 2.0,
            max_interval: Duration::from_secs(120),
            non_retryable_errors: vec![],
        };
        // Would be 60 * 2^5 = 1920s, capped at 120s
        let d = policy.next_delay(6).unwrap();
        assert_eq!(d, Duration::from_secs(120));
    }

    #[test]
    fn trigger_rule_all_success_requires_all_success() {
        let results = vec![TaskStatus::Succeeded, TaskStatus::Succeeded];
        assert!(TriggerRule::AllSuccess.should_run(&results));

        let results = vec![TaskStatus::Succeeded, TaskStatus::Failed];
        assert!(!TriggerRule::AllSuccess.should_run(&results));
    }

    #[test]
    fn trigger_rule_all_done_runs_on_any_completion() {
        let results = vec![TaskStatus::Succeeded, TaskStatus::Failed];
        assert!(TriggerRule::AllDone.should_run(&results));
    }
}
```

**Step 2: Run test — expect FAIL**

```bash
cargo test -p autumn-harvest policy
```

**Step 3: Implement policy types**

```rust
//! Retry policies, trigger rules, and scheduling types.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// How an activity failure is retried.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Maximum number of attempts (including the first). 1 = no retries.
    pub max_attempts: u32,
    /// Delay before the first retry.
    pub initial_interval: Duration,
    /// Multiplier applied after each retry (`1.0` = fixed delay).
    pub backoff_coefficient: f64,
    /// Upper bound on delay between retries.
    pub max_interval: Duration,
    /// Error type names (as returned by `std::error::Error::source` chain)
    /// that must not be retried.
    pub non_retryable_errors: Vec<String>,
}

impl RetryPolicy {
    /// Exponential backoff: doubles each retry, capped at 5 minutes.
    pub fn exponential(max_attempts: u32, initial: Duration) -> Self {
        Self {
            max_attempts,
            initial_interval: initial,
            backoff_coefficient: 2.0,
            max_interval: Duration::from_secs(300),
            non_retryable_errors: vec![],
        }
    }

    /// Fixed delay: same interval every retry.
    pub fn fixed(max_attempts: u32, interval: Duration) -> Self {
        Self {
            max_attempts,
            initial_interval: interval,
            backoff_coefficient: 1.0,
            max_interval: interval,
            non_retryable_errors: vec![],
        }
    }

    /// Returns the delay before the given attempt number, or `None` if
    /// `attempt >= max_attempts` (i.e., no more retries).
    ///
    /// `attempt` is 1-based: attempt 1 = first retry (after initial failure).
    #[must_use]
    pub fn next_delay(&self, attempt: u32) -> Option<Duration> {
        if attempt >= self.max_attempts {
            return None;
        }
        let secs =
            self.initial_interval.as_secs_f64() * self.backoff_coefficient.powi((attempt - 1) as i32);
        Some(Duration::from_secs_f64(secs.min(self.max_interval.as_secs_f64())))
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::exponential(3, Duration::from_secs(1))
    }
}

/// Status of a completed DAG task, used by trigger rules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskStatus {
    Succeeded,
    Failed,
    Skipped,
}

/// When a DAG task with multiple upstreams should execute.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum TriggerRule {
    /// Run when all upstream tasks succeeded (default).
    #[default]
    AllSuccess,
    /// Run when all upstream tasks completed (any terminal state).
    AllDone,
    /// Run when at least one upstream succeeded.
    OneSuccess,
    /// Run when at least one upstream failed.
    OneFailed,
    /// Run when all upstream tasks failed.
    AllFailed,
    /// Never auto-trigger; must be triggered manually.
    Manual,
}

impl TriggerRule {
    #[must_use]
    pub fn should_run(&self, upstream_statuses: &[TaskStatus]) -> bool {
        match self {
            Self::AllSuccess => upstream_statuses.iter().all(|s| *s == TaskStatus::Succeeded),
            Self::AllDone => !upstream_statuses.is_empty(),
            Self::OneSuccess => upstream_statuses.iter().any(|s| *s == TaskStatus::Succeeded),
            Self::OneFailed => upstream_statuses.iter().any(|s| *s == TaskStatus::Failed),
            Self::AllFailed => {
                !upstream_statuses.is_empty()
                    && upstream_statuses.iter().all(|s| *s == TaskStatus::Failed)
            }
            Self::Manual => false,
        }
    }
}

/// DAG/workflow execution schedule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Schedule {
    /// Standard cron expression (e.g., `"0 2 * * *"` for daily at 2 AM).
    Cron(String),
    /// Fixed interval from the end of the previous run.
    Interval(Duration),
    /// Only runs when triggered manually via API.
    Manual,
}
```

**Step 4: Expose in lib.rs**

```rust
pub mod policy;
pub use policy::{RetryPolicy, Schedule, TaskStatus, TriggerRule};
```

**Step 5: Run test — expect PASS**

```bash
cargo test -p autumn-harvest policy
```

**Step 6: Commit**

```bash
git add -A && git commit -m "feat(policy): add RetryPolicy, TriggerRule, Schedule"
```

---

### Task 5: WorkflowEvent enum (the event-sourcing heart)

**Files:**
- Create: `autumn-harvest/src/event.rs`

**Step 1: Write failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ActivityExecId, ExecutionId, TimerId};
    use chrono::Utc;

    #[test]
    fn workflow_started_round_trips_serde() {
        let event = WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({"user_id": 42}),
            timestamp: Utc::now(),
        };
        let json = serde_json::to_string(&event).unwrap();
        let back: WorkflowEvent = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, WorkflowEvent::WorkflowStarted { .. }));
    }

    #[test]
    fn activity_scheduled_round_trips() {
        let event = WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "send_email".into(),
            input: serde_json::Value::Null,
            queue: "default".into(),
        };
        let json = serde_json::to_string(&event).unwrap();
        let back: WorkflowEvent = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, WorkflowEvent::ActivityScheduled { .. }));
    }

    #[test]
    fn event_type_name_is_stable() {
        let e = WorkflowEvent::WorkflowCompleted { output: serde_json::Value::Null };
        assert_eq!(e.type_name(), "WorkflowCompleted");
    }
}
```

**Step 2: Run test — expect FAIL**

```bash
cargo test -p autumn-harvest event
```

**Step 3: Implement WorkflowEvent**

```rust
//! Event types for the workflow event-sourcing engine.
//!
//! Every state change in a workflow execution is represented as an event
//! appended to `harvest_events`. Replay re-executes the workflow function
//! from the beginning, feeding recorded results back instead of re-executing
//! activities.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::TimeoutType;
use crate::types::{ActivityExecId, ExecutionId, TimerId, WorkerId};

/// All possible events in a workflow's history.
///
/// This enum is append-only — never remove or reorder variants, since stored
/// JSON must deserialize into the same variants after deployment.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum WorkflowEvent {
    // ── Lifecycle ──────────────────────────────────────────────────
    WorkflowStarted {
        input: serde_json::Value,
        timestamp: DateTime<Utc>,
    },
    WorkflowCompleted {
        output: serde_json::Value,
    },
    WorkflowFailed {
        error: String,
    },
    WorkflowCancelled {
        reason: String,
    },

    // ── Activities ────────────────────────────────────────────────
    ActivityScheduled {
        activity_id: ActivityExecId,
        name: String,
        input: serde_json::Value,
        queue: String,
    },
    ActivityStarted {
        activity_id: ActivityExecId,
        worker_id: WorkerId,
    },
    ActivityCompleted {
        activity_id: ActivityExecId,
        output: serde_json::Value,
    },
    ActivityFailed {
        activity_id: ActivityExecId,
        error: String,
        attempt: u32,
    },
    ActivityTimedOut {
        activity_id: ActivityExecId,
        timeout_type: TimeoutType,
    },
    ActivityHeartbeat {
        activity_id: ActivityExecId,
        details: serde_json::Value,
    },

    // ── Timers ────────────────────────────────────────────────────
    TimerStarted {
        timer_id: TimerId,
        /// Duration in seconds (serde_json doesn't handle Duration natively).
        duration_secs: u64,
    },
    TimerFired {
        timer_id: TimerId,
    },

    // ── Signals ───────────────────────────────────────────────────
    SignalReceived {
        signal_name: String,
        payload: serde_json::Value,
    },

    // ── Child workflows ───────────────────────────────────────────
    ChildWorkflowStarted {
        child_id: ExecutionId,
        workflow_name: String,
        input: serde_json::Value,
    },
    ChildWorkflowCompleted {
        child_id: ExecutionId,
        output: serde_json::Value,
    },
    ChildWorkflowFailed {
        child_id: ExecutionId,
        error: String,
    },

    // ── Markers (user checkpoints) ────────────────────────────────
    MarkerRecorded {
        name: String,
        details: serde_json::Value,
    },
}

impl WorkflowEvent {
    /// Stable string identifier for this event variant, stored in
    /// `harvest_events.event_type`.
    #[must_use]
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::WorkflowStarted { .. } => "WorkflowStarted",
            Self::WorkflowCompleted { .. } => "WorkflowCompleted",
            Self::WorkflowFailed { .. } => "WorkflowFailed",
            Self::WorkflowCancelled { .. } => "WorkflowCancelled",
            Self::ActivityScheduled { .. } => "ActivityScheduled",
            Self::ActivityStarted { .. } => "ActivityStarted",
            Self::ActivityCompleted { .. } => "ActivityCompleted",
            Self::ActivityFailed { .. } => "ActivityFailed",
            Self::ActivityTimedOut { .. } => "ActivityTimedOut",
            Self::ActivityHeartbeat { .. } => "ActivityHeartbeat",
            Self::TimerStarted { .. } => "TimerStarted",
            Self::TimerFired { .. } => "TimerFired",
            Self::SignalReceived { .. } => "SignalReceived",
            Self::ChildWorkflowStarted { .. } => "ChildWorkflowStarted",
            Self::ChildWorkflowCompleted { .. } => "ChildWorkflowCompleted",
            Self::ChildWorkflowFailed { .. } => "ChildWorkflowFailed",
            Self::MarkerRecorded { .. } => "MarkerRecorded",
        }
    }
}
```

**Step 4: Expose in lib.rs**

```rust
pub mod event;
pub use event::WorkflowEvent;
```

**Step 5: Run test — expect PASS**

```bash
cargo test -p autumn-harvest event
```

**Step 6: Commit**

```bash
git add -A && git commit -m "feat(event): add WorkflowEvent enum with full serde round-trip"
```

---

### Task 6: WorkflowInfo and ActivityInfo structs (macro output types)

**Files:**
- Create: `autumn-harvest/src/info.rs`

These are the structs that `#[workflow]` and `#[activity]` companion functions return — the bridge between proc macros and the runtime.

**Step 1: Write failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_info_fields_accessible() {
        let info = WorkflowInfo {
            name: "test_workflow",
            module: "my_app::workflows",
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        };
        assert_eq!(info.name, "test_workflow");
    }

    #[test]
    fn activity_info_default_policy() {
        let info = ActivityInfo {
            name: "test_activity",
            module: "my_app::activities",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_queue: None,
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        };
        assert!(info.default_retry_policy.is_none());
        assert_eq!(info.default_queue, None);
    }
}
```

**Step 2: Run test — expect FAIL**

```bash
cargo test -p autumn-harvest info
```

**Step 3: Implement info types**

```rust
//! Registration types returned by macro-generated companion functions.
//!
//! `#[workflow]` generates `__autumn_workflow_info_*() -> WorkflowInfo`.
//! `#[activity]` generates `__autumn_activity_info_*() -> ActivityInfo`.
//! These are collected by `workflows![]` and `activities![]` bang macros.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use crate::policy::RetryPolicy;

/// Type-erased workflow handler.
///
/// Takes a `WorkflowContext` reference and serialized JSON input;
/// returns serialized JSON output (or an error string).
///
/// The macro wraps the user's typed function to handle
/// serialization/deserialization at the boundary.
pub type WorkflowHandlerFn = fn(
    &crate::context::WorkflowContext,
    serde_json::Value,
) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, String>> + Send + '_>>;

/// Type-erased activity handler.
pub type ActivityHandlerFn = fn(
    &crate::context::ActivityContext,
    serde_json::Value,
) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, String>> + Send + '_>>;

/// Metadata for a registered workflow, generated by `#[workflow]`.
pub struct WorkflowInfo {
    /// Snake-case function name (e.g., `"onboarding_workflow"`).
    pub name: &'static str,
    /// `module_path!()` value from the macro call site.
    pub module: &'static str,
    /// Type-erased dispatch function.
    pub handler: WorkflowHandlerFn,
}

/// Metadata for a registered activity, generated by `#[activity]`.
pub struct ActivityInfo {
    /// Snake-case function name.
    pub name: &'static str,
    /// `module_path!()` value from the macro call site.
    pub module: &'static str,
    /// Default retry policy (overridable at call site).
    pub default_retry_policy: Option<RetryPolicy>,
    /// Default start-to-close timeout.
    pub default_start_to_close: Option<Duration>,
    /// Default heartbeat timeout (`None` = disabled).
    pub default_heartbeat_timeout: Option<Duration>,
    /// Default schedule-to-start timeout.
    pub default_schedule_to_start: Option<Duration>,
    /// Default task queue name (`None` = `"default"`).
    pub default_queue: Option<&'static str>,
    /// Type-erased dispatch function.
    pub handler: ActivityHandlerFn,
}
```

**Note:** `WorkflowContext` and `ActivityContext` are forward-referenced here — declare the `context` module before `info` in lib.rs, or use a forward declaration stub first.

**Step 4: Add context module stub before info in lib.rs**

```rust
pub mod context; // populated in Task 7
pub mod info;
pub use info::{ActivityHandlerFn, ActivityInfo, WorkflowHandlerFn, WorkflowInfo};
```

**Step 5: Create context stub**

```rust
// autumn-harvest/src/context.rs
// Populated in Task 7. Stub to satisfy forward reference from info.rs.

/// Context passed to every workflow function during execution and replay.
pub struct WorkflowContext {
    // Populated in Task 7
    _private: (),
}

/// Context passed to every activity function during execution.
pub struct ActivityContext {
    // Populated in Task 7
    _private: (),
}
```

**Step 6: Run test — expect PASS**

```bash
cargo test -p autumn-harvest info
```

**Step 7: Commit**

```bash
git add -A && git commit -m "feat(info): add WorkflowInfo, ActivityInfo registration types"
```

---

### Task 7: ActivityContext and WorkflowContext skeletons

**Files:**
- Modify: `autumn-harvest/src/context.rs`

This task builds the context structs that activities and workflows receive. Phase 1 focuses on the structure and heartbeat pathway; full replay engine comes in Phase 2.

**Step 1: Write failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activity_context_state_returns_none_when_not_registered() {
        let ctx = ActivityContext::new_test();
        let state: Option<&String> = ctx.state::<String>();
        assert!(state.is_none());
    }

    #[test]
    fn workflow_context_new_is_in_normal_mode() {
        let ctx = WorkflowContext::new_test();
        assert!(!ctx.is_replaying());
    }

    #[test]
    fn workflow_context_replay_mode_flag() {
        let mut ctx = WorkflowContext::new_test();
        ctx.set_replaying(true);
        assert!(ctx.is_replaying());
    }
}
```

**Step 2: Run test — expect FAIL**

```bash
cargo test -p autumn-harvest context
```

**Step 3: Implement context skeletons**

```rust
//! Execution contexts passed to workflow and activity functions.
//!
//! `WorkflowContext` drives deterministic replay — it tracks the event history
//! pointer and routes commands either to real execution or to history lookup.
//!
//! `ActivityContext` provides heartbeating, state access, and a DB connection
//! to activities.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

/// Context passed to every workflow function.
///
/// In **normal mode** (no history to replay): commands generate new events.
/// In **replay mode** (resuming from Postgres history): commands are matched
/// against recorded events and return the stored result without re-executing.
pub struct WorkflowContext {
    /// When `true`, the context is replaying history rather than executing fresh.
    replaying: bool,
    /// Shared state map (same `AppState` extras as the web server).
    state: Arc<HashMap<TypeId, Box<dyn Any + Send + Sync>>>,
    // Phase 2 will add: event history, event pointer, pending commands channel
}

impl WorkflowContext {
    /// Returns `true` if currently replaying recorded event history.
    #[must_use]
    pub fn is_replaying(&self) -> bool {
        self.replaying
    }

    /// Switch replay mode on or off. Called by the worker executor.
    pub fn set_replaying(&mut self, replaying: bool) {
        self.replaying = replaying;
    }

    /// Access typed shared state (e.g., email clients, config).
    ///
    /// Returns `None` if the state type was not registered with the builder.
    #[must_use]
    pub fn state<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.state.get(&TypeId::of::<T>())?.downcast_ref::<T>()
    }

    /// Constructor for testing — creates a context in normal (non-replay) mode
    /// with empty state.
    #[cfg(any(test, feature = "testing"))]
    #[must_use]
    pub fn new_test() -> Self {
        Self { replaying: false, state: Arc::new(HashMap::new()) }
    }
}

/// Context passed to every activity function.
///
/// Activities may perform I/O, call external services, and interact with the
/// database. The context provides heartbeating to signal liveness and state
/// access for shared resources.
pub struct ActivityContext {
    /// Shared state map.
    state: Arc<HashMap<TypeId, Box<dyn Any + Send + Sync>>>,
    // Phase 2 will add: heartbeat sender, cancellation token, DB pool handle
}

impl ActivityContext {
    /// Access typed shared state.
    #[must_use]
    pub fn state<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.state.get(&TypeId::of::<T>())?.downcast_ref::<T>()
    }

    /// Send a heartbeat to signal the activity is still running.
    ///
    /// Phase 1 stub — full implementation in Phase 2 (worker heartbeat loop).
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow was cancelled and the activity should
    /// stop. Activities should check this return value on long operations.
    pub async fn heartbeat(&self, _details: impl serde::Serialize) -> crate::HarvestResult<()> {
        // Phase 2: send details via heartbeat channel to the worker's batch sender
        Ok(())
    }

    /// Constructor for testing.
    #[cfg(any(test, feature = "testing"))]
    #[must_use]
    pub fn new_test() -> Self {
        Self { state: Arc::new(HashMap::new()) }
    }
}
```

**Step 4: Run test — expect PASS**

```bash
cargo test -p autumn-harvest context
```

**Step 5: Commit**

```bash
git add -A && git commit -m "feat(context): add WorkflowContext and ActivityContext skeletons with state access"
```

---

### Task 8: Diesel migrations (Postgres schema)

**Files:**
- Create: `autumn-harvest/migrations/00000000000000_harvest_initial/up.sql`
- Create: `autumn-harvest/migrations/00000000000000_harvest_initial/down.sql`

> **Note:** No test in this task — migrations are verified by running them against a live DB in Task 10 (integration test). This task is pure SQL.

**Step 1: Write up.sql**

```sql
-- autumn-harvest/migrations/00000000000000_harvest_initial/up.sql
-- Workflow execution tracking and event history for autumn-harvest.
-- All tables prefixed with harvest_ to avoid collisions with application tables.

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Workflow executions (one row per run)
CREATE TABLE harvest_workflow_executions (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_name       TEXT NOT NULL,
    workflow_id         TEXT NOT NULL,
    run_id              UUID NOT NULL DEFAULT gen_random_uuid(),
    shard_id            INT NOT NULL,
    state               TEXT NOT NULL DEFAULT 'RUNNING'
                            CHECK (state IN ('RUNNING','COMPLETED','FAILED','CANCELLED','TIMED_OUT')),
    input               JSONB NOT NULL,
    output              JSONB,
    error               TEXT,
    parent_id           UUID REFERENCES harvest_workflow_executions(id),
    sticky_worker_id    TEXT,
    queue_name          TEXT NOT NULL DEFAULT 'default',
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    execution_timeout   INTERVAL,
    memo                JSONB,
    search_attrs        JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (workflow_id, run_id)
);

CREATE INDEX idx_harvest_we_shard  ON harvest_workflow_executions (shard_id);
CREATE INDEX idx_harvest_we_state  ON harvest_workflow_executions (state)
    WHERE state = 'RUNNING';
CREATE INDEX idx_harvest_we_search ON harvest_workflow_executions USING GIN (search_attrs);

-- Event history (append-only log, one sequence per execution)
CREATE TABLE harvest_events (
    id               BIGSERIAL PRIMARY KEY,
    workflow_exec_id UUID NOT NULL REFERENCES harvest_workflow_executions(id) ON DELETE CASCADE,
    event_id         INT NOT NULL,      -- 0, 1, 2, ... within a workflow
    event_type       TEXT NOT NULL,
    event_data       JSONB NOT NULL,
    timestamp        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (workflow_exec_id, event_id)
);

CREATE INDEX idx_harvest_events_exec ON harvest_events (workflow_exec_id, event_id);

-- Task queue (Postgres-backed work queue)
CREATE TABLE harvest_task_queue (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_name          TEXT NOT NULL,
    task_type           TEXT NOT NULL CHECK (task_type IN ('workflow','activity')),
    workflow_exec_id    UUID REFERENCES harvest_workflow_executions(id) ON DELETE CASCADE,
    activity_name       TEXT,
    input               JSONB NOT NULL,
    state               TEXT NOT NULL DEFAULT 'PENDING'
                            CHECK (state IN ('PENDING','RUNNING','COMPLETED','FAILED')),
    priority            INT NOT NULL DEFAULT 0,
    worker_id           TEXT,
    attempt             INT NOT NULL DEFAULT 0,
    max_attempts        INT NOT NULL DEFAULT 1,
    scheduled_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    last_heartbeat_at   TIMESTAMPTZ,
    heartbeat_timeout   INTERVAL,
    start_to_close      INTERVAL,
    schedule_to_start   INTERVAL,
    retry_policy        JSONB,
    output              JSONB,
    error               TEXT
);

CREATE INDEX idx_harvest_tq_poll ON harvest_task_queue
    (queue_name, state, priority DESC, scheduled_at)
    WHERE state = 'PENDING';
CREATE INDEX idx_harvest_tq_running ON harvest_task_queue
    (state, last_heartbeat_at)
    WHERE state = 'RUNNING';
CREATE INDEX idx_harvest_tq_workflow ON harvest_task_queue (workflow_exec_id);

-- DAG runs
CREATE TABLE harvest_dag_runs (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dag_name              TEXT NOT NULL,
    workflow_exec_id      UUID REFERENCES harvest_workflow_executions(id),
    state                 TEXT NOT NULL DEFAULT 'QUEUED'
                              CHECK (state IN ('QUEUED','RUNNING','SUCCESS','FAILED')),
    logical_date          TIMESTAMPTZ NOT NULL,
    data_interval_start   TIMESTAMPTZ NOT NULL,
    data_interval_end     TIMESTAMPTZ NOT NULL,
    conf                  JSONB,
    started_at            TIMESTAMPTZ,
    completed_at          TIMESTAMPTZ,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (dag_name, logical_date)
);

CREATE INDEX idx_harvest_dr_schedule ON harvest_dag_runs (dag_name, state, logical_date);

-- DAG schedules registry
CREATE TABLE harvest_schedules (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dag_name        TEXT NOT NULL UNIQUE,
    schedule_expr   TEXT,
    timezone        TEXT NOT NULL DEFAULT 'UTC',
    catchup         BOOLEAN NOT NULL DEFAULT FALSE,
    max_active_runs INT NOT NULL DEFAULT 1,
    is_paused       BOOLEAN NOT NULL DEFAULT FALSE,
    last_run_at     TIMESTAMPTZ,
    next_run_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Pending signals for running workflows
CREATE TABLE harvest_signals (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_exec_id UUID NOT NULL REFERENCES harvest_workflow_executions(id) ON DELETE CASCADE,
    signal_name      TEXT NOT NULL,
    payload          JSONB NOT NULL,
    received_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    consumed         BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_harvest_signals_pending ON harvest_signals (workflow_exec_id, signal_name)
    WHERE NOT consumed;

-- Durable timers
CREATE TABLE harvest_timers (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_exec_id UUID NOT NULL REFERENCES harvest_workflow_executions(id) ON DELETE CASCADE,
    timer_id         TEXT NOT NULL,
    fires_at         TIMESTAMPTZ NOT NULL,
    fired            BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_harvest_timers_pending ON harvest_timers (fires_at)
    WHERE NOT fired;

-- Dead letter queue
CREATE TABLE harvest_dead_letters (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    original_task_id UUID NOT NULL,
    queue_name       TEXT NOT NULL,
    task_type        TEXT NOT NULL,
    workflow_exec_id UUID,
    activity_name    TEXT,
    input            JSONB NOT NULL,
    error            TEXT NOT NULL,
    attempts         INT NOT NULL,
    failed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

**Step 2: Write down.sql**

```sql
-- autumn-harvest/migrations/00000000000000_harvest_initial/down.sql
DROP TABLE IF EXISTS harvest_dead_letters;
DROP TABLE IF EXISTS harvest_timers;
DROP TABLE IF EXISTS harvest_signals;
DROP TABLE IF EXISTS harvest_schedules;
DROP TABLE IF EXISTS harvest_dag_runs;
DROP TABLE IF EXISTS harvest_task_queue;
DROP TABLE IF EXISTS harvest_events;
DROP TABLE IF EXISTS harvest_workflow_executions;
```

**Step 3: Commit**

```bash
git add -A && git commit -m "feat(migrations): add harvest_* Postgres schema (initial migration)"
```

---

### Task 9: Diesel models (schema.rs + models.rs)

**Files:**
- Create: `autumn-harvest/src/schema.rs`
- Create: `autumn-harvest/src/models.rs`

`★ Insight ─────────────────────────────────────`
Diesel normally auto-generates `schema.rs` via `diesel print-schema`, but we write it by hand here because we don't yet have a live DB in the workspace. The schema must match the migration SQL exactly — column order, types, and nullability all matter.
`─────────────────────────────────────────────────`

**Step 1: Write schema.rs (hand-written to match migration)**

```rust
// autumn-harvest/src/schema.rs
// @generated by diesel — do not edit by hand in production.
// Keep in sync with migrations/00000000000000_harvest_initial/up.sql.

diesel::table! {
    use diesel::sql_types::*;
    use diesel_async::*;

    harvest_workflow_executions (id) {
        id -> Uuid,
        workflow_name -> Text,
        workflow_id -> Text,
        run_id -> Uuid,
        shard_id -> Int4,
        state -> Text,
        input -> Jsonb,
        output -> Nullable<Jsonb>,
        error -> Nullable<Text>,
        parent_id -> Nullable<Uuid>,
        sticky_worker_id -> Nullable<Text>,
        queue_name -> Text,
        started_at -> Timestamptz,
        completed_at -> Nullable<Timestamptz>,
        execution_timeout -> Nullable<Interval>,
        memo -> Nullable<Jsonb>,
        search_attrs -> Nullable<Jsonb>,
        created_at -> Timestamptz,
    }
}

diesel::table! {
    harvest_events (id) {
        id -> Int8,
        workflow_exec_id -> Uuid,
        event_id -> Int4,
        event_type -> Text,
        event_data -> Jsonb,
        timestamp -> Timestamptz,
    }
}

diesel::table! {
    harvest_task_queue (id) {
        id -> Uuid,
        queue_name -> Text,
        task_type -> Text,
        workflow_exec_id -> Nullable<Uuid>,
        activity_name -> Nullable<Text>,
        input -> Jsonb,
        state -> Text,
        priority -> Int4,
        worker_id -> Nullable<Text>,
        attempt -> Int4,
        max_attempts -> Int4,
        scheduled_at -> Timestamptz,
        started_at -> Nullable<Timestamptz>,
        completed_at -> Nullable<Timestamptz>,
        last_heartbeat_at -> Nullable<Timestamptz>,
        heartbeat_timeout -> Nullable<Interval>,
        start_to_close -> Nullable<Interval>,
        schedule_to_start -> Nullable<Interval>,
        retry_policy -> Nullable<Jsonb>,
        output -> Nullable<Jsonb>,
        error -> Nullable<Text>,
    }
}

diesel::table! {
    harvest_dag_runs (id) {
        id -> Uuid,
        dag_name -> Text,
        workflow_exec_id -> Nullable<Uuid>,
        state -> Text,
        logical_date -> Timestamptz,
        data_interval_start -> Timestamptz,
        data_interval_end -> Timestamptz,
        conf -> Nullable<Jsonb>,
        started_at -> Nullable<Timestamptz>,
        completed_at -> Nullable<Timestamptz>,
        created_at -> Timestamptz,
    }
}

diesel::table! {
    harvest_schedules (id) {
        id -> Uuid,
        dag_name -> Text,
        schedule_expr -> Nullable<Text>,
        timezone -> Text,
        catchup -> Bool,
        max_active_runs -> Int4,
        is_paused -> Bool,
        last_run_at -> Nullable<Timestamptz>,
        next_run_at -> Nullable<Timestamptz>,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}

diesel::table! {
    harvest_signals (id) {
        id -> Uuid,
        workflow_exec_id -> Uuid,
        signal_name -> Text,
        payload -> Jsonb,
        received_at -> Timestamptz,
        consumed -> Bool,
    }
}

diesel::table! {
    harvest_timers (id) {
        id -> Uuid,
        workflow_exec_id -> Uuid,
        timer_id -> Text,
        fires_at -> Timestamptz,
        fired -> Bool,
    }
}

diesel::table! {
    harvest_dead_letters (id) {
        id -> Uuid,
        original_task_id -> Uuid,
        queue_name -> Text,
        task_type -> Text,
        workflow_exec_id -> Nullable<Uuid>,
        activity_name -> Nullable<Text>,
        input -> Jsonb,
        error -> Text,
        attempts -> Int4,
        failed_at -> Timestamptz,
    }
}

diesel::joinable!(harvest_events -> harvest_workflow_executions (workflow_exec_id));
diesel::joinable!(harvest_task_queue -> harvest_workflow_executions (workflow_exec_id));
diesel::joinable!(harvest_dag_runs -> harvest_workflow_executions (workflow_exec_id));
diesel::joinable!(harvest_signals -> harvest_workflow_executions (workflow_exec_id));
diesel::joinable!(harvest_timers -> harvest_workflow_executions (workflow_exec_id));

diesel::allow_tables_to_appear_in_same_query!(
    harvest_workflow_executions,
    harvest_events,
    harvest_task_queue,
    harvest_dag_runs,
    harvest_schedules,
    harvest_signals,
    harvest_timers,
    harvest_dead_letters,
);
```

**Step 2: Write models.rs**

```rust
// autumn-harvest/src/models.rs
//! Diesel model structs mapping to harvest_* tables.
//!
//! Each model has two variants:
//! - The full struct (Queryable + Selectable) for reads
//! - A `New*` struct (Insertable) for inserts — only the fields we supply,
//!   letting Postgres fill in defaults.

use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::schema::*;

// ── WorkflowExecution ─────────────────────────────────────────────

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = harvest_workflow_executions)]
pub struct WorkflowExecution {
    pub id: Uuid,
    pub workflow_name: String,
    pub workflow_id: String,
    pub run_id: Uuid,
    pub shard_id: i32,
    pub state: String,
    pub input: serde_json::Value,
    pub output: Option<serde_json::Value>,
    pub error: Option<String>,
    pub parent_id: Option<Uuid>,
    pub sticky_worker_id: Option<String>,
    pub queue_name: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub execution_timeout: Option<chrono::Duration>,
    pub memo: Option<serde_json::Value>,
    pub search_attrs: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = harvest_workflow_executions)]
pub struct NewWorkflowExecution<'a> {
    pub id: Uuid,
    pub workflow_name: &'a str,
    pub workflow_id: &'a str,
    pub run_id: Uuid,
    pub shard_id: i32,
    pub input: serde_json::Value,
    pub queue_name: &'a str,
    pub execution_timeout: Option<chrono::Duration>,
    pub memo: Option<serde_json::Value>,
    pub search_attrs: Option<serde_json::Value>,
}

// ── HarvestEvent ─────────────────────────────────────────────────

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = harvest_events)]
pub struct HarvestEvent {
    pub id: i64,
    pub workflow_exec_id: Uuid,
    pub event_id: i32,
    pub event_type: String,
    pub event_data: serde_json::Value,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = harvest_events)]
pub struct NewHarvestEvent<'a> {
    pub workflow_exec_id: Uuid,
    pub event_id: i32,
    pub event_type: &'a str,
    pub event_data: serde_json::Value,
}

// ── TaskQueue ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = harvest_task_queue)]
pub struct TaskQueueItem {
    pub id: Uuid,
    pub queue_name: String,
    pub task_type: String,
    pub workflow_exec_id: Option<Uuid>,
    pub activity_name: Option<String>,
    pub input: serde_json::Value,
    pub state: String,
    pub priority: i32,
    pub worker_id: Option<String>,
    pub attempt: i32,
    pub max_attempts: i32,
    pub scheduled_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub heartbeat_timeout: Option<chrono::Duration>,
    pub start_to_close: Option<chrono::Duration>,
    pub schedule_to_start: Option<chrono::Duration>,
    pub retry_policy: Option<serde_json::Value>,
    pub output: Option<serde_json::Value>,
    pub error: Option<String>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = harvest_task_queue)]
pub struct NewTaskQueueItem<'a> {
    pub id: Uuid,
    pub queue_name: &'a str,
    pub task_type: &'a str,
    pub workflow_exec_id: Option<Uuid>,
    pub activity_name: Option<&'a str>,
    pub input: serde_json::Value,
    pub priority: i32,
    pub max_attempts: i32,
    pub scheduled_at: DateTime<Utc>,
    pub heartbeat_timeout: Option<chrono::Duration>,
    pub start_to_close: Option<chrono::Duration>,
    pub schedule_to_start: Option<chrono::Duration>,
    pub retry_policy: Option<serde_json::Value>,
}
```

**Step 3: Expose in lib.rs**

```rust
pub mod models;
pub mod schema;
```

**Step 4: Run compile check**

```bash
cargo build -p autumn-harvest
```

Expected: compiles cleanly (Diesel type errors here indicate schema/model mismatch — fix column types to match)

**Step 5: Commit**

```bash
git add -A && git commit -m "feat(db): add Diesel schema and Queryable/Insertable models for harvest_* tables"
```

---

### Task 10: HarvestBuilder fluent API and prelude

**Files:**
- Create: `autumn-harvest/src/builder.rs`
- Create: `autumn-harvest/src/prelude.rs`
- Modify: `autumn-harvest/src/lib.rs`

**Step 1: Write failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::info::{ActivityInfo, WorkflowInfo};

    fn fake_workflow_info() -> WorkflowInfo {
        WorkflowInfo {
            name: "test",
            module: "test",
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        }
    }

    #[test]
    fn harvest_builder_collects_workflows() {
        let builder = HarvestBuilder::new()
            .workflows(vec![fake_workflow_info()]);
        assert_eq!(builder.workflow_count(), 1);
    }

    #[test]
    fn worker_config_default_queues() {
        let config = WorkerConfig::default();
        assert!(config.queues.contains(&"default".to_string()));
    }

    #[test]
    fn worker_config_builder_adds_queues() {
        let config = WorkerConfig::default().with_queues(["email-workers", "etl"]);
        assert!(config.queues.contains(&"email-workers".to_string()));
    }
}
```

**Step 2: Run test — expect FAIL**

```bash
cargo test -p autumn-harvest builder
```

**Step 3: Implement HarvestBuilder and WorkerConfig**

```rust
//! Fluent API for registering workflows, activities, and configuring the worker.

use std::time::Duration;

use crate::info::{ActivityInfo, WorkflowInfo};

/// Fluent builder for configuring the autumn-harvest engine.
///
/// In a full Autumn app, this is consumed by the `HarvestExt` trait on
/// `AppBuilder`. In tests or standalone use, call `.build()` directly.
#[derive(Default)]
pub struct HarvestBuilder {
    workflows: Vec<WorkflowInfo>,
    activities: Vec<ActivityInfo>,
    worker_config: WorkerConfig,
}

impl HarvestBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register workflow definitions (output of `workflows![]` macro).
    #[must_use]
    pub fn workflows(mut self, workflows: Vec<WorkflowInfo>) -> Self {
        self.workflows.extend(workflows);
        self
    }

    /// Register activity definitions (output of `activities![]` macro).
    #[must_use]
    pub fn activities(mut self, activities: Vec<ActivityInfo>) -> Self {
        self.activities.extend(activities);
        self
    }

    /// Configure the worker (concurrency, queues, timeouts).
    #[must_use]
    pub fn worker(mut self, config: WorkerConfig) -> Self {
        self.worker_config = config;
        self
    }

    /// Number of registered workflows (used in tests and diagnostics).
    #[must_use]
    pub fn workflow_count(&self) -> usize {
        self.workflows.len()
    }

    /// Number of registered activities.
    #[must_use]
    pub fn activity_count(&self) -> usize {
        self.activities.len()
    }
}

/// Worker concurrency and queue configuration.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Queues this worker polls. Defaults to `["default"]`.
    pub queues: Vec<String>,
    /// Maximum concurrent workflow executions on this worker.
    pub max_concurrent_workflows: usize,
    /// Maximum concurrent activity executions on this worker.
    pub max_concurrent_activities: usize,
    /// Graceful shutdown timeout.
    pub shutdown_timeout: Duration,
    /// Maximum cached in-memory workflow states (LRU eviction).
    pub workflow_cache_size: usize,
    /// How long to offer sticky tasks to the sticky worker before fallback.
    pub sticky_timeout: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            queues: vec!["default".to_string()],
            max_concurrent_workflows: 20,
            max_concurrent_activities: 50,
            shutdown_timeout: Duration::from_secs(30),
            workflow_cache_size: 1000,
            sticky_timeout: Duration::from_secs(5),
        }
    }
}

impl WorkerConfig {
    /// Replace the queue list.
    #[must_use]
    pub fn with_queues<'a>(mut self, queues: impl IntoIterator<Item = &'a str>) -> Self {
        self.queues = queues.into_iter().map(str::to_owned).collect();
        self
    }
}
```

**Step 4: Write prelude**

```rust
// autumn-harvest/src/prelude.rs
//! Convenient glob import for autumn-harvest users.
//!
//! ```rust,no_run
//! use autumn_harvest::prelude::*;
//! ```

pub use autumn_web::prelude::*;

pub use crate::builder::{HarvestBuilder, WorkerConfig};
pub use crate::context::{ActivityContext, WorkflowContext};
pub use crate::error::{HarvestError, HarvestResult, TimeoutType};
pub use crate::event::WorkflowEvent;
pub use crate::info::{ActivityInfo, WorkflowInfo};
pub use crate::policy::{RetryPolicy, Schedule, TriggerRule};
pub use crate::types::{ActivityExecId, ExecutionId, TimerId, WorkerId, WorkflowId};

// Re-export macros from autumn-harvest-macros
pub use autumn_harvest_macros::{activity, workflow};
```

**Step 5: Finalize lib.rs**

```rust
// autumn-harvest/src/lib.rs
//! Durable workflow orchestration engine for the Autumn web framework.

pub mod builder;
pub mod context;
pub mod error;
pub mod event;
pub mod info;
pub mod models;
pub mod policy;
pub mod prelude;
pub mod schema;
pub mod types;

pub use builder::{HarvestBuilder, WorkerConfig};
pub use context::{ActivityContext, WorkflowContext};
pub use error::{HarvestError, HarvestResult, TimeoutType, compute_retry_delay};
pub use event::WorkflowEvent;
pub use info::{ActivityHandlerFn, ActivityInfo, WorkflowHandlerFn, WorkflowInfo};
pub use policy::{RetryPolicy, Schedule, TaskStatus, TriggerRule};
pub use types::{ActivityExecId, ExecutionId, TimerId, WorkerId, WorkflowId};
```

**Step 6: Run test — expect PASS**

```bash
cargo test -p autumn-harvest builder
```

**Step 7: Commit**

```bash
git add -A && git commit -m "feat(builder): add HarvestBuilder, WorkerConfig, and prelude"
```

---

### Task 11: `#[workflow]` proc macro

**Files:**
- Modify: `autumn-harvest-macros/src/lib.rs`
- Create: `autumn-harvest-macros/src/workflow.rs`

`★ Insight ─────────────────────────────────────`
The companion function must use `::autumn_harvest::` paths — never `::autumn_web::` or upstream paths directly — because downstream crates won't have those as direct deps. This is the key lesson from the `feedback_macro_paths.md` memory.
`─────────────────────────────────────────────────`

**Step 1: Write test in autumn-harvest (not macros) — integration via expansion**

Add a test file `autumn-harvest/tests/macros_workflow.rs`:

```rust
use autumn_harvest::prelude::*;

#[workflow]
async fn test_workflow(ctx: &WorkflowContext, _input: String) -> Result<String, String> {
    Ok("done".into())
}

#[test]
fn workflow_companion_exists_and_returns_info() {
    let info = __autumn_workflow_info_test_workflow();
    assert_eq!(info.name, "test_workflow");
}
```

**Step 2: Run test — expect FAIL (companion fn doesn't exist yet)**

```bash
cargo test -p autumn-harvest --test macros_workflow
```

**Step 3: Implement `#[workflow]` macro**

Create `autumn-harvest-macros/src/workflow.rs`:

```rust
//! `#[workflow]` attribute macro implementation.
//!
//! Emits the original function unchanged plus a companion:
//!   `pub fn __autumn_workflow_info_{name}() -> ::autumn_harvest::WorkflowInfo`

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::ItemFn;

pub fn workflow_macro(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn: ItemFn = match syn::parse2(item) {
        Ok(f) => f,
        Err(e) => return e.to_compile_error(),
    };

    if input_fn.sig.asyncness.is_none() {
        return syn::Error::new_spanned(
            &input_fn.sig.fn_token,
            "#[workflow] functions must be async",
        )
        .to_compile_error();
    }

    let fn_name = &input_fn.sig.ident;
    let fn_name_str = fn_name.to_string();
    let companion_name = format_ident!("__autumn_workflow_info_{fn_name}");

    // Collect parameter names after the first (ctx is first, rest are inputs).
    // For Phase 1 we pass all non-ctx args as a single JSON value.
    let params: Vec<_> = input_fn.sig.inputs.iter().skip(1).collect();
    let param_names: Vec<_> = params
        .iter()
        .filter_map(|arg| {
            if let syn::FnArg::Typed(pat) = arg {
                if let syn::Pat::Ident(ident) = pat.pat.as_ref() {
                    return Some(&ident.ident);
                }
            }
            None
        })
        .collect();

    let dispatch = if param_names.is_empty() {
        quote! {
            let result = #fn_name(ctx).await;
            result.map(|v| ::autumn_harvest::serde_json::to_value(v)
                .unwrap_or(::autumn_harvest::serde_json::Value::Null))
                .map_err(|e| e.to_string())
        }
    } else if param_names.len() == 1 {
        let name = &param_names[0];
        quote! {
            let #name = ::autumn_harvest::serde_json::from_value(input)
                .map_err(|e| e.to_string())?;
            let result = #fn_name(ctx, #name).await;
            result.map(|v| ::autumn_harvest::serde_json::to_value(v)
                .unwrap_or(::autumn_harvest::serde_json::Value::Null))
                .map_err(|e| e.to_string())
        }
    } else {
        // Multiple params: expect input to be a JSON array [arg1, arg2, ...]
        let indices = (0..param_names.len()).map(syn::Index::from);
        let names = &param_names;
        quote! {
            let args: ::autumn_harvest::serde_json::Value = input;
            #(
                let #names = ::autumn_harvest::serde_json::from_value(args[#indices].clone())
                    .map_err(|e| e.to_string())?;
            )*
            let result = #fn_name(ctx, #(#names),*).await;
            result.map(|v| ::autumn_harvest::serde_json::to_value(v)
                .unwrap_or(::autumn_harvest::serde_json::Value::Null))
                .map_err(|e| e.to_string())
        }
    };

    quote! {
        #input_fn

        #[doc(hidden)]
        pub fn #companion_name() -> ::autumn_harvest::WorkflowInfo {
            ::autumn_harvest::WorkflowInfo {
                name: #fn_name_str,
                module: module_path!(),
                handler: |ctx, input| {
                    ::std::boxed::Box::pin(async move {
                        #dispatch
                    })
                },
            }
        }
    }
}
```

**Step 4: Update lib.rs in macros crate**

```rust
use proc_macro::TokenStream;

mod activity;
mod workflow;

#[proc_macro_attribute]
pub fn workflow(attr: TokenStream, item: TokenStream) -> TokenStream {
    workflow::workflow_macro(attr.into(), item.into()).into()
}

#[proc_macro_attribute]
pub fn activity(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Populated in Task 12
    item
}
```

**Step 5: Add serde_json re-export in autumn-harvest lib.rs**

```rust
// Allow macro-generated code to use ::autumn_harvest::serde_json
pub use serde_json;
```

**Step 6: Run test — expect PASS**

```bash
cargo test -p autumn-harvest --test macros_workflow
```

**Step 7: Commit**

```bash
git add -A && git commit -m "feat(macros): implement #[workflow] companion function generator"
```

---

### Task 12: `#[activity]` proc macro

**Files:**
- Create: `autumn-harvest-macros/src/activity.rs`
- Create: `autumn-harvest/tests/macros_activity.rs`

**Step 1: Write failing test**

```rust
// autumn-harvest/tests/macros_activity.rs
use autumn_harvest::prelude::*;
use std::time::Duration;

#[activity]
async fn simple_activity(ctx: &ActivityContext, name: String) -> Result<String, String> {
    Ok(format!("hello {name}"))
}

#[activity(
    retry = RetryPolicy::fixed(3, Duration::from_secs(1)),
    start_to_close = "30s",
    queue = "email-workers"
)]
async fn configured_activity(ctx: &ActivityContext, input: String) -> Result<String, String> {
    Ok(input)
}

#[test]
fn activity_companion_returns_name() {
    let info = __autumn_activity_info_simple_activity();
    assert_eq!(info.name, "simple_activity");
    assert!(info.default_retry_policy.is_none());
    assert_eq!(info.default_queue, None);
}

#[test]
fn configured_activity_companion_has_policy() {
    let info = __autumn_activity_info_configured_activity();
    assert_eq!(info.name, "configured_activity");
    assert!(info.default_retry_policy.is_some());
    assert_eq!(info.default_queue, Some("email-workers"));
    assert_eq!(info.default_start_to_close, Some(Duration::from_secs(30)));
}
```

**Step 2: Run test — expect FAIL**

```bash
cargo test -p autumn-harvest --test macros_activity
```

**Step 3: Implement `#[activity]` macro**

```rust
// autumn-harvest-macros/src/activity.rs
//! `#[activity]` attribute macro implementation.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemFn, LitStr};

struct ActivityAttrs {
    /// RetryPolicy expression string (e.g. `RetryPolicy::fixed(3, ...)`)
    retry: Option<TokenStream>,
    /// Duration string like "30s", "5m"
    start_to_close: Option<String>,
    heartbeat_timeout: Option<String>,
    schedule_to_start: Option<String>,
    queue: Option<String>,
}

fn parse_attrs(attr: TokenStream) -> syn::Result<ActivityAttrs> {
    let mut result = ActivityAttrs {
        retry: None,
        start_to_close: None,
        heartbeat_timeout: None,
        schedule_to_start: None,
        queue: None,
    };

    syn::meta::parser(|meta| {
        if meta.path.is_ident("retry") {
            let value = meta.value()?;
            result.retry = Some(value.parse::<TokenStream>()?);
            Ok(())
        } else if meta.path.is_ident("start_to_close") {
            let value: LitStr = meta.value()?.parse()?;
            result.start_to_close = Some(value.value());
            Ok(())
        } else if meta.path.is_ident("heartbeat_timeout") {
            let value: LitStr = meta.value()?.parse()?;
            result.heartbeat_timeout = Some(value.value());
            Ok(())
        } else if meta.path.is_ident("schedule_to_start") {
            let value: LitStr = meta.value()?.parse()?;
            result.schedule_to_start = Some(value.value());
            Ok(())
        } else if meta.path.is_ident("queue") {
            let value: LitStr = meta.value()?.parse()?;
            result.queue = Some(value.value());
            Ok(())
        } else {
            Err(meta.error("unsupported attribute: expected retry, start_to_close, heartbeat_timeout, schedule_to_start, or queue"))
        }
    })
    .parse2(attr)?;

    Ok(result)
}

fn duration_expr(s: &str) -> TokenStream {
    // Parse "30s", "5m", "1h" etc. into a Duration literal.
    // Reuse parse_duration logic at runtime.
    quote! {
        ::autumn_harvest::task_duration(#s)
            .expect(concat!("invalid duration string: ", #s))
    }
}

pub fn activity_macro(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attrs = match parse_attrs(attr) {
        Ok(a) => a,
        Err(e) => return e.to_compile_error(),
    };

    let input_fn: ItemFn = match syn::parse2(item) {
        Ok(f) => f,
        Err(e) => return e.to_compile_error(),
    };

    if input_fn.sig.asyncness.is_none() {
        return syn::Error::new_spanned(
            &input_fn.sig.fn_token,
            "#[activity] functions must be async",
        )
        .to_compile_error();
    }

    let fn_name = &input_fn.sig.ident;
    let fn_name_str = fn_name.to_string();
    let companion_name = format_ident!("__autumn_activity_info_{fn_name}");

    let retry_expr = attrs.retry.as_ref().map_or_else(
        || quote! { None },
        |policy| quote! { Some(#policy) },
    );

    let start_to_close_expr = attrs.start_to_close.as_deref().map_or_else(
        || quote! { None },
        |s| { let d = duration_expr(s); quote! { Some(#d) } },
    );

    let heartbeat_timeout_expr = attrs.heartbeat_timeout.as_deref().map_or_else(
        || quote! { None },
        |s| { let d = duration_expr(s); quote! { Some(#d) } },
    );

    let schedule_to_start_expr = attrs.schedule_to_start.as_deref().map_or_else(
        || quote! { None },
        |s| { let d = duration_expr(s); quote! { Some(#d) } },
    );

    let queue_expr = attrs.queue.as_deref().map_or_else(
        || quote! { None },
        |q| quote! { Some(#q) },
    );

    // Build dispatch the same way as #[workflow]
    let params: Vec<_> = input_fn.sig.inputs.iter().skip(1).collect();
    let param_names: Vec<_> = params
        .iter()
        .filter_map(|arg| {
            if let syn::FnArg::Typed(pat) = arg {
                if let syn::Pat::Ident(ident) = pat.pat.as_ref() {
                    return Some(&ident.ident);
                }
            }
            None
        })
        .collect();

    let dispatch = if param_names.is_empty() {
        quote! {
            let result = #fn_name(ctx).await;
            result.map(|v| ::autumn_harvest::serde_json::to_value(v)
                .unwrap_or(::autumn_harvest::serde_json::Value::Null))
                .map_err(|e| e.to_string())
        }
    } else if param_names.len() == 1 {
        let name = &param_names[0];
        quote! {
            let #name = ::autumn_harvest::serde_json::from_value(input)
                .map_err(|e| e.to_string())?;
            let result = #fn_name(ctx, #name).await;
            result.map(|v| ::autumn_harvest::serde_json::to_value(v)
                .unwrap_or(::autumn_harvest::serde_json::Value::Null))
                .map_err(|e| e.to_string())
        }
    } else {
        let indices = (0..param_names.len()).map(syn::Index::from);
        let names = &param_names;
        quote! {
            let args: ::autumn_harvest::serde_json::Value = input;
            #(
                let #names = ::autumn_harvest::serde_json::from_value(args[#indices].clone())
                    .map_err(|e| e.to_string())?;
            )*
            let result = #fn_name(ctx, #(#names),*).await;
            result.map(|v| ::autumn_harvest::serde_json::to_value(v)
                .unwrap_or(::autumn_harvest::serde_json::Value::Null))
                .map_err(|e| e.to_string())
        }
    };

    quote! {
        #input_fn

        #[doc(hidden)]
        pub fn #companion_name() -> ::autumn_harvest::ActivityInfo {
            ::autumn_harvest::ActivityInfo {
                name: #fn_name_str,
                module: module_path!(),
                default_retry_policy: #retry_expr,
                default_start_to_close: #start_to_close_expr,
                default_heartbeat_timeout: #heartbeat_timeout_expr,
                default_schedule_to_start: #schedule_to_start_expr,
                default_queue: #queue_expr,
                handler: |ctx, input| {
                    ::std::boxed::Box::pin(async move {
                        #dispatch
                    })
                },
            }
        }
    }
}
```

**Step 4: Add `task_duration` helper in autumn-harvest/src/lib.rs**

```rust
/// Parse a human-readable duration string like `"5m"`, `"30s"`, `"1h"`.
///
/// Used by macro-generated code — not intended for direct use.
#[doc(hidden)]
pub fn task_duration(s: &str) -> Option<std::time::Duration> {
    autumn_web::task::parse_duration(s)
}
```

**Step 5: Wire activity macro in macros lib.rs**

```rust
#[proc_macro_attribute]
pub fn activity(attr: TokenStream, item: TokenStream) -> TokenStream {
    activity::activity_macro(attr.into(), item.into()).into()
}
```

**Step 6: Run test — expect PASS**

```bash
cargo test -p autumn-harvest --test macros_activity
```

**Step 7: Commit**

```bash
git add -A && git commit -m "feat(macros): implement #[activity] with retry/timeout/queue attributes"
```

---

### Task 13: `workflows![]` and `activities![]` bang macros

**Files:**
- Create: `autumn-harvest-macros/src/collect.rs`
- Create: `autumn-harvest/tests/macros_collect.rs`

**Step 1: Write failing test**

```rust
// autumn-harvest/tests/macros_collect.rs
use autumn_harvest::prelude::*;
use autumn_harvest_macros::{activities, workflows};

#[workflow]
async fn wf_a(ctx: &WorkflowContext, _x: String) -> Result<(), String> { Ok(()) }

#[workflow]
async fn wf_b(ctx: &WorkflowContext, _x: String) -> Result<(), String> { Ok(()) }

#[activity]
async fn act_a(ctx: &ActivityContext, _x: String) -> Result<(), String> { Ok(()) }

#[test]
fn workflows_macro_collects_correct_count() {
    let wfs: Vec<WorkflowInfo> = workflows![wf_a, wf_b];
    assert_eq!(wfs.len(), 2);
    assert_eq!(wfs[0].name, "wf_a");
    assert_eq!(wfs[1].name, "wf_b");
}

#[test]
fn activities_macro_collects_correct_count() {
    let acts: Vec<ActivityInfo> = activities![act_a];
    assert_eq!(acts.len(), 1);
    assert_eq!(acts[0].name, "act_a");
}
```

**Step 2: Run test — expect FAIL**

```bash
cargo test -p autumn-harvest --test macros_collect
```

**Step 3: Implement collect macros**

```rust
// autumn-harvest-macros/src/collect.rs
use proc_macro2::TokenStream;
use quote::quote;
use syn::{parse::Parser, punctuated::Punctuated, Ident, Token};

pub fn workflows_macro(input: TokenStream) -> TokenStream {
    let names = match Punctuated::<Ident, Token![,]>::parse_terminated.parse2(input) {
        Ok(n) => n,
        Err(e) => return e.to_compile_error(),
    };

    let calls: Vec<_> = names
        .iter()
        .map(|name| {
            let companion =
                quote::format_ident!("__autumn_workflow_info_{name}");
            quote! { #companion() }
        })
        .collect();

    quote! {
        vec![ #(#calls),* ]
    }
}

pub fn activities_macro(input: TokenStream) -> TokenStream {
    let names = match Punctuated::<Ident, Token![,]>::parse_terminated.parse2(input) {
        Ok(n) => n,
        Err(e) => return e.to_compile_error(),
    };

    let calls: Vec<_> = names
        .iter()
        .map(|name| {
            let companion =
                quote::format_ident!("__autumn_activity_info_{name}");
            quote! { #companion() }
        })
        .collect();

    quote! {
        vec![ #(#calls),* ]
    }
}
```

**Step 4: Register in macros lib.rs**

```rust
mod collect;

#[proc_macro]
pub fn workflows(input: TokenStream) -> TokenStream {
    collect::workflows_macro(input.into()).into()
}

#[proc_macro]
pub fn activities(input: TokenStream) -> TokenStream {
    collect::activities_macro(input.into()).into()
}
```

**Step 5: Re-export in autumn-harvest prelude.rs**

```rust
pub use autumn_harvest_macros::{activities, activity, workflow, workflows};
```

**Step 6: Run test — expect PASS**

```bash
cargo test -p autumn-harvest --test macros_collect
```

**Step 7: Commit**

```bash
git add -A && git commit -m "feat(macros): add workflows![] and activities![] collection macros"
```

---

### Task 14: Full build + clippy pass

**Step 1: Run full test suite**

```bash
cd ~/autumn-harvest && cargo test
```

Expected: all tests pass

**Step 2: Run clippy pedantic + nursery**

```bash
cargo clippy --all-targets -- -D warnings
```

Fix any lints. Common issues:
- `#[allow(clippy::module_name_repetitions)]` for `HarvestError` in `error.rs`
- `#[must_use]` on pure functions
- Missing `#[doc]` on public items
- `Box::pin` closures needing lifetime annotations

**Step 3: Run rustfmt**

```bash
cargo fmt --all -- --check
```

Fix any formatting drift.

**Step 4: Final commit**

```bash
git add -A && git commit -m "chore: clippy + fmt clean across autumn-harvest Phase 1"
```

---

### Task 15: CLAUDE.md for the new repo

**Files:**
- Create: `~/autumn-harvest/CLAUDE.md`

**Step 1: Write CLAUDE.md**

```markdown
# autumn-harvest

Postgres-backed durable workflow orchestration engine for the Autumn web framework.
Companion to `autumn-web`. Implements Temporal-style durable execution + Airflow-style
DAG scheduling, Postgres-only, no external brokers.

## Workspace

- `autumn-harvest/` — core library (traits, event store, context, builder, Diesel models)
- `autumn-harvest-macros/` — proc macros (`#[workflow]`, `#[activity]`, `workflows![]`, etc.)

## Dependency

Requires `autumn-web` via path dep: `path = "../autumn/autumn"`. Publish order when
releasing: `autumn-harvest-macros` first, then `autumn-harvest`.

## Schema

Migrations live in `autumn-harvest/migrations/`. Run with `diesel migration run`.
Schema is hand-maintained in `autumn-harvest/src/schema.rs` — keep in sync with SQL.

## Macro Pattern

`#[workflow]` and `#[activity]` generate companion functions:
- `__autumn_workflow_info_{name}() -> WorkflowInfo`
- `__autumn_activity_info_{name}() -> ActivityInfo`

All generated code uses `::autumn_harvest::` paths — never upstream crate paths.

## Phase Status

- Phase 1 (complete): types, error, event, policy, context stubs, models, macros
- Phase 2 (next): replay engine, task queue worker, LISTEN/NOTIFY, heartbeating
- Phase 3: DAG scheduler, signals/queries, saga pattern, management API
```

**Step 2: Commit**

```bash
git add CLAUDE.md && git commit -m "docs: add CLAUDE.md for autumn-harvest"
```

---

## Design Decision: WorkflowContext Suspension Model

Before Phase 2, you'll need to decide how `WorkflowContext::execute_activity` suspends the workflow coroutine. This is the most architecturally significant decision in the engine. There are two main approaches:

**Option A: Tokio oneshot channels (simpler)**
Each `execute_activity` call creates a oneshot channel, stores the receiver in the context, and `.await`s it. The worker sends the result through the channel when the activity completes. Clean async, but the coroutine stays allocated in memory the whole time.

**Option B: Coroutine serialization (durable but complex)**
Serialize the workflow's async state machine to disk between suspension points. Allows truly durable "worker dies, workflow resumes elsewhere" guarantees. Very complex to implement in stable Rust.

For Phase 2, **Option A** is the right call — it matches Temporal's in-process model where the workflow coroutine lives in the sticky worker's memory. The durability guarantee comes from the event history in Postgres, not from persisting the coroutine itself. If the worker dies, a new worker replays the history to rebuild the coroutine state from scratch.

This is worth understanding before implementing the worker in Phase 2.
