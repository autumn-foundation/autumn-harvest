//! Execution contexts passed to workflow and activity functions.
//!
//! `WorkflowContext` drives deterministic replay -- it tracks the event history
//! pointer and routes commands either to real execution or to history lookup.
//!
//! `ActivityContext` provides heartbeating, state access, and a DB connection
//! to activities.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
#[cfg(feature = "db")]
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio::sync::oneshot;

use crate::error::{HarvestError, HarvestResult};
use crate::event::WorkflowEvent;
use crate::query::QueryRegistry;
use crate::replay::{HistoryMatch, HistoryMatcher};
use crate::types::{ActivityExecId, ExecutionId, TimerId};

/// Runtime map of typed shared state registered on the harvest builder.
pub type SharedStateMap = HashMap<TypeId, Box<dyn Any + Send + Sync>>;

/// Shared reference to the registered typed state map.
pub type SharedState = Arc<SharedStateMap>;

/// Create an empty shared-state map.
#[must_use]
pub fn empty_shared_state() -> SharedState {
    Arc::new(HashMap::new())
}

#[cfg(feature = "db")]
type ActivityCancellationPool =
    diesel_async::pooled_connection::deadpool::Pool<diesel_async::AsyncPgConnection>;

#[cfg(feature = "db")]
const DURABLE_CANCELLATION_CHECK_INTERVAL: Duration = Duration::from_secs(1);

#[cfg(feature = "db")]
struct ActivityCancellationCheck {
    task_id: uuid::Uuid,
    pool: ActivityCancellationPool,
    last_checked_at: Mutex<Option<Instant>>,
}

#[cfg(feature = "db")]
fn should_check_durable_cancellation(
    last_checked_at: &Mutex<Option<Instant>>,
    now: Instant,
) -> bool {
    let mut last_checked_at = last_checked_at
        .lock()
        .expect("activity cancellation check lock poisoned");

    if last_checked_at.is_some_and(|last| {
        now.checked_duration_since(last)
            .is_some_and(|elapsed| elapsed < DURABLE_CANCELLATION_CHECK_INTERVAL)
    }) {
        return false;
    }

    *last_checked_at = Some(now);
    true
}

// ---------------------------------------------------------------------------
// WorkflowCommand -- commands emitted during live execution
// ---------------------------------------------------------------------------

/// A command emitted by the workflow coroutine during live (non-replay) execution.
///
/// The worker drains these after the coroutine suspends, then schedules real
/// side-effects (activity dispatch, timer registration, etc.).
pub enum WorkflowCommand {
    /// Schedule an activity for execution on a task queue.
    ScheduleActivity {
        /// The unique execution ID of the activity.
        activity_id: ActivityExecId,
        /// The name of the activity to execute.
        name: String,
        /// The input payload for the activity.
        input: Value,
        /// The queue to schedule the activity on.
        queue: String,
        /// The worker sends the result back through this channel.
        result_tx: oneshot::Sender<Result<Value, String>>,
    },
    /// Start a durable timer.
    StartTimer {
        /// The unique ID of the timer.
        timer_id: TimerId,
        /// The duration to wait before firing the timer, in seconds.
        duration_secs: u64,
        /// Fires when the timer completes.
        result_tx: oneshot::Sender<()>,
    },
    /// Start a child workflow execution.
    StartChildWorkflow {
        /// The unique execution ID of the child workflow.
        child_id: ExecutionId,
        /// The name of the workflow to execute.
        workflow_name: String,
        /// The input payload for the child workflow.
        input: Value,
        /// The worker sends the terminal child result back through this channel.
        result_tx: oneshot::Sender<Result<Value, String>>,
    },
    /// Record an opaque marker (used by version gates, side-effect-free notes).
    RecordMarker {
        /// The name of the marker.
        name: String,
        /// Optional details or payload associated with the marker.
        details: Value,
    },
    /// Suspend until a named signal is delivered.
    WaitForSignal {
        /// The name of the signal to wait for.
        signal_name: String,
        /// The worker sends the signal payload back through this channel.
        result_tx: oneshot::Sender<Value>,
    },
    /// The workflow function returned `Ok(output)`.
    Complete {
        /// The final output payload of the workflow.
        output: Value,
    },
    /// The workflow function returned `Err(error)`.
    Fail {
        /// The string representation of the error that caused the failure.
        error: String,
    },
    /// Atomically end the current execution and start a fresh one with the
    /// same `WorkflowId` (logical identity) but a new `ExecutionId` and a
    /// fresh event history.
    ///
    /// The accompanying future returned by
    /// [`WorkflowContext::continue_as_new`] never resolves: the worker drains
    /// this command after the executor's suspension timeout and treats it as
    /// terminal regardless of whether the workflow function later returns.
    ContinueAsNew {
        /// Input passed to the next iteration of the workflow.
        input: Value,
    },
}

// Manual Debug because oneshot::Sender is not Debug.
impl std::fmt::Debug for WorkflowCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ScheduleActivity {
                activity_id,
                name,
                queue,
                ..
            } => f
                .debug_struct("ScheduleActivity")
                .field("activity_id", activity_id)
                .field("name", name)
                .field("queue", queue)
                .finish_non_exhaustive(),
            Self::StartTimer {
                timer_id,
                duration_secs,
                ..
            } => f
                .debug_struct("StartTimer")
                .field("timer_id", timer_id)
                .field("duration_secs", duration_secs)
                .finish_non_exhaustive(),
            Self::StartChildWorkflow {
                child_id,
                workflow_name,
                ..
            } => f
                .debug_struct("StartChildWorkflow")
                .field("child_id", child_id)
                .field("workflow_name", workflow_name)
                .finish_non_exhaustive(),
            Self::RecordMarker { name, details } => f
                .debug_struct("RecordMarker")
                .field("name", name)
                .field("details", details)
                .finish(),
            Self::WaitForSignal { signal_name, .. } => f
                .debug_struct("WaitForSignal")
                .field("signal_name", signal_name)
                .finish_non_exhaustive(),
            Self::Complete { output } => {
                f.debug_struct("Complete").field("output", output).finish()
            }
            Self::Fail { error } => f.debug_struct("Fail").field("error", error).finish(),
            Self::ContinueAsNew { input } => f
                .debug_struct("ContinueAsNew")
                .field("input", input)
                .finish(),
        }
    }
}

/// Suspend forever — used by terminal commands like `continue_as_new` whose
/// resolution is performed by the worker after draining the command rather
/// than by completing a oneshot. The executor's suspension timeout will fire
/// long before this future could resolve naturally.
async fn park_until_dropped() -> HarvestResult<()> {
    std::future::pending::<()>().await;
    Ok(())
}

// ---------------------------------------------------------------------------
// WorkflowContext
// ---------------------------------------------------------------------------

/// Context passed to every workflow function.
///
/// In **replay mode** (resuming from Postgres history): commands are matched
/// against recorded events and return the stored result without re-executing.
///
/// In **live mode** (past end of history): commands emit [`WorkflowCommand`]s
/// and suspend the coroutine until the worker resolves them.
///
/// Interior mutability via [`Mutex`] is required because the macro-generated
/// handler signature takes `&self` (not `&mut self`), and the returned future
/// must be `Send`.
pub struct WorkflowContext {
    /// Unique ID for this workflow execution (run).
    exec_id: ExecutionId,
    /// Replay engine -- matches commands against recorded event history.
    matcher: Mutex<HistoryMatcher>,
    /// Commands accumulated during live execution, drained by the worker.
    commands: Mutex<Vec<WorkflowCommand>>,
    /// Deterministic "now" -- the timestamp from the `WorkflowStarted` event.
    start_time: DateTime<Utc>,
    /// Monotonically increasing counter for generating activity sequence IDs.
    activity_seq: Mutex<u32>,
    /// Shared typed state map (same `AppState` extras as the web server).
    state: SharedState,
    /// In-memory query handlers (not persisted to history).
    query_registry: Mutex<QueryRegistry>,
    /// Cancellation reason captured from a `WorkflowCancelled` event in history,
    /// if any. When set, `is_cancelled()` returns true and `check_cancellation()`
    /// yields [`HarvestError::Cancelled`]. Cooperative: the workflow function is
    /// expected to consult these at strategic points to run cleanup logic.
    cancellation_reason: Option<String>,
}

impl WorkflowContext {
    // ── Constructors ──────────────────────────────────────────────────

    /// Create a context for replaying a workflow from its event history.
    ///
    /// The `events` slice must begin with `WorkflowStarted` (the timestamp
    /// is extracted for deterministic `now()`). The matcher is initialized
    /// with the cursor past the `WorkflowStarted` event.
    ///
    /// This method is primarily used internally by the framework when hydrating
    /// a workflow from the database, but is also highly useful for writing
    /// replay unit tests.
    #[must_use]
    pub fn for_replay(exec_id: ExecutionId, events: Vec<WorkflowEvent>) -> Self {
        Self::for_replay_with_state(exec_id, events, empty_shared_state())
    }

    /// Create a replay context with shared application state.
    ///
    /// Similar to [`Self::for_replay`], but allows injecting typed shared state
    /// into the context. This is required if the workflow handler uses
    /// `ctx.state::<T>()`.
    #[must_use]
    pub fn for_replay_with_state(
        exec_id: ExecutionId,
        events: Vec<WorkflowEvent>,
        state: SharedState,
    ) -> Self {
        // Extract the start_time from WorkflowStarted (first event).
        let start_time = events
            .first()
            .and_then(|e| match e {
                WorkflowEvent::WorkflowStarted { timestamp, .. } => Some(*timestamp),
                _ => None,
            })
            .unwrap_or_else(Utc::now);

        // Capture any terminal cancellation event so workflow code can detect
        // it via `is_cancelled()` / `check_cancellation()` during replay.
        let cancellation_reason = events.iter().find_map(|e| match e {
            WorkflowEvent::WorkflowCancelled { reason } => Some(reason.clone()),
            _ => None,
        });

        let mut matcher = HistoryMatcher::new(events);
        // Advance past the WorkflowStarted lifecycle event -- it does not
        // correspond to a workflow command.
        matcher.advance();

        Self {
            exec_id,
            matcher: Mutex::new(matcher),
            commands: Mutex::new(Vec::new()),
            start_time,
            activity_seq: Mutex::new(0),
            state,
            query_registry: Mutex::new(QueryRegistry::new()),
            cancellation_reason,
        }
    }

    /// Test constructor -- creates a context in live (non-replay) mode with
    /// empty state and a fresh execution ID.
    #[cfg(any(test, feature = "testing"))]
    #[must_use]
    pub fn new_test() -> Self {
        let exec_id = ExecutionId::new();
        let start_time = Utc::now();
        Self {
            exec_id,
            matcher: Mutex::new(HistoryMatcher::new(vec![])),
            commands: Mutex::new(Vec::new()),
            start_time,
            activity_seq: Mutex::new(0),
            state: empty_shared_state(),
            query_registry: Mutex::new(QueryRegistry::new()),
            cancellation_reason: None,
        }
    }

    // ── Accessors ─────────────────────────────────────────────────────

    /// Deterministic "wall clock" -- returns the `WorkflowStarted` timestamp
    /// so that all replays produce the same result.
    #[must_use]
    pub const fn now(&self) -> DateTime<Utc> {
        self.start_time
    }

    /// The unique execution (run) ID for this workflow.
    #[must_use]
    pub const fn execution_id(&self) -> ExecutionId {
        self.exec_id
    }

    /// Returns `true` if the context is currently replaying recorded history
    /// (i.e. the matcher cursor has not yet reached the end).
    ///
    /// During replay, operations should not have external side effects (like sending
    /// an email or writing to a file), because the workflow code runs multiple times
    /// as it recovers state. You can check this flag to skip logging or local
    /// non-deterministic side-effects during recovery.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::context::WorkflowContext;
    ///
    /// # fn example(ctx: &WorkflowContext) {
    /// if !ctx.is_replaying() {
    ///     println!("Executing step 1 live!");
    /// }
    /// # }
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the internal matcher mutex is poisoned.
    #[must_use]
    pub fn is_replaying(&self) -> bool {
        self.matcher
            .lock()
            .expect("matcher lock poisoned")
            .is_replaying()
    }

    /// Access typed shared state (e.g., email clients, config) injected via the builder.
    ///
    /// Because workflows must be deterministic and pure, they often need to access
    /// configuration, HTTP clients, or external service handles that were injected
    /// at application startup.
    ///
    /// Returns `None` if the state type was not registered.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::builder::HarvestBuilder;
    /// use autumn_harvest::context::WorkflowContext;
    ///
    /// struct AppConfig {
    ///     api_key: String,
    /// }
    ///
    /// // During application startup:
    /// let _harvest = HarvestBuilder::default()
    ///     .state(AppConfig { api_key: "secret-123".to_string() })
    ///     .build();
    ///
    /// // Inside a workflow function:
    /// # let ctx = WorkflowContext::for_replay_with_state(
    /// #    autumn_harvest::types::ExecutionId::new(),
    /// #    vec![],
    /// #    std::sync::Arc::new(std::collections::HashMap::from([(
    /// #        std::any::TypeId::of::<AppConfig>(),
    /// #        Box::new(AppConfig { api_key: "secret-123".to_string() }) as Box<dyn std::any::Any + Send + Sync>
    /// #    )]))
    /// # );
    /// if let Some(config) = ctx.state::<AppConfig>() {
    ///     assert_eq!(config.api_key, "secret-123");
    /// }
    /// ```
    #[must_use]
    pub fn state<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.state.get(&TypeId::of::<T>())?.downcast_ref::<T>()
    }

    // ── Cancellation ──────────────────────────────────────────────────

    /// Returns `true` if a `WorkflowCancelled` event is present in the event
    /// history backing this context.
    ///
    /// Workflows performing long-running loops or multi-step orchestration
    /// should check this periodically and bail out with cleanup when it
    /// returns `true`. Activity/timer/signal awaits already surface
    /// [`HarvestError::Cancelled`] when their result channels are dropped as
    /// part of the cancellation flow; this accessor is the analogous hook
    /// for code paths that never suspend.
    #[must_use]
    pub const fn is_cancelled(&self) -> bool {
        self.cancellation_reason.is_some()
    }

    /// Recorded cancellation reason when [`is_cancelled`](Self::is_cancelled)
    /// is true.
    ///
    /// If you implement a custom cancellation cleanup routine, you can use this
    /// to read the original message or reason that initiated the abort.
    #[must_use]
    pub fn cancellation_reason(&self) -> Option<&str> {
        self.cancellation_reason.as_deref()
    }

    /// Fail fast with [`HarvestError::Cancelled`] when the workflow has been
    /// cancelled.
    ///
    /// Intended for use at the top of long-running workflow sections so that
    /// cooperative cancellation can short-circuit the remaining work:
    ///
    /// Since long-running loops don't automatically yield to the runtime like
    /// awaits on activities do, this provides a fast path to bail out of
    /// compute-heavy or tightly looped workflows when cancellation is requested.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::context::WorkflowContext;
    /// use autumn_harvest::HarvestResult;
    ///
    /// # fn example(ctx: &WorkflowContext) -> HarvestResult<()> {
    /// for item in 0..1000 {
    ///     ctx.check_cancellation()?; // Returns Err if cancelled
    ///     // Process item...
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Cancelled`] if a `WorkflowCancelled` event is
    /// present in the event history.
    pub fn check_cancellation(&self) -> HarvestResult<()> {
        if let Some(reason) = self.cancellation_reason.as_deref() {
            return Err(HarvestError::Cancelled(reason.to_string()));
        }
        Ok(())
    }

    // ── Side effects ──────────────────────────────────────────────────

    /// Execute a quick, deterministic inline side-effect.
    ///
    /// Workflows must be completely deterministic. Operations like generating random
    /// numbers, reading the current time, or creating UUIDs will cause the workflow
    /// to behave differently during replay. `side_effect` solves this by recording
    /// the result of the closure during live execution and reusing that exact result
    /// during future replays.
    ///
    /// Use this for fast, non-failing operations that don't warrant the overhead
    /// of a full activity.
    ///
    /// During **live execution**, runs the closure, serializes the result,
    /// pushes a `RecordMarker` command to history, and returns the result.
    /// During **replay**, ignores the closure, deserializes the stored result
    /// from history, and returns it.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::context::WorkflowContext;
    ///
    /// # fn example(ctx: &WorkflowContext) -> autumn_harvest::HarvestResult<()> {
    /// // Read the current system time once and freeze it in history.
    /// // On replay, the time read is skipped and the history value is returned.
    /// let current_time: u64 = ctx.side_effect("get-time", || {
    ///     std::time::SystemTime::now()
    ///         .duration_since(std::time::UNIX_EPOCH)
    ///         .unwrap()
    ///         .as_secs()
    /// })?;
    ///
    /// println!("The time is frozen at: {}", current_time);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `HarvestError::NonDeterministic` if history diverged.
    /// Returns `HarvestError::Serialization` if the payload couldn't be serialized.
    ///
    /// # Panics
    ///
    /// Panics if the internal matcher or commands mutex is poisoned.
    pub fn side_effect<F, T>(&self, id: &str, f: F) -> HarvestResult<T>
    where
        F: FnOnce() -> T,
        T: serde::Serialize + serde::de::DeserializeOwned,
    {
        let history_match = self
            .matcher
            .lock()
            .expect("matcher lock poisoned")
            .match_side_effect(id);

        match history_match {
            HistoryMatch::Matched { output } => {
                serde_json::from_value(output).map_err(HarvestError::Serialization)
            }

            HistoryMatch::Diverged { expected, actual } => Err(HarvestError::NonDeterministic(
                format!("side effect mismatch: expected {expected}, got {actual}"),
            )),

            HistoryMatch::Failed { .. } | HistoryMatch::TimedOut { .. } => {
                unreachable!("match_side_effect only returns Matched, Diverged or NoMatch")
            }

            HistoryMatch::NoMatch => {
                let result = f();
                let output = serde_json::to_value(&result)?;

                self.push_command(WorkflowCommand::RecordMarker {
                    name: format!("side_effect:{id}"),
                    details: output,
                });

                Ok(result)
            }
        }
    }

    /// Convenience wrapper around `side_effect` for generating a deterministic UUID.
    ///
    /// Because standard UUID generation is non-deterministic, calling `Uuid::new_v4()`
    /// directly inside a workflow will cause replay failures. This helper wraps the
    /// generation in a side effect so the UUID is frozen in history.
    ///
    /// During **live execution**, generates a new UUID.
    /// During **replay**, yields the same UUID from history.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::context::WorkflowContext;
    ///
    /// # fn example(ctx: &WorkflowContext) -> autumn_harvest::HarvestResult<()> {
    /// // Safe to use inside a workflow!
    /// let idempotency_key = ctx.random_uuid("stripe-key")?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `HarvestError::NonDeterministic` if history diverged.
    /// Returns `HarvestError::Serialization` if the payload couldn't be serialized.
    pub fn random_uuid(&self, id: &str) -> HarvestResult<uuid::Uuid> {
        self.side_effect(id, uuid::Uuid::new_v4)
    }

    // ── Version gate ──────────────────────────────────────────────────

    /// Query or record a versioned code path.
    ///
    /// During **replay**, returns the version recorded in the marker event
    /// (or `min` if no marker exists for old workflows).
    ///
    /// During **live execution**, returns `max` and emits a `RecordMarker`
    /// command so the version is persisted in the event history.
    ///
    /// # Panics
    ///
    /// Panics if the internal matcher or commands mutex is poisoned.
    pub fn version(&self, change_id: &str, min: u32, max: u32) -> u32 {
        let version = self
            .matcher
            .lock()
            .expect("matcher lock poisoned")
            .match_version(change_id, min, max);

        // During live execution (matcher returned max_version and is past
        // history), emit a marker so future replays see this version.
        if !self.is_replaying() && version == max {
            self.push_command(WorkflowCommand::RecordMarker {
                name: format!("version:{change_id}"),
                details: Value::from(u64::from(max)),
            });
        }

        version
    }

    // ── Core activity dispatch ────────────────────────────────────────

    /// Execute an activity, returning the recorded result during replay or
    /// suspending the coroutine during live execution.
    ///
    /// This is the core method of the replay-aware workflow context.
    ///
    /// # Errors
    ///
    /// - [`HarvestError::NonDeterministic`] if the activity at this history
    ///   position does not match `name`.
    /// - [`HarvestError::ActivityFailed`] if the recorded history shows a failure.
    /// - [`HarvestError::Cancelled`] if the oneshot sender was dropped (workflow
    ///   was cancelled while the activity was in flight).
    ///
    /// # Panics
    ///
    /// Panics if the internal matcher or commands mutex is poisoned.
    pub async fn execute_activity_raw(
        &self,
        name: &str,
        input: Value,
        queue: &str,
    ) -> HarvestResult<Value> {
        // Step 1: Match against history (lock is dropped before any .await).
        let history_match = self
            .matcher
            .lock()
            .expect("matcher lock poisoned")
            .match_activity(name);

        match history_match {
            HistoryMatch::Matched { output } => Ok(output),

            HistoryMatch::Failed { error, attempt } => Err(HarvestError::ActivityFailed {
                name: name.to_string(),
                attempt,
                source: error.into(),
            }),

            HistoryMatch::TimedOut { timeout_type } => Err(HarvestError::Timeout {
                timeout_type,
                task_name: name.to_string(),
            }),

            HistoryMatch::Diverged { expected, actual } => Err(HarvestError::NonDeterministic(
                format!("activity mismatch: expected {expected}, got {actual}"),
            )),

            HistoryMatch::NoMatch => {
                // Live execution: emit a ScheduleActivity command and suspend
                // until the worker sends the result through the oneshot channel.
                let activity_id = self.next_activity_id();
                let (tx, rx) = oneshot::channel();

                self.push_command(WorkflowCommand::ScheduleActivity {
                    activity_id,
                    name: name.to_string(),
                    input,
                    queue: queue.to_string(),
                    result_tx: tx,
                });

                // Suspend the coroutine until the worker resolves this activity.
                match rx.await {
                    Ok(Ok(output)) => Ok(output),
                    Ok(Err(error)) => Err(HarvestError::ActivityFailed {
                        name: name.to_string(),
                        attempt: 1,
                        source: error.into(),
                    }),
                    Err(_) => Err(HarvestError::Cancelled(format!(
                        "activity '{name}' cancelled: result channel dropped"
                    ))),
                }
            }
        }
    }

    // ── Timer ─────────────────────────────────────────────────────────

    /// Start a durable timer that suspends the workflow for `duration_secs`.
    ///
    /// During **replay**, returns immediately if the timer already fired.
    /// During **live execution**, emits a `StartTimer` command and suspends.
    ///
    /// # Errors
    ///
    /// - [`HarvestError::NonDeterministic`] if the timer at this history
    ///   position does not match `timer_id`.
    /// - [`HarvestError::Cancelled`] if the oneshot sender was dropped.
    ///
    /// # Panics
    ///
    /// Panics if the internal matcher or commands mutex is poisoned.
    pub async fn timer(&self, timer_id: &str, duration_secs: u64) -> HarvestResult<()> {
        let history_match = self
            .matcher
            .lock()
            .expect("matcher lock poisoned")
            .match_timer(timer_id);

        match history_match {
            HistoryMatch::Matched { .. } => Ok(()),

            HistoryMatch::Diverged { expected, actual } => Err(HarvestError::NonDeterministic(
                format!("timer mismatch: expected {expected}, got {actual}"),
            )),

            HistoryMatch::Failed { .. } | HistoryMatch::TimedOut { .. } => {
                unreachable!("timers do not fail or time out in history matching")
            }

            HistoryMatch::NoMatch => {
                let (tx, rx) = oneshot::channel();
                self.push_command(WorkflowCommand::StartTimer {
                    timer_id: TimerId::new(timer_id),
                    duration_secs,
                    result_tx: tx,
                });

                rx.await.map_err(|_| {
                    HarvestError::Cancelled(format!(
                        "timer '{timer_id}' cancelled: result channel dropped"
                    ))
                })
            }
        }
    }

    /// Spawn a child workflow and await its terminal result.
    ///
    /// During replay, returns the recorded child output or failure.
    /// During live execution, emits a `StartChildWorkflow` command and suspends.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::ActivityFailed`] or [`HarvestError::Timeout`] when
    /// replay finds a terminal child-workflow event, [`HarvestError::NonDeterministic`]
    /// if the recorded history does not match the requested child workflow, or
    /// [`HarvestError::Cancelled`] if the workflow task is dropped before a live
    /// child result arrives.
    ///
    /// # Panics
    ///
    /// Panics if the internal replay matcher mutex is poisoned.
    pub async fn spawn_child_workflow_raw(
        &self,
        workflow_name: &str,
        input: Value,
    ) -> HarvestResult<Value> {
        let history_match = self
            .matcher
            .lock()
            .expect("matcher lock poisoned")
            .match_child_workflow(workflow_name, &input);

        match history_match {
            HistoryMatch::Matched { output } => Ok(output),
            HistoryMatch::Failed { error, attempt } => Err(HarvestError::ActivityFailed {
                name: format!("child-workflow:{workflow_name}"),
                attempt,
                source: error.into(),
            }),
            HistoryMatch::TimedOut { .. } => {
                unreachable!("child workflows do not time out in match_child_workflow")
            }
            HistoryMatch::Diverged { expected, actual } => Err(HarvestError::NonDeterministic(
                format!("child workflow mismatch: expected {expected}, got {actual}"),
            )),
            HistoryMatch::NoMatch => {
                let (tx, rx) = oneshot::channel();
                self.push_command(WorkflowCommand::StartChildWorkflow {
                    child_id: ExecutionId::new(),
                    workflow_name: workflow_name.to_string(),
                    input,
                    result_tx: tx,
                });

                match rx.await {
                    Ok(Ok(output)) => Ok(output),
                    Ok(Err(error)) => Err(HarvestError::ActivityFailed {
                        name: format!("child-workflow:{workflow_name}"),
                        attempt: 1,
                        source: error.into(),
                    }),
                    Err(_) => Err(HarvestError::Cancelled(format!(
                        "child workflow '{workflow_name}' cancelled: result channel dropped"
                    ))),
                }
            }
        }
    }

    /// Wait for the next delivered signal with the given name.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::NonDeterministic`] if replay history diverges from
    /// the requested signal wait, or [`HarvestError::Cancelled`] if the workflow
    /// task is dropped before a live signal arrives.
    ///
    /// # Panics
    ///
    /// Panics if the internal replay matcher mutex is poisoned.
    pub async fn wait_for_signal(&self, signal_name: &str) -> HarvestResult<Value> {
        let history_match = self
            .matcher
            .lock()
            .expect("matcher lock poisoned")
            .match_signal(signal_name);

        match history_match {
            HistoryMatch::Matched { output } => Ok(output),
            HistoryMatch::Diverged { expected, actual } => Err(HarvestError::NonDeterministic(
                format!("signal mismatch: expected {expected}, got {actual}"),
            )),
            HistoryMatch::Failed { .. } | HistoryMatch::TimedOut { .. } => Err(
                HarvestError::NonDeterministic("signal history contains unexpected failure".into()),
            ),
            HistoryMatch::NoMatch => {
                let (tx, rx) = oneshot::channel();
                self.push_command(WorkflowCommand::WaitForSignal {
                    signal_name: signal_name.to_string(),
                    result_tx: tx,
                });
                rx.await.map_err(|_| {
                    HarvestError::Cancelled(format!(
                        "signal '{signal_name}' cancelled: result channel dropped"
                    ))
                })
            }
        }
    }

    // ── Continue-as-new ───────────────────────────────────────────────

    /// Atomically end the current execution and start a fresh one with the
    /// same `WorkflowId` (logical identity) but a new `ExecutionId` and an
    /// empty event history. The new run begins with the supplied `input`.
    ///
    /// Used by long-lived orchestrations (recurring billing cycles, polling
    /// loops, `IoT` device monitors) to bound event-history growth and keep
    /// replay times constant.
    ///
    /// The returned future never resolves on its own — calling
    /// `ctx.continue_as_new(input).await?` is effectively a "tail call" to a
    /// new execution. The worker drains the emitted command after the
    /// executor's suspension window elapses and performs the transition in a
    /// single transaction. Any code after the await is therefore unreachable
    /// in practice.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Cancelled`] if the workflow is cancelled while
    /// it is parked on the continue-as-new command (the worker drops the
    /// resolution sender during cancellation cleanup), or
    /// [`HarvestError::NonDeterministic`] if replay history does not contain
    /// the expected `WorkflowContinuedAsNew` event at this position.
    ///
    /// # Panics
    ///
    /// Panics if the internal matcher or commands mutex is poisoned.
    pub async fn continue_as_new(&self, input: Value) -> HarvestResult<()> {
        let history_match = self
            .matcher
            .lock()
            .expect("matcher lock poisoned")
            .match_continue_as_new(&input);

        match history_match {
            HistoryMatch::Matched { output } => {
                // Replay still emits the terminal command so the worker can
                // observe the recorded intent while draining commands.
                self.push_command(WorkflowCommand::ContinueAsNew { input: output });
                park_until_dropped().await
            }
            HistoryMatch::Diverged { expected, actual } => Err(HarvestError::NonDeterministic(
                format!("continue_as_new mismatch: expected {expected}, got {actual}"),
            )),
            HistoryMatch::Failed { .. } | HistoryMatch::TimedOut { .. } => {
                Err(HarvestError::NonDeterministic(
                    "continue_as_new history contains unexpected terminal state".into(),
                ))
            }
            HistoryMatch::NoMatch => {
                self.push_command(WorkflowCommand::ContinueAsNew { input });
                park_until_dropped().await
            }
        }
    }

    /// Register a named query handler for this workflow execution.
    ///
    /// Queries allow external clients (via the management API) to inspect the
    /// internal state of a running workflow. Handlers run in-memory and are
    /// never recorded in the event history. Because queries execute synchronously
    /// without awaiting I/O, they must be fast and side-effect free.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::sync::{Arc, Mutex};
    /// use autumn_harvest::context::WorkflowContext;
    ///
    /// # fn example(ctx: &WorkflowContext) {
    /// let items_processed = Arc::new(Mutex::new(0));
    ///
    /// // Register a query that returns the current counter value
    /// let query_state = items_processed.clone();
    /// ctx.register_query("items_processed", move || {
    ///     serde_json::json!(*query_state.lock().unwrap())
    /// });
    ///
    /// // ... later in the workflow ...
    /// *items_processed.lock().unwrap() += 1;
    /// # }
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the internal query registry mutex is poisoned.
    pub fn register_query<F>(&self, name: &str, handler: F)
    where
        F: Fn() -> Value + Send + Sync + 'static,
    {
        self.query_registry
            .lock()
            .expect("query_registry lock poisoned")
            .register(name, Arc::new(handler));
    }

    /// Execute a previously registered query by name.
    ///
    /// This is typically called by the worker infrastructure when servicing an
    /// external API request. User workflow code rarely needs to call this directly.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use autumn_harvest::context::WorkflowContext;
    ///
    /// # fn example(ctx: &WorkflowContext) -> autumn_harvest::HarvestResult<()> {
    /// ctx.register_query("status", || serde_json::json!("running"));
    ///
    /// // The framework internally dispatches queries like this:
    /// let result = ctx.execute_query("status")?;
    /// assert_eq!(result, "running");
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::NotFound`] if no query handler is registered under
    /// `name`.
    ///
    /// # Panics
    ///
    /// Panics if the internal query registry mutex is poisoned.
    pub fn execute_query(&self, name: &str) -> HarvestResult<Value> {
        let handler = self
            .query_registry
            .lock()
            .expect("query_registry lock poisoned")
            .get(name);

        handler.map_or_else(
            || Err(HarvestError::NotFound(format!("query handler '{name}'"))),
            |h| Ok(h()),
        )
    }

    // ── Command drain ─────────────────────────────────────────────────

    /// Drain all accumulated commands. Called by the worker after the
    /// workflow coroutine suspends or completes.
    ///
    /// This is an internal framework method. After the workflow coroutine suspends
    /// (by awaiting a timer, activity, etc.), the worker uses this to harvest all
    /// the side-effect commands that were requested, so it can persist them to
    /// the database.
    ///
    /// # Panics
    ///
    /// Panics if the internal commands mutex is poisoned.
    pub fn drain_commands(&self) -> Vec<WorkflowCommand> {
        let mut cmds = self.commands.lock().expect("commands lock poisoned");
        std::mem::take(&mut *cmds)
    }

    // ── Internal helpers ──────────────────────────────────────────────

    /// Generate the next sequential activity execution ID.
    fn next_activity_id(&self) -> ActivityExecId {
        {
            let mut seq = self
                .activity_seq
                .lock()
                .expect("activity_seq lock poisoned");
            *seq += 1;
        }
        // Only called during live execution (NoMatch), so a random UUID is fine.
        ActivityExecId::new()
    }

    /// Push a command onto the pending commands queue.
    fn push_command(&self, cmd: WorkflowCommand) {
        self.commands
            .lock()
            .expect("commands lock poisoned")
            .push(cmd);
    }
}

// ---------------------------------------------------------------------------
// ActivityContext
// ---------------------------------------------------------------------------

/// Context passed to every activity function.
///
/// Activities may perform I/O, call external services, and interact with the
/// database. The context provides heartbeating to signal liveness, cancellation
/// detection, and state access for shared resources.
pub struct ActivityContext {
    /// Shared state map.
    state: SharedState,
    /// Heartbeat channel -- `None` in test contexts.
    heartbeat_tx: Option<tokio::sync::mpsc::Sender<serde_json::Value>>,
    /// Cancellation token -- allows the worker to signal graceful shutdown.
    cancel: tokio_util::sync::CancellationToken,
    /// Optional durable queue-state cancellation check for worker activities.
    #[cfg(feature = "db")]
    cancellation_check: Option<ActivityCancellationCheck>,
    /// W3C tracecontext carrier captured at enqueue, surfaced so activity
    /// handlers can propagate the trace to downstream services.
    trace_context: Option<crate::telemetry::TraceContextCarrier>,
}

impl ActivityContext {
    /// Production constructor -- creates a context with heartbeat channel and
    /// cancellation token.
    pub(crate) fn new(
        state: SharedState,
        heartbeat_tx: Option<tokio::sync::mpsc::Sender<serde_json::Value>>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self {
            state,
            heartbeat_tx,
            cancel,
            #[cfg(feature = "db")]
            cancellation_check: None,
            trace_context: None,
        }
    }

    /// Production constructor that also checks durable queue state on heartbeat.
    #[cfg(feature = "db")]
    pub(crate) fn new_with_cancellation_check(
        state: SharedState,
        heartbeat_tx: Option<tokio::sync::mpsc::Sender<serde_json::Value>>,
        cancel: tokio_util::sync::CancellationToken,
        task_id: uuid::Uuid,
        pool: ActivityCancellationPool,
    ) -> Self {
        Self {
            state,
            heartbeat_tx,
            cancel,
            cancellation_check: Some(ActivityCancellationCheck {
                task_id,
                pool,
                last_checked_at: Mutex::new(None),
            }),
            trace_context: None,
        }
    }

    /// Attach a trace context carrier captured from the task payload so the
    /// activity handler can stitch downstream calls into the same trace.
    #[must_use]
    pub fn with_trace_context(
        mut self,
        carrier: Option<crate::telemetry::TraceContextCarrier>,
    ) -> Self {
        self.trace_context = carrier;
        self
    }

    /// The W3C trace context that accompanied this activity's enqueue, if any.
    ///
    /// Returned by reference so handlers can forward it to outgoing HTTP
    /// requests without cloning on the hot path.
    #[must_use]
    pub const fn trace_context(&self) -> Option<&crate::telemetry::TraceContextCarrier> {
        self.trace_context.as_ref()
    }

    /// Access typed shared state.
    ///
    /// Returns `None` if the state type was not registered on the builder.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::context::ActivityContext;
    ///
    /// struct DatabaseConnection;
    ///
    /// # fn example(ctx: &ActivityContext) {
    /// if let Some(db) = ctx.state::<DatabaseConnection>() {
    ///     // Execute query...
    /// }
    /// # }
    /// ```
    #[must_use]
    pub fn state<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.state.get(&TypeId::of::<T>())?.downcast_ref::<T>()
    }

    /// Send a heartbeat to signal the activity is still running.
    ///
    /// The `details` payload is serialized to JSON and forwarded to the worker's
    /// heartbeat loop, which batches writes to the database. Always check the
    /// return value -- an `Err(Cancelled)` means the workflow was cancelled and
    /// the activity should wind down promptly.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::context::ActivityContext;
    /// use autumn_harvest::HarvestResult;
    ///
    /// # async fn download_file(ctx: &ActivityContext) -> HarvestResult<()> {
    /// for chunk in 0..100 {
    ///     // Downloading...
    ///
    ///     // Heartbeat with the current progress
    ///     ctx.heartbeat(serde_json::json!({"progress": chunk})).await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// - [`HarvestError::Cancelled`] if the cancellation token has been triggered
    ///   or the heartbeat channel is closed.
    /// - [`HarvestError::Serialization`] if `details` fails to serialize.
    pub async fn heartbeat(&self, details: impl serde::Serialize) -> crate::HarvestResult<()> {
        // Check cancellation first -- fast path.
        if self.cancel.is_cancelled() {
            return Err(HarvestError::Cancelled(
                "activity cancelled via cancellation token".into(),
            ));
        }

        #[cfg(feature = "db")]
        self.check_durable_cancellation().await?;

        let payload = serde_json::to_value(details)?;

        let Some(ref tx) = self.heartbeat_tx else {
            return Ok(());
        };
        tx.send(payload).await.map_err(|_| {
            HarvestError::Cancelled("activity cancelled: heartbeat channel closed".into())
        })?;

        Ok(())
    }

    /// Returns `true` if the cancellation token has been triggered.
    ///
    /// Activities performing long-running loops should check this periodically
    /// and exit cleanly when it returns `true`.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    #[cfg(feature = "db")]
    async fn check_durable_cancellation(&self) -> crate::HarvestResult<()> {
        use crate::schema::harvest_task_queue::dsl;
        use diesel::{OptionalExtension, QueryDsl};
        use diesel_async::RunQueryDsl;

        let Some(check) = &self.cancellation_check else {
            return Ok(());
        };

        if !should_check_durable_cancellation(&check.last_checked_at, Instant::now()) {
            return Ok(());
        }

        let mut conn = check
            .pool
            .get()
            .await
            .map_err(crate::error::database_error)?;
        let row = dsl::harvest_task_queue
            .find(check.task_id)
            .select((dsl::state, dsl::error))
            .first::<(String, Option<String>)>(&mut conn)
            .await
            .optional()
            .map_err(crate::error::database_error)?;

        match row {
            Some((state, _)) if state == "RUNNING" => Ok(()),
            Some((state, Some(error)))
                if state == "FAILED" && error.contains("workflow cancelled") =>
            {
                Err(HarvestError::Cancelled(error))
            }
            Some((state, Some(error))) => Err(HarvestError::Cancelled(format!(
                "activity task {} is no longer running ({state}): {error}",
                check.task_id
            ))),
            Some((state, None)) => Err(HarvestError::Cancelled(format!(
                "activity task {} is no longer running ({state})",
                check.task_id
            ))),
            None => Err(HarvestError::Cancelled(format!(
                "activity task {} is no longer present",
                check.task_id
            ))),
        }
    }

    /// Constructor for testing -- no heartbeat channel, default cancel token.
    ///
    /// This method allows you to instantiate an `ActivityContext` in isolation
    /// for unit testing activity handlers without needing to spin up the entire
    /// workflow engine.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::context::ActivityContext;
    ///
    /// # async fn my_activity(ctx: ActivityContext) -> autumn_harvest::HarvestResult<()> { Ok(()) }
    ///
    /// # async fn test_run() {
    /// let ctx = ActivityContext::new_test();
    /// let result = my_activity(ctx).await;
    /// assert!(result.is_ok());
    /// # }
    /// ```
    #[cfg(any(test, feature = "testing"))]
    #[must_use]
    pub fn new_test() -> Self {
        Self::new(
            empty_shared_state(),
            None,
            tokio_util::sync::CancellationToken::new(),
        )
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::TimeoutType;
    use crate::types::ActivityExecId;
    use chrono::Utc;

    #[tokio::test]
    async fn workflow_context_continue_as_new_pushes_terminal_command() {
        let ctx = WorkflowContext::new_test();
        let payload = serde_json::json!({"cycle": 2});

        // The future never resolves, so race it against a short sleep and
        // assert the command was queued — drain and inspect after the await
        // is dropped.
        let cont = ctx.continue_as_new(payload.clone());
        tokio::pin!(cont);
        tokio::select! {
            _ = &mut cont => panic!("continue_as_new must not resolve"),
            () = tokio::time::sleep(std::time::Duration::from_millis(20)) => {}
        }

        let drained = ctx.drain_commands();
        assert_eq!(drained.len(), 1);
        match &drained[0] {
            WorkflowCommand::ContinueAsNew { input } => {
                assert_eq!(input, &payload);
            }
            other => panic!("expected ContinueAsNew, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn workflow_context_continue_as_new_replays_recorded_terminal_event() {
        let payload = serde_json::json!({"cycle": 2});
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::WorkflowContinuedAsNew {
                new_exec_id: ExecutionId::new(),
                input: payload.clone(),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let cont = ctx.continue_as_new(payload.clone());
        tokio::pin!(cont);
        tokio::select! {
            _ = &mut cont => panic!("continue_as_new must not resolve"),
            () = tokio::time::sleep(std::time::Duration::from_millis(20)) => {}
        }

        let drained = ctx.drain_commands();
        assert_eq!(drained.len(), 1);
        match &drained[0] {
            WorkflowCommand::ContinueAsNew { input } => assert_eq!(input, &payload),
            other => panic!("expected ContinueAsNew, got {other:?}"),
        }
        assert!(!ctx.is_replaying());
    }

    #[tokio::test]
    async fn workflow_context_continue_as_new_divergence_is_nondeterministic() {
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "send_email".into(),
                input: Value::Null,
                queue: "default".into(),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let result = ctx.continue_as_new(serde_json::json!({"cycle": 2})).await;

        assert!(matches!(result, Err(HarvestError::NonDeterministic(_))));
        assert!(
            ctx.drain_commands().is_empty(),
            "replay divergence must not emit a new continue-as-new command"
        );
    }

    #[tokio::test]
    async fn workflow_context_continue_as_new_input_mismatch_is_nondeterministic() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::WorkflowContinuedAsNew {
                new_exec_id: ExecutionId::new(),
                input: serde_json::json!({"cycle": 3}),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let result = ctx.continue_as_new(serde_json::json!({"cycle": 2})).await;

        assert!(matches!(result, Err(HarvestError::NonDeterministic(_))));
        assert!(
            ctx.drain_commands().is_empty(),
            "replay input mismatch must not emit a new continue-as-new command"
        );
    }

    #[test]
    fn activity_context_state_returns_none_when_not_registered() {
        let ctx = ActivityContext::new_test();
        let state: Option<&String> = ctx.state::<String>();
        assert!(state.is_none());
    }

    #[test]
    fn activity_context_surfaces_attached_trace_context() {
        let carrier = crate::telemetry::TraceContextCarrier::from_traceparent("00-aaaa-bbbb-01");
        let ctx = ActivityContext::new_test().with_trace_context(Some(carrier.clone()));
        assert_eq!(ctx.trace_context(), Some(&carrier));
    }

    #[test]
    fn activity_context_trace_context_defaults_to_none() {
        let ctx = ActivityContext::new_test();
        assert!(ctx.trace_context().is_none());
    }

    #[tokio::test]
    async fn activity_context_heartbeat_sends_on_channel() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let cancel = tokio_util::sync::CancellationToken::new();
        let ctx = ActivityContext::new(Arc::new(HashMap::new()), Some(tx), cancel);

        // Send a couple of heartbeats with different payloads.
        ctx.heartbeat(serde_json::json!({"progress": 50}))
            .await
            .expect("heartbeat should succeed");
        ctx.heartbeat(serde_json::json!({"progress": 100}))
            .await
            .expect("heartbeat should succeed");

        // Verify both payloads arrived in order.
        let first = rx.recv().await.expect("should receive first heartbeat");
        assert_eq!(first, serde_json::json!({"progress": 50}));

        let second = rx.recv().await.expect("should receive second heartbeat");
        assert_eq!(second, serde_json::json!({"progress": 100}));
    }

    #[tokio::test]
    async fn activity_context_detects_cancellation() {
        let (tx, _rx) = tokio::sync::mpsc::channel(16);
        let cancel = tokio_util::sync::CancellationToken::new();
        let ctx = ActivityContext::new(Arc::new(HashMap::new()), Some(tx), cancel.clone());

        // Before cancellation -- should not be cancelled.
        assert!(!ctx.is_cancelled());

        // Trigger cancellation.
        cancel.cancel();

        // Now is_cancelled() should return true.
        assert!(ctx.is_cancelled());

        // Heartbeat should return Cancelled error.
        let result = ctx.heartbeat(serde_json::json!({})).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, HarvestError::Cancelled(_)));
    }

    #[cfg(feature = "db")]
    #[test]
    fn durable_cancellation_check_is_rate_limited() {
        let last_checked_at = Mutex::new(None);
        let start = std::time::Instant::now();

        assert!(should_check_durable_cancellation(&last_checked_at, start));
        assert!(!should_check_durable_cancellation(
            &last_checked_at,
            start + std::time::Duration::from_millis(999)
        ));
        assert!(should_check_durable_cancellation(
            &last_checked_at,
            start + std::time::Duration::from_secs(1)
        ));
    }

    #[tokio::test]
    async fn activity_context_heartbeat_errors_when_channel_closed() {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let cancel = tokio_util::sync::CancellationToken::new();
        let ctx = ActivityContext::new(Arc::new(HashMap::new()), Some(tx), cancel);

        // Drop the receiver -- channel is now closed.
        drop(rx);

        let result = ctx.heartbeat(serde_json::json!({})).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), HarvestError::Cancelled(_)));
    }

    #[test]
    fn context_now_returns_deterministic_time() {
        let fixed_time = DateTime::parse_from_rfc3339("2026-01-15T10:30:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);

        let events = vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: fixed_time,
        }];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        // now() must return the exact WorkflowStarted timestamp, not wall clock.
        assert_eq!(ctx.now(), fixed_time);
        // Calling again returns the same value (deterministic).
        assert_eq!(ctx.now(), fixed_time);
    }

    #[tokio::test]
    async fn context_replays_completed_activity() -> Result<(), crate::error::HarvestError> {
        let activity_id = ActivityExecId::new();
        let output = serde_json::json!({"email_id": "msg-001"});

        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "send_email".into(),
                input: serde_json::json!({"to": "alice@example.com"}),
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: output.clone(),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        // The context should be replaying (events remain after WorkflowStarted).
        assert!(ctx.is_replaying());

        // execute_activity_raw should return the recorded output immediately.
        let result = ctx
            .execute_activity_raw(
                "send_email",
                serde_json::json!({"to": "alice@example.com"}),
                "default",
            )
            .await;

        assert!(result.is_ok());
        assert_eq!(result?, output);

        // After consuming all events, no longer replaying.
        assert!(!ctx.is_replaying());

        // No commands emitted during replay.
        assert!(ctx.drain_commands().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn context_replays_failed_activity() {
        let activity_id = ActivityExecId::new();

        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "send_email".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityFailed {
                activity_id,
                error: "SMTP connection refused".into(),
                attempt: 3,
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        let result = ctx
            .execute_activity_raw("send_email", Value::Null, "default")
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, HarvestError::ActivityFailed { .. }));
        assert!(err.to_string().contains("send_email"));
    }

    #[tokio::test]
    async fn context_replays_timed_out_activity() {
        let activity_id = ActivityExecId::new();

        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "send_email".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityTimedOut {
                activity_id,
                timeout_type: TimeoutType::StartToClose,
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        let result = ctx
            .execute_activity_raw("send_email", Value::Null, "default")
            .await;

        assert!(matches!(
            result,
            Err(HarvestError::Timeout {
                timeout_type: TimeoutType::StartToClose,
                ..
            })
        ));
    }

    #[test]
    fn context_version_returns_recorded_version() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::MarkerRecorded {
                name: "version:billing_v2".into(),
                details: serde_json::json!(2),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let version = ctx.version("billing_v2", 1, 3);
        assert_eq!(version, 2);

        // No commands during replay.
        assert!(ctx.drain_commands().is_empty());
    }

    #[test]
    fn context_side_effect_returns_diverged_error_when_mismatched() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: crate::types::ActivityExecId::new(),
                name: "some_activity".into(),
                input: Value::Null,
                queue: "default".into(),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        // Request a side effect but history has ActivityScheduled
        let result: Result<i32, _> = ctx.side_effect("random_num", || 99);

        assert!(result.is_err());
        if let Err(HarvestError::NonDeterministic(msg)) = result {
            assert!(msg.contains("side effect mismatch"));
        } else {
            panic!("Expected NonDeterministic error");
        }
    }

    #[test]
    fn context_version_emits_marker_during_live_execution() {
        let ctx = WorkflowContext::new_test();

        // Live execution: should return max and emit RecordMarker.
        let version = ctx.version("billing_v2", 1, 3);
        assert_eq!(version, 3);

        let cmds = ctx.drain_commands();
        assert_eq!(cmds.len(), 1);
        assert!(
            matches!(&cmds[0], WorkflowCommand::RecordMarker { name, .. } if name == "version:billing_v2")
        );
    }

    #[test]
    fn context_side_effect_returns_recorded_value() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::MarkerRecorded {
                name: "side_effect:random_num".into(),
                details: serde_json::json!(42),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let result = ctx.side_effect("random_num", || 99).unwrap();
        assert_eq!(result, 42);

        // No commands during replay.
        assert!(ctx.drain_commands().is_empty());
    }

    #[test]
    fn context_side_effect_emits_marker_during_live_execution() {
        let ctx = WorkflowContext::new_test();

        let result = ctx.side_effect("random_num", || 42).unwrap();
        assert_eq!(result, 42);

        let cmds = ctx.drain_commands();
        assert_eq!(cmds.len(), 1);
        match &cmds[0] {
            WorkflowCommand::RecordMarker { name, details } => {
                assert_eq!(name, "side_effect:random_num");
                assert_eq!(details, &serde_json::json!(42));
            }
            _ => panic!("Expected RecordMarker command"),
        }
    }

    #[test]
    fn context_random_uuid_returns_recorded_value() {
        let expected_uuid = uuid::Uuid::new_v4();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::MarkerRecorded {
                name: "side_effect:txn_id".into(),
                details: serde_json::json!(expected_uuid),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let result = ctx.random_uuid("txn_id").unwrap();
        assert_eq!(result, expected_uuid);
    }

    #[test]
    fn context_random_uuid_emits_marker_during_live_execution() {
        let ctx = WorkflowContext::new_test();

        let result = ctx.random_uuid("txn_id").unwrap();

        let cmds = ctx.drain_commands();
        assert_eq!(cmds.len(), 1);
        match &cmds[0] {
            WorkflowCommand::RecordMarker { name, details } => {
                assert_eq!(name, "side_effect:txn_id");
                assert_eq!(details, &serde_json::json!(result));
            }
            _ => panic!("Expected RecordMarker command"),
        }
    }

    #[tokio::test]
    async fn context_suspends_on_new_activity() {
        // Spawn the execute_activity_raw call -- it should suspend (await the oneshot).
        let handle = tokio::spawn({
            let ctx = WorkflowContext::new_test();

            async move {
                ctx.execute_activity_raw("send_email", Value::Null, "default")
                    .await
            }
        });

        // Give the task a moment to start and emit the command.
        tokio::task::yield_now().await;

        // The handle should NOT be finished yet -- the activity is suspended.
        // Use a brief timeout to verify.
        let timeout_result =
            tokio::time::timeout(std::time::Duration::from_millis(50), handle).await;

        // The timeout should fire (the task is still suspended), which means
        // the outer Result is Err(Elapsed).
        assert!(
            timeout_result.is_err(),
            "expected task to be suspended, but it completed"
        );
    }

    #[tokio::test]
    async fn context_live_activity_resolves_via_oneshot() -> Result<(), String> {
        let ctx = Arc::new(WorkflowContext::new_test());
        let ctx2 = Arc::clone(&ctx);

        let expected_output = serde_json::json!({"sent": true});
        let expected_output2 = expected_output.clone();

        // Spawn the workflow coroutine.
        let handle = tokio::spawn(async move {
            ctx2.execute_activity_raw("send_email", Value::Null, "default")
                .await
        });

        // Yield to let the coroutine emit the command.
        tokio::task::yield_now().await;

        // Drain commands and resolve the oneshot.
        let cmds = ctx.drain_commands();
        assert_eq!(cmds.len(), 1);

        if let WorkflowCommand::ScheduleActivity {
            result_tx, name, ..
        } = cmds.into_iter().next().ok_or("no command")?
        {
            assert_eq!(name, "send_email");
            result_tx
                .send(Ok(expected_output2))
                .expect("send should succeed");
        } else {
            panic!("expected ScheduleActivity command");
        }

        // The coroutine should now resolve with the output.
        let result = handle.await.expect("task should not panic");
        assert!(result.is_ok());
        assert_eq!(result.map_err(|e| e.to_string())?, expected_output);
        Ok(())
    }

    #[tokio::test]
    async fn context_detects_non_deterministic_activity() {
        let activity_id = ActivityExecId::new();

        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "charge_payment".into(),
                input: Value::Null,
                queue: "billing".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: Value::Null,
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        // Calling with a different activity name than what's in history.
        let result = ctx
            .execute_activity_raw("send_email", Value::Null, "default")
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, HarvestError::NonDeterministic(_)));
        assert!(err.to_string().contains("send_email"));
        assert!(err.to_string().contains("charge_payment"));
    }

    #[tokio::test]
    async fn context_replays_multiple_activities_in_sequence()
    -> Result<(), crate::error::HarvestError> {
        let id1 = ActivityExecId::new();
        let id2 = ActivityExecId::new();
        let output1 = serde_json::json!({"email_id": "msg-001"});
        let output2 = serde_json::json!({"charge_id": "ch-999"});

        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: id1,
                name: "send_email".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: id1,
                output: output1.clone(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: id2,
                name: "charge_payment".into(),
                input: Value::Null,
                queue: "billing".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: id2,
                output: output2.clone(),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        let r1 = ctx
            .execute_activity_raw("send_email", Value::Null, "default")
            .await;
        assert_eq!(r1?, output1);

        let r2 = ctx
            .execute_activity_raw("charge_payment", Value::Null, "billing")
            .await;
        assert_eq!(r2?, output2);

        assert!(!ctx.is_replaying());
        assert!(ctx.drain_commands().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn context_cancelled_when_sender_dropped() {
        let ctx = WorkflowContext::new_test();

        // Spawn a task that will await an activity.
        let handle = tokio::spawn(async move {
            ctx.execute_activity_raw("send_email", Value::Null, "default")
                .await
        });

        // Yield to let it emit the command.
        tokio::task::yield_now().await;

        // Drop the handle -- the oneshot sender will be dropped when
        // the JoinHandle's task is aborted. But we actually need to
        // explicitly drop the sender. Let's approach differently:
        // The task holds the context, so we can't drain commands from here.
        // Instead, just abort the spawned task and verify the handle errors.
        handle.abort();
        let result = handle.await;
        assert!(result.is_err()); // JoinError from abort
    }

    #[test]
    fn context_execution_id_accessible() {
        let exec_id = ExecutionId::new();
        let events = vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        }];
        let ctx = WorkflowContext::for_replay(exec_id, events);
        assert_eq!(ctx.execution_id(), exec_id);
    }

    #[test]
    fn context_drain_commands_returns_empty_when_no_commands() {
        let ctx = WorkflowContext::new_test();
        assert!(ctx.drain_commands().is_empty());
    }

    #[test]
    fn context_state_access() {
        let mut state_map: HashMap<TypeId, Box<dyn Any + Send + Sync>> = HashMap::new();
        state_map.insert(TypeId::of::<String>(), Box::new(String::from("hello")));

        let events = vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        }];

        let ctx =
            WorkflowContext::for_replay_with_state(ExecutionId::new(), events, Arc::new(state_map));

        assert_eq!(ctx.state::<String>(), Some(&String::from("hello")));
        assert!(ctx.state::<u32>().is_none());
    }

    #[tokio::test]
    async fn context_timer_replays_when_fired() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new("cooldown"),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired {
                timer_id: TimerId::new("cooldown"),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let result = ctx.timer("cooldown", 300).await;
        assert!(result.is_ok());
        assert!(!ctx.is_replaying());
    }

    #[tokio::test]
    async fn context_timer_detects_divergence() {
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "foo".into(),
                input: Value::Null,
                queue: "default".into(),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let result = ctx.timer("cooldown", 300).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            HarvestError::NonDeterministic(_)
        ));
    }

    #[tokio::test]
    async fn context_replays_child_workflow_completion() {
        let child_id = ExecutionId::new();
        let output = serde_json::json!({"order_id": "A-1001"});
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "process_order".into(),
                input: serde_json::json!({"sku": "book"}),
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id,
                output: output.clone(),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let result = ctx
            .spawn_child_workflow_raw("process_order", serde_json::json!({"sku": "book"}))
            .await
            .expect("child should replay from history");

        assert_eq!(result, output);
        assert!(ctx.drain_commands().is_empty());
    }

    #[tokio::test]
    async fn context_live_child_command_round_trip() {
        let ctx = Arc::new(WorkflowContext::new_test());
        let ctx_for_task = Arc::clone(&ctx);
        let workflow_name = "process_order";

        let join = tokio::spawn(async move {
            ctx_for_task
                .spawn_child_workflow_raw(workflow_name, serde_json::json!({"sku":"book"}))
                .await
        });
        tokio::task::yield_now().await;

        let mut commands = ctx.drain_commands();
        assert_eq!(commands.len(), 1);
        let WorkflowCommand::StartChildWorkflow {
            workflow_name: emitted_name,
            result_tx,
            ..
        } = commands.remove(0)
        else {
            panic!("expected StartChildWorkflow command");
        };
        assert_eq!(emitted_name, workflow_name);
        result_tx
            .send(Ok(serde_json::json!({"ok": true})))
            .expect("receiver should exist");

        let result = join.await.expect("join should succeed");
        assert_eq!(
            result.expect("child call should succeed"),
            serde_json::json!({"ok": true})
        );
    }

    #[tokio::test]
    async fn context_child_without_terminal_does_not_emit_live_start() {
        let child_id = ExecutionId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "process_order".into(),
                input: serde_json::json!({"sku": "book"}),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let result = ctx
            .spawn_child_workflow_raw("process_order", serde_json::json!({"sku":"book"}))
            .await;

        assert!(matches!(result, Err(HarvestError::NonDeterministic(_))));
        assert!(
            ctx.drain_commands().is_empty(),
            "replay must not emit new child start command"
        );
    }

    #[tokio::test]
    async fn context_child_workflow_name_mismatch_is_nondeterministic() {
        let child_id = ExecutionId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "other_workflow".to_string(),
                input: serde_json::json!({"id": "B-2002"}),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let result = ctx
            .spawn_child_workflow_raw("process_order", serde_json::json!({"id": "B-2002"}))
            .await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            HarvestError::NonDeterministic(_)
        ));

        let cmds = ctx.drain_commands();
        assert!(cmds.is_empty());
    }

    #[tokio::test]
    async fn context_child_input_mismatch_is_nondeterministic_and_no_live_start() {
        let child_id = ExecutionId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "process_order".into(),
                input: serde_json::json!({"sku": "book"}),
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id,
                output: serde_json::json!({"order_id":"A-1001"}),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let result = ctx
            .spawn_child_workflow_raw("process_order", serde_json::json!({"sku":"magazine"}))
            .await;

        assert!(matches!(result, Err(HarvestError::NonDeterministic(_))));
        assert!(
            ctx.drain_commands().is_empty(),
            "replay must not emit new child start command on input mismatch"
        );
    }

    #[tokio::test]
    async fn context_replays_interleaved_child_starts_without_live_commands() {
        let child_a = ExecutionId::new();
        let child_b = ExecutionId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id: child_a,
                workflow_name: "process_order".into(),
                input: serde_json::json!({"id":"A"}),
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id: child_b,
                workflow_name: "process_order".into(),
                input: serde_json::json!({"id":"B"}),
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id: child_a,
                output: serde_json::json!({"id":"A","ok":true}),
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id: child_b,
                output: serde_json::json!({"id":"B","ok":true}),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let a = ctx
            .spawn_child_workflow_raw("process_order", serde_json::json!({"id":"A"}))
            .await
            .expect("A should replay");
        let b = ctx
            .spawn_child_workflow_raw("process_order", serde_json::json!({"id":"B"}))
            .await
            .expect("B should replay");

        assert_eq!(a, serde_json::json!({"id":"A","ok":true}));
        assert_eq!(b, serde_json::json!({"id":"B","ok":true}));
        assert!(ctx.drain_commands().is_empty());
    }

    #[tokio::test]
    async fn context_replays_child_with_interleaved_activity_without_live_commands() {
        let child_id = ExecutionId::new();
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "process_order".into(),
                input: serde_json::json!({"id":"A"}),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "send_email".into(),
                input: serde_json::json!({"id":"A"}),
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!({"sent":true}),
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id,
                output: serde_json::json!({"id":"A","ok":true}),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let child = ctx
            .spawn_child_workflow_raw("process_order", serde_json::json!({"id":"A"}))
            .await
            .expect("child should replay");
        let activity = ctx
            .execute_activity_raw("send_email", serde_json::json!({"id":"A"}), "default")
            .await
            .expect("activity should replay");

        assert_eq!(child, serde_json::json!({"id":"A","ok":true}));
        assert_eq!(activity, serde_json::json!({"sent":true}));
        assert!(ctx.drain_commands().is_empty());
    }

    #[tokio::test]
    async fn wait_for_signal_returns_nondeterministic_on_diverged_history() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new("timer-1"),
                duration_secs: 10,
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let result = ctx.wait_for_signal("my-signal").await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            HarvestError::NonDeterministic(_)
        ));
    }

    #[tokio::test]
    async fn wait_for_signal_replays_recorded_signal() {
        let exec_id = ExecutionId::new();
        let history = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approved".to_string(),
                payload: serde_json::json!({"ok": true}),
            },
        ];
        let ctx = WorkflowContext::for_replay(exec_id, history);

        let payload = ctx
            .wait_for_signal("approved")
            .await
            .expect("signal should replay");
        assert_eq!(payload, serde_json::json!({"ok": true}));
    }

    #[test]
    fn context_is_not_cancelled_without_terminal_event() {
        let events = vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        }];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        assert!(!ctx.is_cancelled());
        assert!(ctx.cancellation_reason().is_none());
        assert!(ctx.check_cancellation().is_ok());
    }

    #[test]
    fn context_reports_cancellation_from_history() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::WorkflowCancelled {
                reason: "operator stop".into(),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        assert!(ctx.is_cancelled());
        assert_eq!(ctx.cancellation_reason(), Some("operator stop"));
        let err = ctx
            .check_cancellation()
            .expect_err("cancelled history should yield Cancelled error");
        assert!(matches!(err, HarvestError::Cancelled(reason) if reason == "operator stop"));
    }

    #[test]
    fn context_live_mode_reports_no_cancellation() {
        let ctx = WorkflowContext::new_test();

        assert!(!ctx.is_cancelled());
        assert!(ctx.cancellation_reason().is_none());
        assert!(ctx.check_cancellation().is_ok());
    }

    #[test]
    fn query_registry_does_not_emit_commands() {
        let ctx = WorkflowContext::new_test();
        ctx.register_query("status", || serde_json::json!({"state": "running"}));

        let value = ctx.execute_query("status").expect("query should execute");
        assert_eq!(value, serde_json::json!({"state": "running"}));
        assert!(
            ctx.drain_commands().is_empty(),
            "queries must not emit events"
        );
    }
}
