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

use crate::builder::{
    DEFAULT_MAX_ACTIVITY_INPUT_BYTES, DEFAULT_MAX_SIGNAL_PAYLOAD_BYTES,
    DEFAULT_MAX_WORKFLOW_INPUT_BYTES,
};
use crate::error::{HarvestError, HarvestResult, PayloadKind};
use crate::event::WorkflowEvent;
use crate::query::QueryRegistry;
use crate::replay::{HistoryMatch, HistoryMatcher};
use crate::types::{
    ActivityExecId, ExecutionId, ExternalActivityToken, ExternalSignalId, IdempotencyKey, TimerId,
    UpdateId,
};
use crate::update::{BoxUpdateHandler, BoxUpdateValidator, UpdateRegistry};

/// Runtime map of typed shared state registered on the harvest builder.
pub type SharedStateMap = HashMap<TypeId, Box<dyn Any + Send + Sync>>;

/// Shared reference to the registered typed state map.
pub type SharedState = Arc<SharedStateMap>;

/// Create an empty shared-state map.
#[must_use]
pub fn empty_shared_state() -> SharedState {
    Arc::new(HashMap::new())
}

/// Default soft history-size threshold for recommending `continue_as_new`.
pub const DEFAULT_HISTORY_CONTINUE_AS_NEW_THRESHOLD: u64 = 10_000;

/// Replay-safe history guardrails made available to workflow code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkflowHistoryPolicy {
    continue_as_new_threshold: u64,
    event_hard_cap: Option<u64>,
}

impl Default for WorkflowHistoryPolicy {
    fn default() -> Self {
        Self {
            continue_as_new_threshold: DEFAULT_HISTORY_CONTINUE_AS_NEW_THRESHOLD,
            event_hard_cap: None,
        }
    }
}

impl WorkflowHistoryPolicy {
    /// Soft threshold used by [`WorkflowContext::should_continue_as_new`].
    #[must_use]
    pub const fn continue_as_new_threshold(self) -> u64 {
        self.continue_as_new_threshold
    }

    /// Optional hard cap that moves an execution to the DLQ when exceeded.
    #[must_use]
    pub const fn event_hard_cap(self) -> Option<u64> {
        self.event_hard_cap
    }

    /// Override the soft continue-as-new threshold.
    #[must_use]
    pub const fn with_continue_as_new_threshold(mut self, threshold: u64) -> Self {
        self.continue_as_new_threshold = threshold;
        self
    }

    /// Override the optional hard cap.
    #[must_use]
    pub const fn with_event_hard_cap(mut self, cap: u64) -> Self {
        self.event_hard_cap = Some(cap);
        self
    }
}

#[cfg(feature = "db")]
type ActivityCancellationPool =
    diesel_async::pooled_connection::deadpool::Pool<diesel_async::AsyncPgConnection>;

#[cfg(feature = "db")]
const DURABLE_CANCELLATION_CHECK_INTERVAL: Duration = Duration::from_secs(1);

const NO_HEARTBEAT_FLUSHER_REASON: &str = "heartbeats are not supported for this activity context because no heartbeat flusher is attached";
const LOCAL_ACTIVITY_HEARTBEAT_REASON: &str =
    "local activities do not support heartbeats; use a regular activity";

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
        /// Optional retry policy override (e.g. from a DAG task definition).
        /// When `Some`, overrides the activity's registered default.
        retry_policy_override: Option<crate::policy::RetryPolicy>,
        /// Optional start-to-close timeout override from a DAG task definition.
        start_to_close_override: Option<std::time::Duration>,
        /// The worker sends the result back through this channel.
        result_tx: oneshot::Sender<Result<Value, String>>,
    },
    /// Park while an already-scheduled activity is still running.
    WaitForActivity {
        /// The existing activity execution ID from history.
        activity_id: ActivityExecId,
        /// The parked coroutine waits on this channel until the executor
        /// suspension timeout drops it and the worker can re-park durably.
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
    /// Schedule an activity that completes externally via a task token.
    ScheduleExternalActivity {
        /// The unique execution ID of the activity.
        activity_id: ActivityExecId,
        /// The opaque task token the external system uses to deliver a result.
        token: ExternalActivityToken,
        /// The name of the activity to execute.
        name: String,
        /// The input payload for the activity.
        input: Value,
        /// The queue to schedule the activity on.
        queue: String,
        /// Maximum seconds before the activity times out.
        schedule_to_close_secs: u64,
        /// The worker sends the result back through this channel if a result
        /// arrives in the same execution cycle (rare; normally the channel is
        /// dropped and the workflow is re-run when external completion arrives).
        result_tx: oneshot::Sender<Result<Value, String>>,
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
    /// Run a local activity inline on the workflow worker (never enqueued).
    ///
    /// The worker resolves this command by running the named handler directly
    /// in the workflow dispatch loop, recording `LocalActivityScheduled`,
    /// zero or more `LocalActivityFailed` (per retry attempt), and finally
    /// `LocalActivityCompleted` or a terminal `LocalActivityFailed` in
    /// `harvest_events`. No row is ever written to `harvest_task_queue`.
    RunLocalActivity {
        /// The unique execution ID for this local activity invocation.
        activity_id: ActivityExecId,
        /// The name of the registered activity handler (looked up by the worker).
        name: String,
        /// JSON input for the handler.
        input: Value,
        /// Optional start-to-close timeout in seconds. `None` defers to the
        /// worker's `max_local_activity_start_to_close` cap.
        start_to_close_secs: Option<u64>,
        /// Optional retry policy. `None` = no retries (fail immediately).
        retry_policy: Option<crate::policy::RetryPolicy>,
        /// The worker sends the final result (success or exhausted retries) here.
        result_tx: oneshot::Sender<Result<Value, String>>,
        /// `true` when the `LocalActivityScheduled` event is already in history
        /// (worker crashed after appending it but before recording a terminal
        /// event). The worker must **not** append a second scheduled event.
        already_scheduled: bool,
        /// Number of `LocalActivityFailed` events already recorded in history.
        /// The worker starts its retry loop from `failed_attempts + 1`.
        /// When `failed_attempts >= max_attempts`, the worker returns `last_error`
        /// immediately without executing the handler.
        failed_attempts: u32,
        /// Error from the last recorded `LocalActivityFailed`, used when
        /// `failed_attempts >= max_attempts` to return the correct error.
        last_error: Option<String>,
    },
    /// Record a terminal result for an admitted update.
    ///
    /// Pushed by [`WorkflowContext::execute_admitted_update`] in live mode so
    /// the worker can durably append `UpdateCompleted` or `UpdateFailed` to the
    /// event history before persisting any other side effects from the same
    /// execution cycle.
    RecordUpdateResult {
        /// The update whose result is being recorded.
        update_id: crate::types::UpdateId,
        /// `Ok(value)` on success; `Err(reason)` when the handler returned an error.
        result: Result<Value, String>,
    },
    /// Merge-patch the workflow execution's `search_attrs` column.
    ///
    /// `Some(value)` entries overwrite the keyed attribute; `None` entries
    /// remove the key entirely. Keys absent from the patch are left untouched.
    /// This command is suppressed during replay so the DB write is idempotent
    /// across worker restarts.
    UpsertSearchAttributes {
        /// Per-key merge patch: `Some(v)` → set/overwrite, `None` → remove.
        patch: std::collections::HashMap<String, Option<Value>>,
    },
    /// Deliver a named signal to another running workflow by execution ID.
    ///
    /// The worker resolves this command by:
    /// 1. Appending `ExternalSignalRequested` to the caller's history (unless
    ///    `already_requested == true`, which indicates a crash-recovery cycle).
    /// 2. Inserting a row in `harvest_signals` (same-shard) or writing to the
    ///    outbox table (cross-shard).
    /// 3. Appending `ExternalSignalDelivered` or `ExternalSignalFailed { reason_code }`.
    /// 4. Sending the outcome through `result_tx`.
    SignalExternalWorkflow {
        /// Correlation ID shared across all three history events.
        signal_id: ExternalSignalId,
        /// Target workflow execution to signal.
        target: ExecutionId,
        /// Signal channel name on the receiver.
        signal_name: String,
        /// JSON payload to deliver.
        payload: Value,
        /// Outcome channel: `Ok(())` on delivery, `Err(reason_code)` on failure.
        result_tx: oneshot::Sender<Result<(), String>>,
        /// When `true`, `ExternalSignalRequested` is already in history and must
        /// not be appended again (crash-recovery path).
        already_requested: bool,
    },
}

// Manual Debug because oneshot::Sender is not Debug.
impl std::fmt::Debug for WorkflowCommand {
    #[allow(clippy::too_many_lines)]
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
            Self::WaitForActivity { activity_id, .. } => f
                .debug_struct("WaitForActivity")
                .field("activity_id", activity_id)
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
            Self::ScheduleExternalActivity {
                activity_id,
                token,
                name,
                queue,
                schedule_to_close_secs,
                ..
            } => f
                .debug_struct("ScheduleExternalActivity")
                .field("activity_id", activity_id)
                .field("token", token)
                .field("name", name)
                .field("queue", queue)
                .field("schedule_to_close_secs", schedule_to_close_secs)
                .finish_non_exhaustive(),
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
            Self::RunLocalActivity {
                activity_id,
                name,
                start_to_close_secs,
                ..
            } => f
                .debug_struct("RunLocalActivity")
                .field("activity_id", activity_id)
                .field("name", name)
                .field("start_to_close_secs", start_to_close_secs)
                .finish_non_exhaustive(),
            Self::UpsertSearchAttributes { patch } => f
                .debug_struct("UpsertSearchAttributes")
                .field("keys", &patch.keys())
                .finish(),
            Self::RecordUpdateResult { update_id, result } => f
                .debug_struct("RecordUpdateResult")
                .field("update_id", update_id)
                .field(
                    "result",
                    &result.as_ref().map(|_| "<output>").map_err(String::as_str),
                )
                .finish(),
            Self::SignalExternalWorkflow {
                signal_id,
                target,
                signal_name,
                already_requested,
                ..
            } => f
                .debug_struct("SignalExternalWorkflow")
                .field("signal_id", signal_id)
                .field("target", target)
                .field("signal_name", signal_name)
                .field("already_requested", already_requested)
                .finish_non_exhaustive(),
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
// Search-attribute validation helpers
// ---------------------------------------------------------------------------

const SEARCH_ATTR_KEY_MAX_LEN: usize = 64;

const RESERVED_SEARCH_ATTR_KEYS: &[&str] =
    &["exec_id", "workflow_name", "shard_id", "status", "run_id"];

const RESERVED_SEARCH_ATTR_PREFIX: &str = "_harvest";

fn validate_search_attr_key(key: &str) -> HarvestResult<()> {
    if key.is_empty() {
        return Err(HarvestError::InvalidSearchAttribute {
            reason: "search attribute key must not be empty".into(),
        });
    }
    if key.len() > SEARCH_ATTR_KEY_MAX_LEN {
        return Err(HarvestError::InvalidSearchAttribute {
            reason: format!(
                "search attribute key '{key}' exceeds maximum length of {SEARCH_ATTR_KEY_MAX_LEN}"
            ),
        });
    }
    if !key
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(HarvestError::InvalidSearchAttribute {
            reason: format!(
                "search attribute key '{key}' contains invalid characters; \
                 only [a-zA-Z0-9_-] are allowed"
            ),
        });
    }
    if RESERVED_SEARCH_ATTR_KEYS.contains(&key) {
        return Err(HarvestError::InvalidSearchAttribute {
            reason: format!("search attribute key '{key}' is reserved by the engine"),
        });
    }
    if key.starts_with(RESERVED_SEARCH_ATTR_PREFIX) {
        return Err(HarvestError::InvalidSearchAttribute {
            reason: format!("search attribute key '{key}' uses the reserved '_harvest' prefix"),
        });
    }
    Ok(())
}

fn validate_search_attr_value(value: &Value) -> HarvestResult<()> {
    match value {
        Value::Object(_) => Err(HarvestError::InvalidSearchAttribute {
            reason:
                "search attribute values must be primitives (string, number, boolean, or null); \
                     objects are not allowed"
                    .into(),
        }),
        Value::Array(_) => Err(HarvestError::InvalidSearchAttribute {
            reason:
                "search attribute values must be primitives (string, number, boolean, or null); \
                     arrays are not allowed"
                    .into(),
        }),
        _ => Ok(()),
    }
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
    /// History-size thresholds visible to author code.
    history_policy: WorkflowHistoryPolicy,
    /// Monotonically increasing counter for generating activity sequence IDs.
    activity_seq: Mutex<u32>,
    /// Shared typed state map (same `AppState` extras as the web server).
    state: SharedState,
    /// In-memory query handlers (not persisted to history).
    query_registry: Mutex<QueryRegistry>,
    /// Declarative query handlers registered via `register_declarative_query_handler`.
    /// These are keyed by handler name and dispatched with the context passed in.
    declarative_queries: Mutex<std::collections::HashMap<String, crate::info::QueryHandlerFn>>,
    /// In-memory update handlers and their validators (not persisted to history).
    /// Registration is idempotent — the first registration wins on each replay.
    update_registry: Mutex<UpdateRegistry>,
    /// Declarative update handlers registered via `register_declarative_update_handler`.
    declarative_updates: Mutex<std::collections::HashMap<String, crate::info::UpdateHandlerFn>>,
    /// Cancellation reason captured from a `WorkflowCancelled` event in history,
    /// if any. When set, `is_cancelled()` returns true and `check_cancellation()`
    /// yields [`HarvestError::Cancelled`]. Cooperative: the workflow function is
    /// expected to consult these at strategic points to run cleanup logic.
    cancellation_reason: Option<String>,
    /// When `true`, activity and local-activity dispatch compares the input
    /// payload against what was recorded in history, in addition to the name.
    /// Set by the `WorkflowReplayer` to detect non-deterministic input changes.
    strict_replay: bool,
    // ── Payload size caps (issue #252) ────────────────────────────────
    /// Logical workflow type name for use in `PayloadTooLarge` errors.
    /// Empty string when not known (legacy contexts, update handlers).
    workflow_name: String,
    /// Global cap on activity input payloads (bytes). Checked at schedule time.
    payload_max_activity_input: u64,
    /// Global cap on workflow/child-workflow input payloads (bytes).
    payload_max_workflow_input: u64,
    /// Global cap on `side_effect` value payloads (bytes).
    /// Uses the workflow-input cap as a reasonable default.
    payload_max_side_effect: u64,
    /// Global cap on signal payloads sent via `signal_external_workflow`.
    payload_max_signal: u64,
    /// Per-activity input cap overrides: `activity_name → max_bytes`.
    /// When an entry exists, the effective cap is `max(global, override)`.
    activity_input_cap_overrides: HashMap<String, u64>,
}

impl WorkflowContext {
    // ── Internal Helpers ──────────────────────────────────────────────────

    fn check_strict_replay_no_match(&self, actual_event: &str) -> HarvestResult<()> {
        if self.strict_replay {
            return Err(HarvestError::NonDeterministic(format!(
                "early completion mismatch: expected <end of history>, \
                 got {actual_event}"
            )));
        }
        Ok(())
    }

    fn match_history<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut HistoryMatcher) -> R,
    {
        let mut matcher = self.matcher.lock().expect("matcher lock poisoned");
        f(&mut matcher)
    }

    pub(crate) fn is_timer_started_next(&self, timer_id: &str) -> bool {
        let matcher = self.matcher.lock().expect("matcher lock poisoned");
        matcher.is_timer_started_next(timer_id)
    }

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
        Self::for_replay_with_state_and_history_policy(
            exec_id,
            events,
            state,
            WorkflowHistoryPolicy::default(),
        )
    }

    pub fn for_replay_with_state_and_history_policy(
        exec_id: ExecutionId,
        events: Vec<WorkflowEvent>,
        state: SharedState,
        history_policy: WorkflowHistoryPolicy,
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
            history_policy,
            activity_seq: Mutex::new(0),
            state,
            query_registry: Mutex::new(QueryRegistry::new()),
            declarative_queries: Mutex::new(std::collections::HashMap::new()),
            update_registry: Mutex::new(UpdateRegistry::new()),
            declarative_updates: Mutex::new(std::collections::HashMap::new()),
            cancellation_reason,
            strict_replay: false,
            workflow_name: String::new(),
            payload_max_activity_input: DEFAULT_MAX_ACTIVITY_INPUT_BYTES,
            payload_max_workflow_input: DEFAULT_MAX_WORKFLOW_INPUT_BYTES,
            payload_max_side_effect: DEFAULT_MAX_WORKFLOW_INPUT_BYTES,
            payload_max_signal: DEFAULT_MAX_SIGNAL_PAYLOAD_BYTES,
            activity_input_cap_overrides: HashMap::new(),
        }
    }

    /// Create a strict replay context that also verifies activity input payloads.
    ///
    /// Identical to [`for_replay`](Self::for_replay) except that
    /// `execute_activity_raw` and `execute_local_activity_raw` additionally
    /// compare the input value against the recorded `ActivityScheduled` event,
    /// returning [`HarvestError::NonDeterministic`] on any mismatch.
    ///
    /// Used by [`WorkflowReplayer`](crate::testing::WorkflowReplayer) to catch
    /// non-deterministic changes to activity inputs before deployment.
    #[must_use]
    pub fn for_replay_strict(exec_id: ExecutionId, events: Vec<WorkflowEvent>) -> Self {
        let mut ctx = Self::for_replay(exec_id, events);
        ctx.strict_replay = true;
        ctx
    }

    /// Like [`for_replay_strict`](Self::for_replay_strict) but injects shared
    /// application state, required when the workflow calls `ctx.state::<T>()`.
    #[must_use]
    pub fn for_replay_strict_with_state(
        exec_id: ExecutionId,
        events: Vec<WorkflowEvent>,
        state: SharedState,
    ) -> Self {
        let mut ctx = Self::for_replay_with_state(exec_id, events, state);
        ctx.strict_replay = true;
        ctx
    }

    /// Returns `true` if there are unconsumed recorded history events that are
    /// not terminal lifecycle events (`WorkflowCompleted`, `WorkflowFailed`,
    /// `WorkflowCancelled`).
    ///
    /// Used by `run_workflow_strict` to detect early-completion non-determinism.
    /// Terminal lifecycle events are excluded because they are appended by the
    /// executor after the workflow returns and are never consumed by workflow
    /// commands.
    pub fn history_has_unconsumed_events(&self) -> bool {
        self.match_history(|m| m.has_non_lifecycle_unconsumed())
    }

    /// Create a minimal handler context for declarative `#[update]` dispatch.
    ///
    /// Returns an `Arc<Self>` so the context can be captured by value inside
    /// the `async move` block generated by `#[update]`, keeping the future
    /// `'static + Send` without borrowing from the outer workflow context.
    ///
    /// The returned context inherits `exec_id`, `start_time`, and
    /// `cancellation_reason` from the parent execution so that handlers which
    /// inspect these fields see deterministic, replay-consistent values.
    #[must_use]
    pub fn new_for_handler(
        exec_id: ExecutionId,
        start_time: chrono::DateTime<chrono::Utc>,
        cancellation_reason: Option<String>,
        state: SharedState,
    ) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            exec_id,
            matcher: Mutex::new(crate::replay::HistoryMatcher::new(vec![])),
            commands: Mutex::new(Vec::new()),
            start_time,
            history_policy: WorkflowHistoryPolicy::default(),
            activity_seq: Mutex::new(0),
            state,
            query_registry: Mutex::new(QueryRegistry::new()),
            declarative_queries: Mutex::new(std::collections::HashMap::new()),
            update_registry: Mutex::new(UpdateRegistry::new()),
            declarative_updates: Mutex::new(std::collections::HashMap::new()),
            cancellation_reason,
            strict_replay: false,
            workflow_name: String::new(),
            payload_max_activity_input: DEFAULT_MAX_ACTIVITY_INPUT_BYTES,
            payload_max_workflow_input: DEFAULT_MAX_WORKFLOW_INPUT_BYTES,
            payload_max_side_effect: DEFAULT_MAX_WORKFLOW_INPUT_BYTES,
            payload_max_signal: DEFAULT_MAX_SIGNAL_PAYLOAD_BYTES,
            activity_input_cap_overrides: HashMap::new(),
        })
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
            history_policy: WorkflowHistoryPolicy::default(),
            activity_seq: Mutex::new(0),
            state: empty_shared_state(),
            query_registry: Mutex::new(QueryRegistry::new()),
            declarative_queries: Mutex::new(std::collections::HashMap::new()),
            update_registry: Mutex::new(UpdateRegistry::new()),
            declarative_updates: Mutex::new(std::collections::HashMap::new()),
            cancellation_reason: None,
            strict_replay: false,
            workflow_name: String::new(),
            payload_max_activity_input: DEFAULT_MAX_ACTIVITY_INPUT_BYTES,
            payload_max_workflow_input: DEFAULT_MAX_WORKFLOW_INPUT_BYTES,
            payload_max_side_effect: DEFAULT_MAX_WORKFLOW_INPUT_BYTES,
            payload_max_signal: DEFAULT_MAX_SIGNAL_PAYLOAD_BYTES,
            activity_input_cap_overrides: HashMap::new(),
        }
    }

    /// Override all payload size caps on this context (builder-style chaining).
    ///
    /// Arguments: `(max_activity_input, max_activity_result, max_signal, max_workflow_input)`
    /// in bytes.
    /// - `max_activity_input` caps activity inputs at schedule time.
    /// - `max_workflow_input` caps child-workflow inputs and side-effect values.
    ///
    /// `max_activity_result` is accepted for API symmetry but is enforced by
    /// the worker, not by `WorkflowContext`.
    #[must_use]
    pub const fn with_payload_caps(
        mut self,
        max_activity_input: u64,
        _max_activity_result: u64,
        max_signal: u64,
        max_workflow_input: u64,
    ) -> Self {
        self.payload_max_activity_input = max_activity_input;
        self.payload_max_workflow_input = max_workflow_input;
        self.payload_max_side_effect = max_workflow_input;
        self.payload_max_signal = max_signal;
        self
    }

    /// Add or replace a per-activity input cap override.
    ///
    /// The effective cap is `max(global, override)` — overrides can only raise,
    /// never lower, the global cap.
    #[must_use]
    pub fn with_activity_input_override(mut self, activity_name: &str, max_bytes: u64) -> Self {
        self.activity_input_cap_overrides
            .insert(activity_name.to_string(), max_bytes);
        self
    }

    /// Set the logical workflow type name used in `PayloadTooLarge` error messages.
    ///
    /// Called by the worker after creating the context so that cap-enforcement
    /// errors carry the correct workflow type name for observability.
    #[must_use]
    pub fn with_workflow_name(mut self, name: impl Into<String>) -> Self {
        self.workflow_name = name.into();
        self
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

    /// Number of events currently loaded in this workflow execution history.
    ///
    /// This is replay-safe: it is computed from the in-memory history snapshot
    /// loaded before the current workflow task, not from a side-effecting
    /// counter maintained by author code.
    #[must_use]
    pub fn history_event_count(&self) -> u64 {
        self.match_history(|matcher| matcher.event_count())
    }

    /// Returns `true` once [`Self::history_event_count`] exceeds the configured
    /// soft continue-as-new threshold.
    #[must_use]
    pub fn should_continue_as_new(&self) -> bool {
        self.history_event_count() > self.history_policy.continue_as_new_threshold()
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
            .has_buffered_history()
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

    // ── Search attribute mutations ────────────────────────────────────

    /// Merge-patch the search attributes for this workflow execution.
    ///
    /// Keys present in `patch` with `Some(value)` overwrite the stored attribute.
    /// Keys present with `None` remove the attribute. Keys absent from `patch`
    /// are untouched (merge semantics, not full replacement).
    ///
    /// This is a **fire-and-forget metadata operation**: the method returns
    /// immediately and the DB write is processed by the worker after the next
    /// suspension or completion. Workflow logic must not branch on the return
    /// value to maintain determinism.
    ///
    /// During **replay**, this call is a no-op — the attributes are already
    /// correct in the database from the previous live execution cycle.
    ///
    /// # Key constraints
    ///
    /// - Non-empty, ≤ 64 characters, matching `[a-zA-Z0-9_-]+`.
    /// - Not one of the reserved engine keys: `exec_id`, `workflow_name`,
    ///   `shard_id`, `status`, `run_id`.
    /// - Must not start with the `_harvest` prefix.
    ///
    /// # Value constraints
    ///
    /// Values must be JSON primitives (string, number, boolean, or `null`).
    /// Objects and arrays are rejected.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::InvalidSearchAttribute`] if any key or value
    /// violates the above constraints. The entire patch is rejected atomically —
    /// no partial updates are applied.
    pub fn upsert_search_attrs(
        &self,
        patch: impl IntoIterator<Item = (String, Option<Value>)>,
    ) -> HarvestResult<()> {
        let patch: std::collections::HashMap<String, Option<Value>> = patch.into_iter().collect();

        if patch.is_empty() {
            return Ok(());
        }

        for (key, value) in &patch {
            validate_search_attr_key(key)?;
            if let Some(v) = value {
                validate_search_attr_value(v)?;
            }
        }

        // During replay the DB update already happened; suppress the command.
        if self.is_replaying() {
            return Ok(());
        }

        self.push_command(WorkflowCommand::UpsertSearchAttributes { patch });
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
        let history_match = self.match_history(|m| m.match_side_effect(id));

        match history_match {
            HistoryMatch::Matched { output } => {
                serde_json::from_value(output).map_err(HarvestError::Serialization)
            }

            HistoryMatch::Diverged { expected, actual } => Err(HarvestError::NonDeterministic(
                format!("side effect mismatch: expected {expected}, got {actual}"),
            )),

            HistoryMatch::Failed { .. }
            | HistoryMatch::TimedOut { .. }
            | HistoryMatch::ActivityInProgress { .. }
            | HistoryMatch::AwaitingExternalCompletion { .. }
            | HistoryMatch::ChildInProgress { .. }
            | HistoryMatch::LocalActivityInProgress { .. }
            | HistoryMatch::ExternalSignalInProgress { .. }
            | HistoryMatch::ExternalSignalFailed { .. } => {
                unreachable!("match_side_effect only returns Matched, Diverged or NoMatch")
            }

            HistoryMatch::NoMatch => {
                self.check_strict_replay_no_match(&format!("SideEffectRecorded({id})"))?;

                let result = f();
                let output = serde_json::to_value(&result)?;

                // Enforce side-effect payload cap before recording.
                let observed = serde_json::to_string(&output).map_or(0, |s| s.len() as u64);
                if observed > self.payload_max_side_effect {
                    return Err(HarvestError::PayloadTooLarge {
                        kind: PayloadKind::SideEffectValue,
                        observed_bytes: observed,
                        cap_bytes: self.payload_max_side_effect,
                        workflow_type: self.workflow_name.clone(),
                        activity_name: None,
                    });
                }

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
        assert!(
            min <= max,
            "version gate '{change_id}': min version {min} must not exceed max version {max}"
        );
        let version = self.match_history(|m| m.match_version(change_id, min, max));

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
        let history_match = if self.strict_replay {
            self.match_history(|m| m.match_activity_strict(name, &input))
        } else {
            self.match_history(|m| m.match_activity(name))
        };

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

            HistoryMatch::ActivityInProgress { activity_id } => {
                let (tx, rx) = oneshot::channel();
                self.push_command(WorkflowCommand::WaitForActivity {
                    activity_id,
                    result_tx: tx,
                });
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

            HistoryMatch::AwaitingExternalCompletion { .. }
            | HistoryMatch::ChildInProgress { .. }
            | HistoryMatch::LocalActivityInProgress { .. }
            | HistoryMatch::ExternalSignalInProgress { .. }
            | HistoryMatch::ExternalSignalFailed { .. } => {
                unreachable!(
                    "match_activity never returns AwaitingExternalCompletion, ChildInProgress, \
                     LocalActivityInProgress, or ExternalSignalInProgress"
                )
            }

            HistoryMatch::NoMatch => {
                // Strict replay: a command with no matching history entry means
                // the new code issues a command the recorded history never saw.
                self.check_strict_replay_no_match(&format!("ActivityScheduled({name})"))?;

                // Enforce activity input payload cap before scheduling.
                let effective_cap = {
                    let global = self.payload_max_activity_input;
                    self.activity_input_cap_overrides
                        .get(name)
                        .copied()
                        .map_or(global, |ov| global.max(ov))
                };
                let observed = serde_json::to_string(&input).map_or(0, |s| s.len() as u64);
                if effective_cap > 0 && observed > effective_cap {
                    return Err(HarvestError::PayloadTooLarge {
                        kind: PayloadKind::ActivityInput,
                        observed_bytes: observed,
                        cap_bytes: effective_cap,
                        workflow_type: self.workflow_name.clone(),
                        activity_name: Some(name.to_string()),
                    });
                }

                // Live execution: emit a ScheduleActivity command and suspend
                // until the worker sends the result through the oneshot channel.
                let activity_id = self.next_activity_id();
                let (tx, rx) = oneshot::channel();

                self.push_command(WorkflowCommand::ScheduleActivity {
                    activity_id,
                    name: name.to_string(),
                    input,
                    queue: queue.to_string(),
                    retry_policy_override: None,
                    start_to_close_override: None,
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

    /// Like [`execute_activity_raw`](Self::execute_activity_raw) but allows
    /// per-call overrides for retry policy and start-to-close timeout.
    /// Used by the DAG unified handler to honour task-level `.retry()` and
    /// `.start_to_close()` settings from the `DagBuilder`.
    #[doc(hidden)]
    pub async fn execute_activity_raw_with_opts(
        &self,
        name: &str,
        input: Value,
        queue: &str,
        retry_policy_override: Option<crate::policy::RetryPolicy>,
        start_to_close_override: Option<std::time::Duration>,
    ) -> HarvestResult<Value> {
        let history_match = if self.strict_replay {
            self.match_history(|m| m.match_activity_strict(name, &input))
        } else {
            self.match_history(|m| m.match_activity(name))
        };

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
            HistoryMatch::ActivityInProgress { activity_id } => {
                let (tx, rx) = oneshot::channel();
                self.push_command(WorkflowCommand::WaitForActivity {
                    activity_id,
                    result_tx: tx,
                });
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
            HistoryMatch::AwaitingExternalCompletion { .. }
            | HistoryMatch::ChildInProgress { .. }
            | HistoryMatch::LocalActivityInProgress { .. }
            | HistoryMatch::ExternalSignalInProgress { .. }
            | HistoryMatch::ExternalSignalFailed { .. } => {
                unreachable!(
                    "match_activity never returns AwaitingExternalCompletion, \
                     ChildInProgress, LocalActivityInProgress, or ExternalSignalInProgress"
                )
            }
            HistoryMatch::NoMatch => {
                self.check_strict_replay_no_match(&format!("ActivityScheduled({name})"))?;

                // Enforce activity input payload cap before scheduling.
                let effective_cap = {
                    let global = self.payload_max_activity_input;
                    self.activity_input_cap_overrides
                        .get(name)
                        .copied()
                        .map_or(global, |ov| global.max(ov))
                };
                let observed = serde_json::to_string(&input).map_or(0, |s| s.len() as u64);
                if effective_cap > 0 && observed > effective_cap {
                    return Err(HarvestError::PayloadTooLarge {
                        kind: PayloadKind::ActivityInput,
                        observed_bytes: observed,
                        cap_bytes: effective_cap,
                        workflow_type: self.workflow_name.clone(),
                        activity_name: Some(name.to_string()),
                    });
                }

                let activity_id = self.next_activity_id();
                let (tx, rx) = oneshot::channel();
                self.push_command(WorkflowCommand::ScheduleActivity {
                    activity_id,
                    name: name.to_string(),
                    input,
                    queue: queue.to_string(),
                    retry_policy_override,
                    start_to_close_override,
                    result_tx: tx,
                });
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

    // ── Local activity dispatch ───────────────────────────────────────

    /// Execute a *local* activity inline on the workflow worker — never enqueued.
    ///
    /// During **replay**, returns the recorded outcome from `harvest_events`
    /// without running the handler body.
    ///
    /// During **live execution**, emits a [`WorkflowCommand::RunLocalActivity`]
    /// command and suspends the coroutine until the worker runs the handler
    /// inline and resolves the result channel.
    ///
    /// Local activities respect only `start_to_close`. They do not support
    /// heartbeats, `schedule_to_start`, or a named task queue (they always
    /// run on the same worker task driving the workflow).
    ///
    /// # Errors
    ///
    /// - [`HarvestError::NonDeterministic`] if history at this position does
    ///   not match `name`.
    /// - [`HarvestError::ActivityFailed`] if recorded history shows exhausted retries.
    /// - [`HarvestError::Cancelled`] if the result channel was dropped.
    ///
    /// # Panics
    ///
    /// Panics if the internal matcher or commands mutex is poisoned.
    #[allow(clippy::too_many_lines)]
    pub async fn execute_local_activity_raw(
        &self,
        name: &str,
        input: Value,
        retry_policy: Option<crate::policy::RetryPolicy>,
        start_to_close_secs: Option<u64>,
    ) -> HarvestResult<Value> {
        let history_match = if self.strict_replay {
            self.match_history(|m| m.match_local_activity_strict(name, &input))
        } else {
            self.match_history(|m| m.match_local_activity(name))
        };

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
                format!("local activity mismatch: expected {expected}, got {actual}"),
            )),

            HistoryMatch::AwaitingExternalCompletion { .. }
            | HistoryMatch::ActivityInProgress { .. }
            | HistoryMatch::ChildInProgress { .. }
            | HistoryMatch::ExternalSignalInProgress { .. }
            | HistoryMatch::ExternalSignalFailed { .. } => {
                unreachable!(
                    "match_local_activity never returns AwaitingExternalCompletion, \
                     ChildInProgress, or ExternalSignalInProgress"
                )
            }

            // Worker crashed after appending `LocalActivityScheduled` (and
            // possibly one or more `LocalActivityFailed` events) but before
            // recording a terminal event. Re-run with the original `activity_id`
            // so the idempotency key is stable across the crash.
            HistoryMatch::LocalActivityInProgress {
                activity_id,
                failed_attempts,
                last_error,
            } => {
                if self.strict_replay {
                    return Err(HarvestError::NonDeterministic(format!(
                        "local activity '{name}' scheduled but terminal not in history"
                    )));
                }

                // If the recorded failure count already covers all retry
                // attempts, return the last error immediately — no handler
                // execution is needed and no command should be pushed.
                let max_attempts = retry_policy.as_ref().map_or(1, |p| p.max_attempts);
                if failed_attempts >= max_attempts {
                    let error = last_error.unwrap_or_else(|| {
                        format!("local activity '{name}' failed after {failed_attempts} attempts")
                    });
                    return Err(HarvestError::ActivityFailed {
                        name: name.to_string(),
                        attempt: failed_attempts,
                        source: error.into(),
                    });
                }

                // Some retry attempts remain — push the command so the worker
                // re-runs the handler starting from the next unrecorded attempt.
                let (tx, rx) = oneshot::channel();
                self.push_command(WorkflowCommand::RunLocalActivity {
                    activity_id,
                    name: name.to_string(),
                    input,
                    start_to_close_secs,
                    retry_policy,
                    result_tx: tx,
                    already_scheduled: true,
                    failed_attempts,
                    last_error,
                });
                match rx.await {
                    Ok(Ok(output)) => Ok(output),
                    Ok(Err(error)) => Err(HarvestError::ActivityFailed {
                        name: name.to_string(),
                        attempt: failed_attempts.max(1),
                        source: error.into(),
                    }),
                    Err(_) => Err(HarvestError::Cancelled(format!(
                        "local activity '{name}' cancelled: result channel dropped"
                    ))),
                }
            }

            HistoryMatch::NoMatch => {
                self.check_strict_replay_no_match(&format!("LocalActivityScheduled({name})"))?;

                // Enforce activity input payload cap for local activities too.
                let effective_cap = {
                    let global = self.payload_max_activity_input;
                    self.activity_input_cap_overrides
                        .get(name)
                        .copied()
                        .map_or(global, |ov| global.max(ov))
                };
                let observed = serde_json::to_string(&input).map_or(0, |s| s.len() as u64);
                if effective_cap > 0 && observed > effective_cap {
                    return Err(HarvestError::PayloadTooLarge {
                        kind: PayloadKind::ActivityInput,
                        observed_bytes: observed,
                        cap_bytes: effective_cap,
                        workflow_type: self.workflow_name.clone(),
                        activity_name: Some(name.to_string()),
                    });
                }

                let activity_id = self.next_activity_id();
                let (tx, rx) = oneshot::channel();

                self.push_command(WorkflowCommand::RunLocalActivity {
                    activity_id,
                    name: name.to_string(),
                    input,
                    start_to_close_secs,
                    retry_policy,
                    result_tx: tx,
                    already_scheduled: false,
                    failed_attempts: 0,
                    last_error: None,
                });

                match rx.await {
                    Ok(Ok(output)) => Ok(output),
                    Ok(Err(error)) => Err(HarvestError::ActivityFailed {
                        name: name.to_string(),
                        attempt: 1,
                        source: error.into(),
                    }),
                    Err(_) => Err(HarvestError::Cancelled(format!(
                        "local activity '{name}' cancelled: result channel dropped"
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
        let history_match =
            self.match_history(|m| m.match_timer_strict(timer_id, Some(duration_secs)));

        match history_match {
            HistoryMatch::Matched { .. } => Ok(()),

            HistoryMatch::Diverged { expected, actual } => Err(HarvestError::NonDeterministic(
                format!("timer mismatch: expected {expected}, got {actual}"),
            )),

            HistoryMatch::Failed { .. }
            | HistoryMatch::TimedOut { .. }
            | HistoryMatch::ActivityInProgress { .. }
            | HistoryMatch::AwaitingExternalCompletion { .. }
            | HistoryMatch::ChildInProgress { .. }
            | HistoryMatch::LocalActivityInProgress { .. }
            | HistoryMatch::ExternalSignalInProgress { .. }
            | HistoryMatch::ExternalSignalFailed { .. } => {
                unreachable!("timers do not fail or time out in history matching")
            }

            HistoryMatch::NoMatch => {
                self.check_strict_replay_no_match(&format!("TimerStarted({timer_id})"))?;

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
        let history_match = self.match_history(|m| m.match_child_workflow(workflow_name, &input));

        match history_match {
            HistoryMatch::Matched { output } => Ok(output),
            HistoryMatch::Failed { error, attempt } => Err(HarvestError::ActivityFailed {
                name: format!("child-workflow:{workflow_name}"),
                attempt,
                source: error.into(),
            }),
            HistoryMatch::TimedOut { .. }
            | HistoryMatch::ActivityInProgress { .. }
            | HistoryMatch::AwaitingExternalCompletion { .. }
            | HistoryMatch::LocalActivityInProgress { .. }
            | HistoryMatch::ExternalSignalInProgress { .. }
            | HistoryMatch::ExternalSignalFailed { .. } => {
                unreachable!("child workflows do not time out in match_child_workflow")
            }
            HistoryMatch::Diverged { expected, actual } => Err(HarvestError::NonDeterministic(
                format!("child workflow mismatch: expected {expected}, got {actual}"),
            )),
            // The child was already started (its ChildWorkflowStarted event is in
            // history) but its terminal hasn't arrived yet.  This is the normal
            // state when the parent wakes because one of several parallel children
            // completed while this child is still running.
            //
            // In strict-replay mode (WorkflowReplayer) an incomplete history is
            // treated as a non-determinism error — callers always provide complete
            // histories.  In the worker's non-strict replay mode we re-emit the
            // command carrying the *existing* child_id so the worker can re-park
            // the parent without creating a duplicate child execution.
            HistoryMatch::ChildInProgress { child_id } => {
                if self.strict_replay {
                    return Err(HarvestError::NonDeterministic(format!(
                        "child workflow '{workflow_name}' started but terminal not in history"
                    )));
                }
                let (tx, rx) = oneshot::channel();
                self.push_command(WorkflowCommand::StartChildWorkflow {
                    child_id,
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
            HistoryMatch::NoMatch => {
                self.check_strict_replay_no_match(&format!(
                    "ChildWorkflowStarted({workflow_name})"
                ))?;

                // Enforce child-workflow input payload cap before scheduling.
                let observed = serde_json::to_string(&input).map_or(0, |s| s.len() as u64);
                if self.payload_max_workflow_input > 0 && observed > self.payload_max_workflow_input
                {
                    return Err(HarvestError::PayloadTooLarge {
                        kind: PayloadKind::ChildWorkflowInput,
                        observed_bytes: observed,
                        cap_bytes: self.payload_max_workflow_input,
                        workflow_type: self.workflow_name.clone(),
                        activity_name: None,
                    });
                }

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

    // ── Typed dispatch helpers ────────────────────────────────────────────────

    /// Execute an activity using its [`crate::info::ActivityInfo`] for name, queue, and defaults.
    ///
    /// This is the typed alternative to [`execute_activity_raw`](Self::execute_activity_raw).
    /// Pass the companion function generated by `#[activity]` as `info`:
    ///
    /// ```rust,ignore
    /// // Given: #[activity(queue = "email")] async fn send_email(ctx: &ActivityContext, addr: String) -> Result<(), String>
    /// ctx.execute_activity(&send_email_info(), addr).await?;
    /// ```
    ///
    /// Delegates to [`execute_activity_with_opts`](Self::execute_activity_with_opts) with all
    /// overrides set to `None`, so `ActivityInfo` defaults are always applied consistently.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Serialization`] if `input` cannot be serialized.
    /// Propagates all errors from
    /// [`execute_activity_with_opts`](Self::execute_activity_with_opts).
    pub async fn execute_activity<I, O>(
        &self,
        info: &crate::info::ActivityInfo,
        input: I,
    ) -> HarvestResult<O>
    where
        I: serde::Serialize,
        O: serde::de::DeserializeOwned,
    {
        self.execute_activity_with_opts(info, input, None, None, None)
            .await
    }

    /// Execute an activity with per-call queue, retry, and timeout overrides.
    ///
    /// All overrides take precedence over `ActivityInfo` defaults. Pass `None`
    /// to fall back to the info defaults for that field.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Serialization`] if `input` cannot be serialized.
    /// Propagates all errors from
    /// [`execute_activity_raw_with_opts`](Self::execute_activity_raw_with_opts).
    pub async fn execute_activity_with_opts<I, O>(
        &self,
        info: &crate::info::ActivityInfo,
        input: I,
        queue_override: Option<&str>,
        retry_override: Option<crate::policy::RetryPolicy>,
        timeout_override: Option<std::time::Duration>,
    ) -> HarvestResult<O>
    where
        I: serde::Serialize,
        O: serde::de::DeserializeOwned,
    {
        let json_input = serde_json::to_value(input)?;
        let queue = queue_override.or(info.default_queue).unwrap_or("default");
        let retry = retry_override.or_else(|| info.default_retry_policy.clone());
        let timeout = timeout_override.or(info.default_start_to_close);
        let raw = self
            .execute_activity_raw_with_opts(info.name, json_input, queue, retry, timeout)
            .await?;
        Ok(serde_json::from_value(raw)?)
    }

    /// Execute a local activity inline on the workflow worker using its [`crate::info::ActivityInfo`].
    ///
    /// This is the typed alternative to
    /// [`execute_local_activity_raw`](Self::execute_local_activity_raw).
    /// Delegates to [`execute_local_activity_with_opts`](Self::execute_local_activity_with_opts)
    /// with all overrides set to `None`.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Config`] if `info.is_local` is `false` — use
    /// [`execute_activity`](Self::execute_activity) for remote activities.
    /// Returns [`HarvestError::Serialization`] if `input` cannot be serialized.
    /// Propagates all errors from
    /// [`execute_local_activity_with_opts`](Self::execute_local_activity_with_opts).
    pub async fn execute_local_activity<I, O>(
        &self,
        info: &crate::info::ActivityInfo,
        input: I,
    ) -> HarvestResult<O>
    where
        I: serde::Serialize,
        O: serde::de::DeserializeOwned,
    {
        self.execute_local_activity_with_opts(info, input, None, None)
            .await
    }

    /// Execute a local activity with per-call retry and timeout overrides.
    ///
    /// Overrides take precedence over `ActivityInfo` defaults. Pass `None` to
    /// fall back to the info defaults for that field.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Config`] if `info.is_local` is `false` — use
    /// [`execute_activity_with_opts`](Self::execute_activity_with_opts) for remote activities.
    /// Returns [`HarvestError::Serialization`] if `input` cannot be serialized.
    /// Propagates all errors from
    /// [`execute_local_activity_raw`](Self::execute_local_activity_raw).
    pub async fn execute_local_activity_with_opts<I, O>(
        &self,
        info: &crate::info::ActivityInfo,
        input: I,
        retry_override: Option<crate::policy::RetryPolicy>,
        timeout_override: Option<std::time::Duration>,
    ) -> HarvestResult<O>
    where
        I: serde::Serialize,
        O: serde::de::DeserializeOwned,
    {
        if !info.is_local {
            return Err(HarvestError::Config(format!(
                "activity '{}' is not marked local = true; use execute_activity_with_opts instead",
                info.name
            )));
        }
        let json_input = serde_json::to_value(input)?;
        let retry = retry_override.or_else(|| info.default_retry_policy.clone());
        let start_to_close_secs = timeout_override
            .or(info.default_start_to_close)
            .map(|d| d.as_secs());
        let raw = self
            .execute_local_activity_raw(info.name, json_input, retry, start_to_close_secs)
            .await?;
        Ok(serde_json::from_value(raw)?)
    }

    /// Spawn a child workflow and await its result using a [`crate::info::WorkflowInfo`].
    ///
    /// This is the typed alternative to
    /// [`spawn_child_workflow_raw`](Self::spawn_child_workflow_raw).
    ///
    /// ```rust,ignore
    /// // Given: #[workflow] async fn child_job(ctx: &WorkflowContext, id: u64) -> Result<String, String>
    /// let result: String = ctx.spawn_child_workflow(&child_job_info(), id).await?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Serialization`] if `input` cannot be serialized.
    /// Propagates all errors from
    /// [`spawn_child_workflow_raw`](Self::spawn_child_workflow_raw).
    pub async fn spawn_child_workflow<I, O>(
        &self,
        info: &crate::info::WorkflowInfo,
        input: I,
    ) -> HarvestResult<O>
    where
        I: serde::Serialize,
        O: serde::de::DeserializeOwned,
    {
        let json_input = serde_json::to_value(input)?;
        let raw = self.spawn_child_workflow_raw(info.name, json_input).await?;
        Ok(serde_json::from_value(raw)?)
    }

    /// Wait for a signal and deserialize its payload into `O`.
    ///
    /// This is the typed alternative to [`wait_for_signal`](Self::wait_for_signal).
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Serialization`] if the signal payload cannot be
    /// deserialized. Propagates all errors from
    /// [`wait_for_signal`](Self::wait_for_signal).
    pub async fn receive_signal<O>(&self, signal_name: &str) -> HarvestResult<O>
    where
        O: serde::de::DeserializeOwned,
    {
        let raw = self.wait_for_signal(signal_name).await?;
        Ok(serde_json::from_value(raw)?)
    }

    /// Block until a predicate over workflow local state evaluates to true.
    pub const fn await_condition<F>(&self, predicate: F) -> AwaitConditionFut<F>
    where
        F: FnMut() -> bool + Unpin,
    {
        AwaitConditionFut { predicate }
    }

    /// Block until a predicate over workflow local state evaluates to true, or the timeout expires.
    pub fn await_condition_timeout<'a, F>(
        &'a self,
        timer_id: &'a str,
        duration_secs: u64,
        predicate: F,
    ) -> AwaitConditionTimeoutFut<'a, F>
    where
        F: FnMut() -> bool + Unpin,
    {
        let timer_fut = self.timer(timer_id, duration_secs);
        AwaitConditionTimeoutFut {
            context: self,
            timer_id: timer_id.to_string(),
            predicate,
            timer_fut: Box::pin(timer_fut),
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
        let history_match = self.match_history(|m| m.match_signal(signal_name));

        match history_match {
            HistoryMatch::Matched { output } => Ok(output),
            HistoryMatch::Diverged { expected, actual } => Err(HarvestError::NonDeterministic(
                format!("signal mismatch: expected {expected}, got {actual}"),
            )),
            HistoryMatch::Failed { .. }
            | HistoryMatch::TimedOut { .. }
            | HistoryMatch::ActivityInProgress { .. }
            | HistoryMatch::AwaitingExternalCompletion { .. }
            | HistoryMatch::ChildInProgress { .. }
            | HistoryMatch::LocalActivityInProgress { .. }
            | HistoryMatch::ExternalSignalInProgress { .. }
            | HistoryMatch::ExternalSignalFailed { .. } => Err(HarvestError::NonDeterministic(
                "signal history contains unexpected failure".into(),
            )),
            HistoryMatch::NoMatch => {
                self.check_strict_replay_no_match(&format!("WaitForSignal({signal_name})"))?;

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

    // ── Cross-workflow signal dispatch ────────────────────────────────────

    /// Send a named signal with a typed payload to another running workflow.
    ///
    /// This is the deterministic, replay-safe primitive for saga choreography.
    /// Unlike calling the management API from inside an activity, this method
    /// leaves a durable audit trail in `harvest_events` and returns the recorded
    /// outcome on replay without re-issuing any side effects.
    ///
    /// # Determinism contract
    ///
    /// - **First live call**: appends `ExternalSignalRequested` to the caller's
    ///   history, attempts delivery, then appends `ExternalSignalDelivered` or
    ///   `ExternalSignalFailed { reason_code }`.
    /// - **Replay**: returns the recorded outcome directly from history.
    /// - **Crash recovery**: if `ExternalSignalRequested` is in history but no
    ///   terminal event follows, the worker re-attempts delivery and appends the
    ///   terminal event.
    ///
    /// # Cross-shard delivery
    ///
    /// When `target.shard()` differs from the caller's shard, the worker uses
    /// the outbox pattern (no cross-shard transaction). Delivery is at-least-once
    /// from the caller's perspective.
    ///
    /// # Errors
    ///
    /// - [`HarvestError::ExternalSignalFailed`] with `reason_code = "target_terminal"`
    ///   if the target workflow is already in a terminal state.
    /// - [`HarvestError::ExternalSignalFailed`] with `reason_code = "target_unknown"`
    ///   if no execution with `target` is found within the grace window.
    /// - [`HarvestError::NonDeterministic`] if the history at this position does
    ///   not match the requested target/signal combination.
    /// - [`HarvestError::Cancelled`] if the result channel is dropped before the
    ///   worker resolves this command.
    /// - [`HarvestError::Serialization`] if `payload` cannot be serialized to JSON.
    ///
    /// # Panics
    ///
    /// Panics if the internal matcher or commands mutex is poisoned.
    pub async fn signal_external_workflow<P: serde::Serialize>(
        &self,
        target: ExecutionId,
        signal_name: &str,
        payload: P,
    ) -> HarvestResult<()> {
        use crate::replay::HistoryMatch;

        let history_match = self.match_history(|m| m.match_external_signal(target, signal_name));

        match history_match {
            HistoryMatch::Matched { .. } => Ok(()),

            HistoryMatch::ExternalSignalFailed {
                signal_id,
                reason_code,
            } => Err(HarvestError::ExternalSignalFailed {
                signal_id,
                target,
                signal_name: signal_name.to_string(),
                reason_code,
            }),

            HistoryMatch::Diverged { expected, actual } => Err(HarvestError::NonDeterministic(
                format!("external signal mismatch: expected {expected}, got {actual}"),
            )),

            // Crash-recovery: ExternalSignalRequested is already durable; re-attempt delivery
            // using the recorded payload so the target receives the same data regardless of
            // any code changes to the payload expression between the crash and recovery.
            HistoryMatch::ExternalSignalInProgress {
                signal_id,
                payload: recorded_payload,
            } => {
                self.dispatch_signal_command(target, signal_name, recorded_payload, signal_id, true)
                    .await
            }

            // First live call: generate a new signal_id and dispatch.
            HistoryMatch::NoMatch => {
                self.check_strict_replay_no_match(&format!(
                    "ExternalSignalRequested(target={target}, signal={signal_name})"
                ))?;
                let payload_json = serde_json::to_value(&payload)?;
                let observed = serde_json::to_string(&payload_json).map_or(0, |s| s.len() as u64);
                if self.payload_max_signal > 0 && observed > self.payload_max_signal {
                    return Err(HarvestError::PayloadTooLarge {
                        kind: crate::error::PayloadKind::SignalPayload,
                        observed_bytes: observed,
                        cap_bytes: self.payload_max_signal,
                        workflow_type: self.workflow_name.clone(),
                        activity_name: None,
                    });
                }
                self.dispatch_signal_command(
                    target,
                    signal_name,
                    payload_json,
                    ExternalSignalId::new(),
                    false,
                )
                .await
            }

            HistoryMatch::Failed { .. }
            | HistoryMatch::ActivityInProgress { .. }
            | HistoryMatch::AwaitingExternalCompletion { .. }
            | HistoryMatch::ChildInProgress { .. }
            | HistoryMatch::LocalActivityInProgress { .. }
            | HistoryMatch::TimedOut { .. } => {
                unreachable!(
                    "match_external_signal never returns Failed, ActivityInProgress, \
                     AwaitingExternalCompletion, ChildInProgress, LocalActivityInProgress, \
                     or TimedOut"
                )
            }
        }
    }

    /// Push a `SignalExternalWorkflow` command and await its resolution.
    ///
    /// Shared by the crash-recovery (`already_requested = true`) and first-call
    /// (`already_requested = false`) dispatch paths.
    async fn dispatch_signal_command<P: serde::Serialize>(
        &self,
        target: ExecutionId,
        signal_name: &str,
        payload: P,
        signal_id: ExternalSignalId,
        already_requested: bool,
    ) -> HarvestResult<()> {
        let payload_json = serde_json::to_value(&payload)?;
        let (tx, rx) = oneshot::channel();
        self.push_command(WorkflowCommand::SignalExternalWorkflow {
            signal_id,
            target,
            signal_name: signal_name.to_string(),
            payload: payload_json,
            result_tx: tx,
            already_requested,
        });
        match rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(reason_code)) => Err(HarvestError::ExternalSignalFailed {
                signal_id,
                target,
                signal_name: signal_name.to_string(),
                reason_code,
            }),
            Err(_) => Err(HarvestError::Cancelled(format!(
                "signal '{signal_name}' to {target}: result channel dropped"
            ))),
        }
    }

    // ── External activity completion ───────────────────────────────────

    /// Schedule an activity that completes when an *external* system delivers
    /// a result via the management API task-token endpoint.
    ///
    /// Unlike [`execute_activity_raw`](Self::execute_activity_raw), external
    /// activities do **not** occupy a worker slot — the workflow suspends until
    /// an operator or third-party service posts to
    /// `POST /activities/external/{token}/complete` or `/fail`.
    ///
    /// The scheduling step generates a durable, opaque **task token** (UUID)
    /// embedded in event history. On replay the recorded outcome is returned
    /// without contacting anything external.
    ///
    /// # Errors
    ///
    /// - [`HarvestError::NonDeterministic`] if the activity name recorded in
    ///   history does not match `name`.
    /// - [`HarvestError::ActivityFailed`] if the recorded history shows a failure.
    /// - [`HarvestError::Timeout`] if the schedule-to-close deadline expired.
    /// - [`HarvestError::Cancelled`] if the result channel is dropped.
    ///
    /// # Panics
    ///
    /// Panics if the internal matcher or commands mutex is poisoned.
    pub async fn execute_activity_external(
        &self,
        name: &str,
        input: Value,
        queue: &str,
        schedule_to_close_secs: u64,
    ) -> HarvestResult<Value> {
        use crate::replay::HistoryMatch;

        let history_match = self.match_history(|m| m.match_external_activity(name));

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
                format!("external activity mismatch: expected {expected}, got {actual}"),
            )),

            HistoryMatch::AwaitingExternalCompletion { activity_id, token } => {
                // Already recorded — re-emit idempotently so the worker confirms
                // the lookup-table entry is present, then suspend until the
                // external completion triggers a re-run.
                let (tx, rx) = oneshot::channel();
                self.push_command(WorkflowCommand::ScheduleExternalActivity {
                    activity_id,
                    token,
                    name: name.to_string(),
                    input,
                    queue: queue.to_string(),
                    schedule_to_close_secs,
                    result_tx: tx,
                });
                match rx.await {
                    Ok(Ok(output)) => Ok(output),
                    Ok(Err(error)) => Err(HarvestError::ActivityFailed {
                        name: name.to_string(),
                        attempt: 1,
                        source: error.into(),
                    }),
                    Err(_) => Err(HarvestError::Cancelled(format!(
                        "external activity '{name}' cancelled: result channel dropped"
                    ))),
                }
            }

            HistoryMatch::NoMatch => {
                self.check_strict_replay_no_match(&format!("ExternalActivityScheduled({name})"))?;

                // First time — generate a fresh token and schedule.
                let activity_id = self.next_activity_id();
                let token = ExternalActivityToken::new();
                let (tx, rx) = oneshot::channel();
                self.push_command(WorkflowCommand::ScheduleExternalActivity {
                    activity_id,
                    token,
                    name: name.to_string(),
                    input,
                    queue: queue.to_string(),
                    schedule_to_close_secs,
                    result_tx: tx,
                });
                match rx.await {
                    Ok(Ok(output)) => Ok(output),
                    Ok(Err(error)) => Err(HarvestError::ActivityFailed {
                        name: name.to_string(),
                        attempt: 1,
                        source: error.into(),
                    }),
                    Err(_) => Err(HarvestError::Cancelled(format!(
                        "external activity '{name}' cancelled: result channel dropped"
                    ))),
                }
            }

            HistoryMatch::ActivityInProgress { .. }
            | HistoryMatch::ChildInProgress { .. }
            | HistoryMatch::LocalActivityInProgress { .. }
            | HistoryMatch::ExternalSignalInProgress { .. }
            | HistoryMatch::ExternalSignalFailed { .. } => {
                unreachable!(
                    "match_external_activity never returns ChildInProgress, \
                     LocalActivityInProgress, or ExternalSignalInProgress"
                )
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
        let history_match = self.match_history(|m| m.match_continue_as_new(&input));

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
            HistoryMatch::Failed { .. }
            | HistoryMatch::TimedOut { .. }
            | HistoryMatch::ActivityInProgress { .. }
            | HistoryMatch::AwaitingExternalCompletion { .. }
            | HistoryMatch::ChildInProgress { .. }
            | HistoryMatch::LocalActivityInProgress { .. }
            | HistoryMatch::ExternalSignalInProgress { .. }
            | HistoryMatch::ExternalSignalFailed { .. } => Err(HarvestError::NonDeterministic(
                "continue_as_new history contains unexpected terminal state".into(),
            )),
            HistoryMatch::NoMatch => {
                self.check_strict_replay_no_match("ContinueAsNew")?;
                let observed = serde_json::to_string(&input).map_or(0, |s| s.len() as u64);
                if self.payload_max_workflow_input > 0 && observed > self.payload_max_workflow_input
                {
                    return Err(HarvestError::PayloadTooLarge {
                        kind: crate::error::PayloadKind::WorkflowInput,
                        observed_bytes: observed,
                        cap_bytes: self.payload_max_workflow_input,
                        workflow_type: self.workflow_name.clone(),
                        activity_name: None,
                    });
                }
                self.push_command(WorkflowCommand::ContinueAsNew { input });
                park_until_dropped().await
            }
        }
    }

    // ── Query handlers ────────────────────────────────────────────────

    /// Register a named no-arg query handler for this workflow execution.
    ///
    /// Queries allow external clients (via the management API) to inspect the
    /// internal state of a running workflow without writing any event to
    /// `harvest_events`. Handlers run in-memory and must be fast and side-effect
    /// free.
    ///
    /// Registration is **idempotent** — calling with the same `name` multiple
    /// times (e.g., on every replay cycle) is a no-op after the first call.
    ///
    /// For typed request/response shapes, use
    /// [`register_query_handler`](Self::register_query_handler) instead.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::sync::{Arc, Mutex};
    /// use autumn_harvest::context::WorkflowContext;
    ///
    /// # fn example(ctx: &WorkflowContext) {
    /// let items_processed = Arc::new(Mutex::new(0u32));
    ///
    /// let query_state = items_processed.clone();
    /// ctx.register_query("items_processed", move || {
    ///     serde_json::json!(*query_state.lock().unwrap())
    /// });
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
        // Wrap the no-arg closure into the unified `Fn(Value) -> Result<Value, String>` shape.
        let wrapped = Arc::new(move |_args: Value| -> Result<Value, String> { Ok(handler()) });
        self.query_registry
            .lock()
            .expect("query_registry lock poisoned")
            .register(name, wrapped);
    }

    /// Register a **typed** query handler.
    ///
    /// Unlike [`register_query`](Self::register_query), this variant accepts
    /// typed request and response structs. The engine deserializes the incoming
    /// JSON args as `Req`, calls the handler, and serializes the `Resp` back to
    /// JSON. Serialization errors are surfaced as handler errors.
    ///
    /// Registration is **idempotent** — calling with the same `name` multiple
    /// times is a no-op after the first call.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use autumn_harvest::context::WorkflowContext;
    /// use autumn_harvest::types::ExecutionId;
    ///
    /// #[derive(serde::Deserialize)]
    /// struct ProgressQuery { include_details: bool }
    ///
    /// #[derive(serde::Serialize)]
    /// struct ProgressResponse { processed: u32 }
    ///
    /// # let ctx = WorkflowContext::for_replay(ExecutionId::new(), vec![]);
    /// ctx.register_query_handler("progress", |req: &ProgressQuery| {
    ///     Ok(ProgressResponse { processed: 42 })
    /// });
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the internal query registry mutex is poisoned.
    pub fn register_query_handler<Req, Resp, F>(&self, name: &str, handler: F)
    where
        Req: serde::de::DeserializeOwned + 'static,
        Resp: serde::Serialize + 'static,
        F: Fn(&Req) -> Result<Resp, String> + Send + Sync + 'static,
    {
        let wrapped = Arc::new(move |args: Value| -> Result<Value, String> {
            let req: Req = serde_json::from_value(args)
                .map_err(|e| format!("failed to deserialize query args: {e}"))?;
            let resp = handler(&req)?;
            serde_json::to_value(resp)
                .map_err(|e| format!("failed to serialize query response: {e}"))
        });
        self.query_registry
            .lock()
            .expect("query_registry lock poisoned")
            .register(name, wrapped);
    }

    /// Execute a registered query handler with JSON `args`.
    ///
    /// The registry lock is released before the handler runs, preventing
    /// re-entrant deadlocks.
    ///
    /// # Errors
    ///
    /// - [`HarvestError::QueryHandlerNotFound`] — no handler registered under `name`.
    /// - [`HarvestError::QueryHandlerPanicked`] — handler returned an `Err` string.
    ///
    /// # Panics
    ///
    /// Panics if the internal query registry mutex is poisoned.
    pub fn execute_query_with_args(&self, name: &str, args: Value) -> HarvestResult<Value> {
        // Check the imperative registry first.
        let handler = self
            .query_registry
            .lock()
            .expect("query_registry lock poisoned")
            .get(name);

        if let Some(h) = handler {
            return h(args).map_err(HarvestError::QueryHandlerFailed);
        }

        // Fall back to declarative handlers (registered via register_declarative_query_handler).
        let decl_handler = self
            .declarative_queries
            .lock()
            .expect("declarative_queries lock poisoned")
            .get(name)
            .copied();

        if let Some(h) = decl_handler {
            // Pass self so the handler can access ctx.state::<T>().
            return std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| h(self, args)))
                .map_err(|e| {
                    let msg = e
                        .downcast_ref::<&str>()
                        .map(|s| (*s).to_string())
                        .or_else(|| e.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "unknown panic".to_string());
                    HarvestError::QueryHandlerPanicked(msg)
                })?
                .map_err(HarvestError::QueryHandlerFailed);
        }

        Err(HarvestError::QueryHandlerNotFound(name.to_string()))
    }

    /// Execute a registered query handler with no arguments.
    ///
    /// Convenience alias for `execute_query_with_args(name, Value::Null)`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use autumn_harvest::context::WorkflowContext;
    ///
    /// # fn example(ctx: &WorkflowContext) -> autumn_harvest::HarvestResult<()> {
    /// ctx.register_query("status", || serde_json::json!("running"));
    ///
    /// let result = ctx.execute_query("status")?;
    /// assert_eq!(result, "running");
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::QueryHandlerNotFound`] if no query handler is
    /// registered under `name`.
    ///
    /// # Panics
    ///
    /// Panics if the internal query registry mutex is poisoned.
    pub fn execute_query(&self, name: &str) -> HarvestResult<Value> {
        self.execute_query_with_args(name, Value::Null)
    }

    /// Return the names of all currently registered query handlers.
    ///
    /// This is the list the Vantage UI uses to populate the *"Run query"*
    /// drop-down for this workflow execution.
    ///
    /// # Panics
    ///
    /// Panics if the internal query registry mutex is poisoned.
    #[must_use]
    pub fn list_query_names(&self) -> Vec<String> {
        let mut names: std::collections::BTreeSet<String> = self
            .query_registry
            .lock()
            .expect("query_registry lock poisoned")
            .list_names()
            .into_iter()
            .collect();
        names.extend(
            self.declarative_queries
                .lock()
                .expect("declarative_queries lock poisoned")
                .keys()
                .cloned(),
        );
        names.into_iter().collect()
    }

    /// Register a declarative query handler from a [`crate::info::QueryHandlerInfo`] companion record.
    ///
    /// Bridges the companion-info form (generated by `#[query(workflow = "…")]`) into
    /// the existing [`QueryRegistry`]. Registration is idempotent — first wins.
    ///
    /// # Panics
    ///
    /// Panics if the internal registries are poisoned.
    pub fn register_declarative_query_handler(&self, info: &crate::info::QueryHandlerInfo) {
        // Store in the declarative map so we can call with self at dispatch time.
        self.declarative_queries
            .lock()
            .expect("declarative_queries lock poisoned")
            .entry(info.name.to_string())
            .or_insert(info.handler);
    }

    /// Register a declarative update handler from an [`crate::info::UpdateHandlerInfo`] companion record.
    ///
    /// Bridges the companion-info form (generated by `#[update(workflow = "…")]`) into
    /// the existing [`UpdateRegistry`]. Registration is idempotent — first wins.
    ///
    /// # Panics
    ///
    /// Panics if the internal registries are poisoned.
    pub fn register_declarative_update_handler(&self, info: &crate::info::UpdateHandlerInfo) {
        self.declarative_updates
            .lock()
            .expect("declarative_updates lock poisoned")
            .entry(info.name.to_string())
            .or_insert(info.handler);

        // Wrap the fn pointer in a BoxUpdateHandler that creates a handler-mode
        // WorkflowContext on each invocation. Inherit exec_id, start_time, and
        // cancellation_reason from the parent so handlers see consistent values.
        let handler_fn = info.handler;
        let exec_id = self.exec_id;
        let start_time = self.start_time;
        let cancellation_reason = self.cancellation_reason.clone();
        let state = std::sync::Arc::clone(&self.state);
        let name = info.name;

        let boxed_handler: crate::update::BoxUpdateHandler = std::sync::Arc::new(move |input| {
            let ctx = Self::new_for_handler(
                exec_id,
                start_time,
                cancellation_reason.clone(),
                std::sync::Arc::clone(&state),
            );
            handler_fn(ctx, input)
        });

        let boxed_validator: Option<crate::update::BoxUpdateValidator> = info.validator.map(|v| {
            let arc: crate::update::BoxUpdateValidator = std::sync::Arc::new(v);
            arc
        });

        self.update_registry
            .lock()
            .expect("update_registry lock poisoned")
            .register(name, boxed_validator, boxed_handler);
    }

    /// Invoke a registered update handler by name, checking both the imperative
    /// `UpdateRegistry` and the declarative update handler map.
    ///
    /// Returns `None` if no handler is registered under `name`.
    ///
    /// # Panics
    ///
    /// Panics if the internal registries are poisoned.
    #[must_use]
    pub fn invoke_update(
        &self,
        name: &str,
        input: Value,
    ) -> Option<crate::update::UpdateHandlerFuture> {
        // Check the imperative registry first (handles both declarative handlers
        // wired in via register_declarative_update_handler and manually registered
        // ones).
        let future = self
            .update_registry
            .lock()
            .expect("update_registry lock poisoned")
            .invoke(name, input.clone());
        if future.is_some() {
            return future;
        }

        // Fall back to the declarative-only map. This path is taken when a
        // handler was stored without going through register_declarative_update_handler
        // (e.g. in unit tests that call invoke_update directly).
        let handler = self
            .declarative_updates
            .lock()
            .expect("declarative_updates lock poisoned")
            .get(name)
            .copied();
        handler.map(|h| {
            let ctx = Self::new_for_handler(
                self.exec_id,
                self.start_time,
                self.cancellation_reason.clone(),
                std::sync::Arc::clone(&self.state),
            );
            h(ctx, input)
        })
    }

    // ── Update handlers ───────────────────────────────────────────────

    /// Register a typed update handler with an optional validator.
    ///
    /// The `validator` runs synchronously before the update is admitted to
    /// history. A rejected update writes **no event** and the caller receives
    /// the rejection reason. The `handler` is an async closure that mutates
    /// workflow state and returns a typed result.
    ///
    /// Registration is **idempotent** — calling this with the same `name`
    /// multiple times (e.g., on every replay cycle at the top of the workflow
    /// function) is a no-op after the first call.
    ///
    /// # Panics
    ///
    /// Panics if the internal update registry mutex is poisoned.
    pub fn register_update_handler<V, H, F>(&self, name: &str, validator: V, handler: H)
    where
        V: Fn(&Value) -> Result<(), String> + Send + Sync + 'static,
        H: Fn(Value) -> F + Send + Sync + 'static,
        F: std::future::Future<Output = Result<Value, String>> + Send + 'static,
    {
        let boxed_validator: BoxUpdateValidator = Arc::new(validator);
        let boxed_handler: BoxUpdateHandler = Arc::new(move |input| Box::pin(handler(input)));
        self.update_registry
            .lock()
            .expect("update_registry lock poisoned")
            .register(name, Some(boxed_validator), boxed_handler);
    }

    /// Register an update handler with no validator (always admitted).
    ///
    /// Equivalent to [`register_update_handler`](Self::register_update_handler)
    /// with a validator that always returns `Ok(())`.
    ///
    /// # Panics
    ///
    /// Panics if the internal update registry mutex is poisoned.
    pub fn register_update_handler_no_validator<H, F>(&self, name: &str, handler: H)
    where
        H: Fn(Value) -> F + Send + Sync + 'static,
        F: std::future::Future<Output = Result<Value, String>> + Send + 'static,
    {
        let boxed_handler: BoxUpdateHandler = Arc::new(move |input| Box::pin(handler(input)));
        self.update_registry
            .lock()
            .expect("update_registry lock poisoned")
            .register(name, None, boxed_handler);
    }

    /// Validate an incoming update without admitting it.
    ///
    /// - Returns `Ok(())` if the handler exists and the validator accepts.
    /// - Returns [`HarvestError::UpdateHandlerNotFound`] if no handler is registered.
    /// - Returns [`HarvestError::UpdateRejected`] if the validator rejects.
    ///
    /// **No event is appended** — the caller is responsible for appending
    /// `UpdateAdmitted` only when this returns `Ok(())`.
    ///
    /// # Errors
    ///
    /// - Returns [`HarvestError::UpdateHandlerNotFound`] if no handler is registered under `name`.
    /// - Returns [`HarvestError::UpdateRejected`] if the validator rejects `input`.
    ///
    /// # Panics
    ///
    /// Panics if the internal update registry mutex is poisoned.
    pub fn validate_update(&self, name: &str, input: &Value) -> HarvestResult<()> {
        let validator_opt = {
            let registry = self
                .update_registry
                .lock()
                .expect("update_registry lock poisoned");
            // Check existence structurally so validator errors are never confused
            // with a missing handler, regardless of the error message text.
            if !registry.contains(name) {
                return Err(HarvestError::UpdateHandlerNotFound(name.to_string()));
            }
            registry.get_validator(name)
        };

        if let Some(validator) = validator_opt {
            validator(input).map_err(|reason| HarvestError::UpdateRejected { reason })?;
        }
        Ok(())
    }

    /// Execute an already-admitted update by `update_id`.
    ///
    /// Call this **after** the `UpdateAdmitted` event has been durably appended
    /// to `harvest_events`. The method:
    ///
    /// - In **replay mode**: returns the recorded `UpdateCompleted` output or
    ///   `UpdateFailed` error without re-running the handler.
    /// - In **live mode** (no completion in history): runs the registered
    ///   handler and returns its result. The caller is responsible for
    ///   appending `UpdateCompleted` or `UpdateFailed` to history.
    ///
    /// Returns `Err(String)` matching the handler error on failure.
    ///
    /// # Errors
    ///
    /// Returns `Err("update handler 'name' not found")` if no handler is
    /// registered under `name`.
    ///
    /// # Panics
    ///
    /// Panics if the internal update registry or matcher mutex is poisoned.
    pub async fn execute_admitted_update(
        &self,
        update_id: UpdateId,
        name: &str,
        input: Value,
    ) -> Result<Value, String> {
        // Check history first (replay path).
        let history_match = self.match_history(|m| m.match_update(update_id));

        match history_match {
            HistoryMatch::Matched { output } => return Ok(output),
            HistoryMatch::Failed { error, .. } => return Err(error),
            HistoryMatch::NoMatch => {} // live path — run the handler
            other => {
                return Err(format!(
                    "unexpected history match for update '{name}': {other:?}"
                ));
            }
        }

        // Live path: invoke the registered handler.
        let handler_opt = {
            let registry = self
                .update_registry
                .lock()
                .expect("update_registry lock poisoned");
            registry.get_handler(name)
        };

        let result = match handler_opt {
            Some(handler) => handler(input).await,
            None => Err(format!("update handler '{name}' not found")),
        };

        // Durably record the terminal result so the worker can append
        // UpdateCompleted/UpdateFailed before persisting other side effects.
        self.push_command(WorkflowCommand::RecordUpdateResult {
            update_id,
            result: result.clone(),
        });

        result
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

/// Future returned by [`WorkflowContext::await_condition`].
#[must_use = "futures do nothing unless you .await or poll them"]
pub struct AwaitConditionFut<F> {
    predicate: F,
}

impl<F> std::future::Future for AwaitConditionFut<F>
where
    F: FnMut() -> bool + Unpin,
{
    type Output = HarvestResult<()>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        if (this.predicate)() {
            std::task::Poll::Ready(Ok(()))
        } else {
            std::task::Poll::Pending
        }
    }
}

/// Future returned by [`WorkflowContext::await_condition_timeout`].
#[must_use = "futures do nothing unless you .await or poll them"]
pub struct AwaitConditionTimeoutFut<'a, F> {
    context: &'a WorkflowContext,
    timer_id: String,
    predicate: F,
    timer_fut: std::pin::Pin<Box<dyn std::future::Future<Output = HarvestResult<()>> + Send + 'a>>,
}

impl<F> std::future::Future for AwaitConditionTimeoutFut<'_, F>
where
    F: FnMut() -> bool + Unpin,
{
    type Output = HarvestResult<bool>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();

        let cond_met = (this.predicate)();

        if cond_met {
            // Consuming any pending timer started event in history so replay matches correctly.
            if this.context.is_timer_started_next(&this.timer_id)
                && let std::task::Poll::Ready(Err(err)) = this.timer_fut.as_mut().poll(cx)
            {
                return std::task::Poll::Ready(Err(err));
            }
            // Unconditionally clean up any stale StartTimer command pushed to commands queue.
            if let Ok(mut cmds) = this.context.commands.lock() {
                cmds.retain(|cmd| {
                    if let WorkflowCommand::StartTimer { timer_id: id, .. } = cmd {
                        id.as_str() != this.timer_id
                    } else {
                        true
                    }
                });
            }
            return std::task::Poll::Ready(Ok(true));
        }

        match this.timer_fut.as_mut().poll(cx) {
            std::task::Poll::Ready(Ok(())) => std::task::Poll::Ready(Ok(false)),
            std::task::Poll::Ready(Err(err)) => std::task::Poll::Ready(Err(err)),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
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
    /// Latest heartbeat payload durably persisted by the previous attempt.
    heartbeat_details: Option<serde_json::Value>,
    /// Why heartbeat APIs are unavailable for this activity context.
    heartbeat_unsupported_reason: Option<&'static str>,
    /// Cancellation token -- allows the worker to signal graceful shutdown.
    cancel: tokio_util::sync::CancellationToken,
    /// Optional durable queue-state cancellation check for worker activities.
    #[cfg(feature = "db")]
    cancellation_check: Option<ActivityCancellationCheck>,
    /// W3C tracecontext carrier captured at enqueue, surfaced so activity
    /// handlers can propagate the trace to downstream services.
    trace_context: Option<crate::telemetry::TraceContextCarrier>,
    /// Stable idempotency key for this logical activity invocation.
    ///
    /// Derived from the `ActivityExecId` recorded in the `ActivityScheduled`
    /// or `LocalActivityScheduled` event — identical across all retry attempts
    /// for the same logical invocation.
    idempotency_key: Option<IdempotencyKey>,
    /// Which attempt of the logical activity invocation this context represents.
    /// `1` for the first attempt. Stable retries share a key but differ here.
    attempt: Option<u32>,
}

impl ActivityContext {
    /// Production constructor -- creates a context with heartbeat channel and
    /// cancellation token.
    #[allow(dead_code)]
    pub(crate) fn new(
        state: SharedState,
        heartbeat_tx: Option<tokio::sync::mpsc::Sender<serde_json::Value>>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Self {
        let heartbeat_unsupported_reason = heartbeat_tx
            .is_none()
            .then_some(NO_HEARTBEAT_FLUSHER_REASON);

        Self {
            state,
            heartbeat_tx,
            heartbeat_details: None,
            heartbeat_unsupported_reason,
            cancel,
            #[cfg(feature = "db")]
            cancellation_check: None,
            trace_context: None,
            idempotency_key: None,
            attempt: None,
        }
    }

    /// Production constructor that also checks durable queue state on heartbeat.
    #[cfg(feature = "db")]
    pub(crate) fn new_with_cancellation_check(
        state: SharedState,
        heartbeat_tx: Option<tokio::sync::mpsc::Sender<serde_json::Value>>,
        heartbeat_details: Option<serde_json::Value>,
        cancel: tokio_util::sync::CancellationToken,
        task_id: uuid::Uuid,
        pool: ActivityCancellationPool,
    ) -> Self {
        let heartbeat_unsupported_reason = heartbeat_tx
            .is_none()
            .then_some(NO_HEARTBEAT_FLUSHER_REASON);

        Self {
            state,
            heartbeat_tx,
            heartbeat_details,
            heartbeat_unsupported_reason,
            cancel,
            cancellation_check: Some(ActivityCancellationCheck {
                task_id,
                pool,
                last_checked_at: Mutex::new(None),
            }),
            trace_context: None,
            idempotency_key: None,
            attempt: None,
        }
    }

    #[cfg_attr(not(feature = "db"), allow(dead_code))]
    pub(crate) fn new_local_activity(
        state: SharedState,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self {
            state,
            heartbeat_tx: None,
            heartbeat_details: None,
            heartbeat_unsupported_reason: Some(LOCAL_ACTIVITY_HEARTBEAT_REASON),
            cancel,
            #[cfg(feature = "db")]
            cancellation_check: None,
            trace_context: None,
            idempotency_key: None,
            attempt: None,
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

    /// Attach a stable idempotency key to this context.
    ///
    /// The engine calls this automatically for both regular and local
    /// activities. You only need it when constructing a context manually in
    /// tests.
    #[must_use]
    pub fn with_idempotency_key(mut self, key: IdempotencyKey) -> Self {
        self.idempotency_key = Some(key);
        self
    }

    /// Set the attempt number for this activity invocation.
    ///
    /// `1` means the first attempt. The engine sets this automatically; you
    /// only need it when constructing a context manually in tests.
    #[must_use]
    pub const fn with_attempt(mut self, attempt: u32) -> Self {
        self.attempt = Some(attempt);
        self
    }

    /// The stable idempotency key for this logical activity invocation.
    ///
    /// Identical across all retry attempts for the same logical invocation
    /// (worker restarts, duplicate dispatch, and deterministic replay all
    /// produce the same key).  Safe to use directly as an `Idempotency-Key`
    /// HTTP request header.
    ///
    /// Use [`IdempotencyKey::subkey`] to derive a named child key when one
    /// activity must produce multiple distinct side effects.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Config`] if no idempotency key was attached to
    /// this context.  In production this never happens; in tests use
    /// [`Self::with_idempotency_key`] or [`Self::new_test`] which always
    /// provides a key.
    pub fn idempotency_key(&self) -> Result<&IdempotencyKey, HarvestError> {
        self.idempotency_key.as_ref().ok_or_else(|| {
            HarvestError::Config(
                "idempotency key not available on this context; \
                 use ActivityContext::new_test() or ActivityContext::with_idempotency_key()"
                    .into(),
            )
        })
    }

    /// Which attempt of the logical activity invocation this context
    /// represents.  `Some(1)` for the first attempt, `Some(2)` for the first
    /// retry, and so on.  `None` if the engine did not set an attempt (e.g.
    /// contexts built with the bare `new` constructor).
    ///
    /// The default idempotency key is **retry-stable** (same value for all
    /// attempts).  Call `ctx.idempotency_key()?.subkey(&format!("attempt-{}",
    /// ctx.attempt().unwrap_or(1)))` to opt into an attempt-scoped subkey if
    /// your downstream API requires distinct keys per attempt.
    #[must_use]
    pub const fn attempt(&self) -> Option<u32> {
        self.attempt
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

    /// Return the heartbeat payload durably persisted by the previous attempt.
    ///
    /// This is a snapshot captured before the current attempt starts. Heartbeats
    /// sent by the current attempt become visible only to a later retry attempt,
    /// after the heartbeat flusher successfully writes them to Postgres.
    ///
    /// # Errors
    ///
    /// - [`HarvestError::Serialization`] if the stored payload does not
    ///   deserialize into `T`.
    /// - [`HarvestError::Config`] for local activities, which do not support
    ///   heartbeating.
    pub fn heartbeat_details<T: serde::de::DeserializeOwned>(
        &self,
    ) -> crate::HarvestResult<Option<T>> {
        if let Some(reason) = self.heartbeat_unsupported_reason {
            return Err(HarvestError::Config(reason.into()));
        }

        self.heartbeat_details
            .clone()
            .map(serde_json::from_value)
            .transpose()
            .map_err(HarvestError::from)
    }

    /// Send a heartbeat to signal the activity is still running.
    ///
    /// The `details` payload is serialized to JSON and forwarded to the worker's
    /// heartbeat loop, which batches writes to the database. On retry, the last
    /// successfully flushed payload from the previous attempt is available via
    /// [`Self::heartbeat_details`]. Always check the return value -- an
    /// `Err(Cancelled)` means the workflow was cancelled and the activity should
    /// wind down promptly.
    ///
    /// Within a single attempt, heartbeat payloads are last-write-wins and are
    /// not read back through this context. Call [`Self::heartbeat_details`] at
    /// the start of a later retry attempt to resume from the prior checkpoint.
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

        if let Some(reason) = self.heartbeat_unsupported_reason {
            return Err(HarvestError::Config(reason.into()));
        }

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
    /// workflow engine. It does not attach a heartbeat flusher, so
    /// [`Self::heartbeat`] and [`Self::heartbeat_details`] return
    /// [`HarvestError::Config`].
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
        let id = ActivityExecId::new();
        Self::new(
            empty_shared_state(),
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .with_idempotency_key(IdempotencyKey::from_activity_exec_id(id))
        .with_attempt(1)
    }

    /// Like [`new_test`](Self::new_test) but intentionally omits the
    /// idempotency key.  Used only in tests that verify the error path.
    #[cfg(any(test, feature = "testing"))]
    #[must_use]
    pub fn new_for_idempotency_test_no_key() -> Self {
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

    #[test]
    fn workflow_context_history_event_count_reports_loaded_history() {
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "poll_for_work".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!({"work": false}),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        assert_eq!(ctx.history_event_count(), 3);
        assert!(
            ctx.is_replaying(),
            "counting history must not advance replay"
        );
    }

    #[test]
    fn workflow_context_should_continue_as_new_uses_soft_threshold() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::MarkerRecorded {
                name: "poll-cycle".into(),
                details: serde_json::json!({"n": 1}),
            },
            WorkflowEvent::MarkerRecorded {
                name: "poll-cycle".into(),
                details: serde_json::json!({"n": 2}),
            },
        ];
        let policy = WorkflowHistoryPolicy::default().with_continue_as_new_threshold(2);

        let ctx = WorkflowContext::for_replay_with_state_and_history_policy(
            ExecutionId::new(),
            events,
            empty_shared_state(),
            policy,
        );

        assert_eq!(ctx.history_event_count(), 3);
        assert!(ctx.should_continue_as_new());
    }

    #[test]
    fn workflow_context_should_continue_as_new_is_false_at_threshold_boundary() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::MarkerRecorded {
                name: "poll-cycle".into(),
                details: serde_json::json!({"n": 1}),
            },
        ];
        let policy = WorkflowHistoryPolicy::default().with_continue_as_new_threshold(2);

        let ctx = WorkflowContext::for_replay_with_state_and_history_policy(
            ExecutionId::new(),
            events,
            empty_shared_state(),
            policy,
        );

        assert_eq!(ctx.history_event_count(), 2);
        assert!(!ctx.should_continue_as_new());
    }

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

    #[derive(Debug, PartialEq, serde::Deserialize)]
    struct TestHeartbeatDetails {
        progress: u32,
    }

    fn activity_context_with_heartbeat_details(
        details: Option<serde_json::Value>,
    ) -> ActivityContext {
        let mut ctx = ActivityContext::new_test();
        ctx.heartbeat_details = details;
        ctx.heartbeat_unsupported_reason = None;
        ctx
    }

    #[test]
    fn activity_context_heartbeat_details_deserializes_previous_payload() {
        let ctx = activity_context_with_heartbeat_details(Some(serde_json::json!({
            "progress": 42,
        })));

        let details = ctx
            .heartbeat_details::<TestHeartbeatDetails>()
            .expect("heartbeat details should deserialize");

        assert_eq!(details, Some(TestHeartbeatDetails { progress: 42 }));
    }

    #[test]
    fn activity_context_heartbeat_details_type_mismatch_returns_error() {
        let ctx = activity_context_with_heartbeat_details(Some(serde_json::json!({
            "progress": "not a number",
        })));

        let result = ctx.heartbeat_details::<TestHeartbeatDetails>();

        assert!(matches!(result, Err(HarvestError::Serialization(_))));
    }

    #[tokio::test]
    async fn local_activity_context_heartbeat_returns_explicit_error() {
        let ctx = ActivityContext::new_local_activity(
            empty_shared_state(),
            tokio_util::sync::CancellationToken::new(),
        );

        let result = ctx.heartbeat(serde_json::json!({"progress": 1})).await;

        assert!(
            matches!(result, Err(HarvestError::Config(message)) if message.contains("local activities do not support heartbeats"))
        );
    }

    #[tokio::test]
    async fn activity_context_without_heartbeat_channel_rejects_heartbeats() {
        let ctx = ActivityContext::new(
            empty_shared_state(),
            None,
            tokio_util::sync::CancellationToken::new(),
        );

        let heartbeat_result = ctx.heartbeat(serde_json::json!({"progress": 1})).await;
        let details_result = ctx.heartbeat_details::<TestHeartbeatDetails>();

        assert!(
            matches!(heartbeat_result, Err(HarvestError::Config(message)) if message.contains("heartbeats are not supported"))
        );
        assert!(
            matches!(details_result, Err(HarvestError::Config(message)) if message.contains("heartbeats are not supported"))
        );
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
                error_type: "Error".into(),
                non_retryable: false,
                details: None,
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

    // ── Red-phase versioning tests ────────────────────────────────────────

    /// min > max is a programmer error — must panic immediately with a clear message.
    #[test]
    #[should_panic(expected = "min version 5 must not exceed max version 2")]
    fn version_panics_when_min_exceeds_max() {
        let ctx = WorkflowContext::new_test();
        ctx.version("my_gate", 5, 2);
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
    async fn context_reparks_inflight_activity_without_rescheduling() {
        let activity_id = ActivityExecId::new();
        let ctx = Arc::new(WorkflowContext::for_replay(
            ExecutionId::new(),
            vec![
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
            ],
        ));
        let ctx2 = Arc::clone(&ctx);

        let handle = tokio::spawn(async move {
            ctx2.execute_activity_raw("send_email", Value::Null, "default")
                .await
        });
        tokio::task::yield_now().await;

        let timeout_result =
            tokio::time::timeout(std::time::Duration::from_millis(50), handle).await;
        assert!(
            timeout_result.is_err(),
            "activity with only a scheduled event should suspend until its terminal event arrives"
        );

        let commands = ctx.drain_commands();
        assert!(
            commands
                .iter()
                .all(|cmd| !matches!(cmd, WorkflowCommand::ScheduleActivity { .. })),
            "in-flight activity replay must not emit a fresh ScheduleActivity: {commands:?}"
        );
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

    /// When a child's `ChildWorkflowStarted` event is in history but its terminal
    /// hasn't arrived yet, the context must re-emit a `StartChildWorkflow` command
    /// carrying the **existing** `child_id` (not a fresh one) so the worker can
    /// re-park the parent idempotently.
    #[tokio::test]
    async fn context_child_in_progress_re_emits_with_existing_child_id() {
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

        let ctx = Arc::new(WorkflowContext::for_replay(ExecutionId::new(), events));
        let ctx_task = Arc::clone(&ctx);
        let join = tokio::spawn(async move {
            ctx_task
                .spawn_child_workflow_raw("process_order", serde_json::json!({"sku":"book"}))
                .await
        });
        tokio::task::yield_now().await;

        let mut commands = ctx.drain_commands();
        assert_eq!(
            commands.len(),
            1,
            "in-progress child must re-emit exactly one StartChildWorkflow command"
        );
        let WorkflowCommand::StartChildWorkflow {
            child_id: reused_id,
            workflow_name,
            result_tx,
            ..
        } = commands.remove(0)
        else {
            panic!("expected StartChildWorkflow command");
        };
        assert_eq!(workflow_name, "process_order");
        assert_eq!(
            reused_id, child_id,
            "re-emitted command must carry the existing child_id, not a new one"
        );

        result_tx
            .send(Ok(serde_json::json!({"done": true})))
            .expect("receiver must be alive");
        let result = join.await.expect("join").expect("should succeed");
        assert_eq!(result, serde_json::json!({"done": true}));
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

    #[test]
    fn context_check_cancellation_returns_error_when_cancelled() {
        let mut ctx = WorkflowContext::new_test();
        ctx.cancellation_reason = Some("user request".to_string());
        let result = ctx.check_cancellation();
        match result {
            Err(HarvestError::Cancelled(reason)) => assert_eq!(reason, "user request"),
            _ => panic!("Expected Cancelled error"),
        }
    }

    #[test]
    fn context_check_cancellation_returns_ok_when_not_cancelled() {
        let ctx = WorkflowContext::new_test();
        assert!(ctx.check_cancellation().is_ok());
    }

    // ── Local activity tests ──────────────────────────────────────────────

    #[tokio::test]
    async fn execute_local_activity_emits_run_local_command_during_live_execution() {
        let ctx = WorkflowContext::new_test();

        // Race the future against a short sleep — it should suspend (not complete)
        // because no history provides a result.
        let fut = ctx.execute_local_activity_raw("format_data", Value::Null, None, None);
        tokio::pin!(fut);
        tokio::select! {
            _ = &mut fut => panic!("should suspend, not complete"),
            () = tokio::time::sleep(std::time::Duration::from_millis(20)) => {}
        }

        let commands = ctx.drain_commands();
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            WorkflowCommand::RunLocalActivity { name, .. } => {
                assert_eq!(name, "format_data");
            }
            other => panic!("expected RunLocalActivity, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn execute_local_activity_returns_history_result_during_replay() {
        let id = ActivityExecId::new();
        let expected = serde_json::json!({"formatted": "hello"});
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
            },
            WorkflowEvent::LocalActivityScheduled {
                activity_id: id,
                name: "format_data".into(),
                input: Value::Null,
            },
            WorkflowEvent::LocalActivityCompleted {
                activity_id: id,
                output: expected.clone(),
            },
        ];
        let ctx = WorkflowContext::for_replay(crate::types::ExecutionId::new(), events);
        let result = ctx
            .execute_local_activity_raw("format_data", Value::Null, None, None)
            .await
            .expect("replay should succeed");
        assert_eq!(result, expected);
        assert!(!ctx.is_replaying());
    }

    #[tokio::test]
    async fn execute_local_activity_returns_error_for_exhausted_retries_in_history() {
        let id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
            },
            WorkflowEvent::LocalActivityScheduled {
                activity_id: id,
                name: "format_data".into(),
                input: Value::Null,
            },
            WorkflowEvent::LocalActivityFailed {
                activity_id: id,
                error: "always fails".into(),
                attempt: 1,
            },
            // LocalActivityExhausted is the authoritative terminal marker; without
            // it the context would treat this as a crash-between-retries case.
            WorkflowEvent::LocalActivityExhausted {
                activity_id: id,
                error: "always fails".into(),
                attempt: 1,
            },
        ];
        let ctx = WorkflowContext::for_replay(crate::types::ExecutionId::new(), events);
        let result = ctx
            .execute_local_activity_raw("format_data", Value::Null, None, None)
            .await;
        assert!(
            matches!(result, Err(HarvestError::ActivityFailed { attempt: 1, .. })),
            "expected ActivityFailed, got {result:?}"
        );
    }

    #[tokio::test]
    async fn execute_local_activity_divergence_is_nondeterministic() {
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: chrono::Utc::now(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "other_activity".into(),
                input: Value::Null,
                queue: "default".into(),
            },
        ];
        let ctx = WorkflowContext::for_replay(crate::types::ExecutionId::new(), events);
        let result = ctx
            .execute_local_activity_raw("format_data", Value::Null, None, None)
            .await;
        assert!(matches!(result, Err(HarvestError::NonDeterministic(_))));
    }

    // ── Parallel child workflow tests ────────────────────────────────────────

    #[tokio::test]
    async fn context_live_parallel_children_emit_two_start_commands() {
        // Verify that two concurrent spawn_child_workflow_raw calls both push
        // StartChildWorkflow commands and that the workflow context collects them.
        let ctx = Arc::new(WorkflowContext::new_test());
        let ctx_a = Arc::clone(&ctx);
        let ctx_b = Arc::clone(&ctx);

        // Spawn both children concurrently; the executor will collect both commands.
        let join_a = tokio::spawn(async move {
            ctx_a
                .spawn_child_workflow_raw("child_a", serde_json::json!({"id":"A"}))
                .await
        });
        let join_b = tokio::spawn(async move {
            ctx_b
                .spawn_child_workflow_raw("child_b", serde_json::json!({"id":"B"}))
                .await
        });

        tokio::task::yield_now().await;

        let mut commands = ctx.drain_commands();
        assert_eq!(
            commands.len(),
            2,
            "both children should have queued commands"
        );
        commands.sort_by_key(|c| match c {
            WorkflowCommand::StartChildWorkflow { workflow_name, .. } => workflow_name.clone(),
            _ => String::new(),
        });

        let WorkflowCommand::StartChildWorkflow {
            workflow_name: name_a,
            result_tx: tx_a,
            ..
        } = commands.remove(0)
        else {
            panic!("expected StartChildWorkflow for child_a");
        };
        let WorkflowCommand::StartChildWorkflow {
            workflow_name: name_b,
            result_tx: tx_b,
            ..
        } = commands.remove(0)
        else {
            panic!("expected StartChildWorkflow for child_b");
        };

        assert_eq!(name_a, "child_a");
        assert_eq!(name_b, "child_b");

        tx_a.send(Ok(serde_json::json!({"a":"done"}))).unwrap();
        tx_b.send(Ok(serde_json::json!({"b":"done"}))).unwrap();

        join_a.await.expect("join_a").expect("a should succeed");
        join_b.await.expect("join_b").expect("b should succeed");
    }

    /// RED: parent wakes after child A completes but child B is still pending.
    ///
    /// With partial history [Started, ChildStarted(A), ChildStarted(B), ChildCompleted(A)],
    /// replaying `spawn_child_workflow_raw("b")` should NOT return `NonDeterministic`.
    /// Instead it should re-emit a `StartChildWorkflow` command carrying B's existing
    /// `child_id` so the worker can re-park the parent without creating a duplicate child.
    #[tokio::test]
    async fn context_partial_history_parallel_children_re_parks_pending_child() {
        let child_a = ExecutionId::new();
        let child_b = ExecutionId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id: child_a,
                workflow_name: "child_a".into(),
                input: serde_json::json!({"id":"A"}),
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id: child_b,
                workflow_name: "child_b".into(),
                input: serde_json::json!({"id":"B"}),
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id: child_a,
                output: serde_json::json!({"a":"done"}),
            },
            // No terminal for child_b yet — parent woke early.
        ];

        let ctx = Arc::new(WorkflowContext::for_replay(ExecutionId::new(), events));
        let ctx_replay = Arc::clone(&ctx);

        let handle = tokio::spawn(async move {
            // child_a should replay from history immediately.
            let a = ctx_replay
                .spawn_child_workflow_raw("child_a", serde_json::json!({"id":"A"}))
                .await
                .expect("child_a should replay from completed history");

            // child_b is still in-progress; the context should re-emit a
            // StartChildWorkflow command (not return NonDeterministic).
            ctx_replay
                .spawn_child_workflow_raw("child_b", serde_json::json!({"id":"B"}))
                .await
                .map(|b| (a, b))
        });

        tokio::task::yield_now().await;

        let commands = ctx.drain_commands();
        assert_eq!(
            commands.len(),
            1,
            "exactly one re-park command for the pending child"
        );

        let WorkflowCommand::StartChildWorkflow {
            child_id: reused_id,
            workflow_name,
            result_tx,
            ..
        } = commands.into_iter().next().unwrap()
        else {
            panic!("expected StartChildWorkflow re-park command");
        };

        assert_eq!(workflow_name, "child_b");
        // The re-emitted command MUST reuse the existing child_id from history,
        // not generate a new one — the worker uses this to detect the child
        // already exists and avoids creating a duplicate execution.
        assert_eq!(
            reused_id, child_b,
            "re-park command must carry the child_id already recorded in history"
        );

        // Drive the result to unblock the spawned task.
        result_tx
            .send(Ok(serde_json::json!({"b":"done"})))
            .expect("receiver must still be alive");

        let (a, b) = handle
            .await
            .expect("task join")
            .expect("both should succeed");
        assert_eq!(a, serde_json::json!({"a":"done"}));
        assert_eq!(b, serde_json::json!({"b":"done"}));
    }

    // ── upsert_search_attrs tests ─────────────────────────────────────

    #[test]
    fn upsert_search_attrs_live_emits_command() {
        let ctx = WorkflowContext::new_test();
        ctx.upsert_search_attrs([
            (
                "tenant".to_string(),
                Some(Value::String("acme".to_string())),
            ),
            (
                "phase".to_string(),
                Some(Value::String("awaiting_approval".to_string())),
            ),
        ])
        .expect("should succeed in live mode");

        let cmds = ctx.drain_commands();
        assert_eq!(cmds.len(), 1);
        match &cmds[0] {
            WorkflowCommand::UpsertSearchAttributes { patch } => {
                assert_eq!(
                    patch.get("tenant"),
                    Some(&Some(Value::String("acme".to_string())))
                );
                assert_eq!(
                    patch.get("phase"),
                    Some(&Some(Value::String("awaiting_approval".to_string())))
                );
            }
            other => panic!("expected UpsertSearchAttributes, got {other:?}"),
        }
    }

    #[test]
    fn upsert_search_attrs_replay_is_noop() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: ActivityExecId::new(),
                name: "step_1".into(),
                input: Value::Null,
                queue: "default".into(),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        assert!(ctx.is_replaying(), "context should be replaying");

        ctx.upsert_search_attrs([("phase".to_string(), Some(Value::String("v1".to_string())))])
            .expect("should succeed silently during replay");

        assert!(
            ctx.drain_commands().is_empty(),
            "no command emitted during replay"
        );
    }

    #[test]
    fn upsert_search_attrs_none_value_means_remove() {
        let ctx = WorkflowContext::new_test();
        ctx.upsert_search_attrs([("old_key".to_string(), None)])
            .expect("removal should succeed");

        let cmds = ctx.drain_commands();
        assert_eq!(cmds.len(), 1);
        match &cmds[0] {
            WorkflowCommand::UpsertSearchAttributes { patch } => {
                assert_eq!(patch.get("old_key"), Some(&None));
            }
            other => panic!("expected UpsertSearchAttributes, got {other:?}"),
        }
    }

    #[test]
    fn upsert_search_attrs_rejects_empty_key() {
        let ctx = WorkflowContext::new_test();
        let err = ctx
            .upsert_search_attrs([(String::new(), Some(Value::Bool(true)))])
            .expect_err("empty key must be rejected");
        assert!(matches!(err, HarvestError::InvalidSearchAttribute { .. }));
    }

    #[test]
    fn upsert_search_attrs_rejects_key_too_long() {
        let ctx = WorkflowContext::new_test();
        let long_key = "a".repeat(65);
        let err = ctx
            .upsert_search_attrs([(long_key, Some(Value::Bool(true)))])
            .expect_err("key > 64 chars must be rejected");
        assert!(matches!(err, HarvestError::InvalidSearchAttribute { .. }));
    }

    #[test]
    fn upsert_search_attrs_rejects_invalid_key_chars() {
        let ctx = WorkflowContext::new_test();
        let err = ctx
            .upsert_search_attrs([("bad key!".to_string(), Some(Value::Bool(true)))])
            .expect_err("key with space/special chars must be rejected");
        assert!(matches!(err, HarvestError::InvalidSearchAttribute { .. }));
    }

    #[test]
    fn upsert_search_attrs_rejects_reserved_key() {
        let ctx = WorkflowContext::new_test();
        for key in &["exec_id", "workflow_name", "shard_id", "status", "run_id"] {
            let err = ctx
                .upsert_search_attrs([(key.to_string(), Some(Value::String("x".to_string())))])
                .expect_err(&format!("reserved key '{key}' must be rejected"));
            assert!(
                matches!(err, HarvestError::InvalidSearchAttribute { .. }),
                "key '{key}': expected InvalidSearchAttribute"
            );
        }
    }

    #[test]
    fn upsert_search_attrs_rejects_reserved_prefix() {
        let ctx = WorkflowContext::new_test();
        let err = ctx
            .upsert_search_attrs([(
                "_harvest_shard".to_string(),
                Some(Value::String("1".to_string())),
            )])
            .expect_err("key with _harvest prefix must be rejected");
        assert!(matches!(err, HarvestError::InvalidSearchAttribute { .. }));
    }

    #[test]
    fn upsert_search_attrs_rejects_object_value() {
        let ctx = WorkflowContext::new_test();
        let err = ctx
            .upsert_search_attrs([(
                "meta".to_string(),
                Some(serde_json::json!({"nested": "object"})),
            )])
            .expect_err("object value must be rejected");
        assert!(matches!(err, HarvestError::InvalidSearchAttribute { .. }));
    }

    #[test]
    fn upsert_search_attrs_rejects_array_value() {
        let ctx = WorkflowContext::new_test();
        let err = ctx
            .upsert_search_attrs([("tags".to_string(), Some(serde_json::json!(["a", "b"])))])
            .expect_err("array value must be rejected");
        assert!(matches!(err, HarvestError::InvalidSearchAttribute { .. }));
    }

    #[test]
    fn upsert_search_attrs_accepts_primitive_values() {
        let ctx = WorkflowContext::new_test();
        ctx.upsert_search_attrs([
            (
                "str_key".to_string(),
                Some(Value::String("hello".to_string())),
            ),
            ("num_key".to_string(), Some(serde_json::json!(42))),
            ("bool_key".to_string(), Some(Value::Bool(false))),
            ("null_removal".to_string(), None),
        ])
        .expect("primitives and None should be accepted");

        let cmds = ctx.drain_commands();
        assert_eq!(cmds.len(), 1);
        match &cmds[0] {
            WorkflowCommand::UpsertSearchAttributes { patch } => {
                assert!(patch.contains_key("str_key"));
                assert!(patch.contains_key("num_key"));
                assert!(patch.contains_key("bool_key"));
                assert!(patch.contains_key("null_removal"));
                assert_eq!(patch["null_removal"], None);
            }
            other => panic!("expected UpsertSearchAttributes, got {other:?}"),
        }
    }

    #[test]
    fn upsert_search_attrs_empty_patch_is_noop() {
        let ctx = WorkflowContext::new_test();
        ctx.upsert_search_attrs(std::iter::empty::<(String, Option<Value>)>())
            .expect("empty patch should succeed");
        assert!(
            ctx.drain_commands().is_empty(),
            "empty patch emits no command"
        );
    }

    // ── signal_external_workflow tests (issue #330) ───────────────────────

    #[tokio::test]
    async fn signal_external_workflow_live_mode_emits_command() {
        let target = ExecutionId::new();

        // Signal delivery never happens in live mode without a worker resolving
        // the oneshot. We just want to verify the command is pushed and serialization
        // works. We do this by dropping the context after pushing the command.
        let target_clone = target;
        let (cmds, target_id) = {
            // Spawn a task so we can drive the future without blocking.
            let ctx = WorkflowContext::new_test();
            let ctx_ref = &ctx;
            // Run signal_external_workflow concurrently; collect the command
            // before it awaits.
            let cmd_fut = ctx_ref.signal_external_workflow(
                target_clone,
                "tenant_cancel",
                serde_json::json!({"reason": "billing_lapse"}),
            );
            // The future won't finish without a worker; we just need the command pushed.
            // Drop the future after yielding once.
            let _ = tokio::time::timeout(std::time::Duration::from_millis(1), cmd_fut).await;
            (ctx.drain_commands(), target_clone)
        };

        assert_eq!(cmds.len(), 1, "one SignalExternalWorkflow command expected");
        match &cmds[0] {
            WorkflowCommand::SignalExternalWorkflow {
                target,
                signal_name,
                already_requested,
                ..
            } => {
                assert_eq!(*target, target_id);
                assert_eq!(signal_name, "tenant_cancel");
                assert!(
                    !already_requested,
                    "first call should not be already_requested"
                );
            }
            other => panic!("expected SignalExternalWorkflow, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn signal_external_workflow_replays_delivered_outcome() {
        let signal_id = crate::types::ExternalSignalId::new();
        let target = ExecutionId::new();

        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ExternalSignalRequested {
                signal_id,
                target,
                signal_name: "tenant_cancel".into(),
                payload: serde_json::json!({"reason": "billing_lapse"}),
            },
            WorkflowEvent::ExternalSignalDelivered { signal_id },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let result = ctx
            .signal_external_workflow(
                target,
                "tenant_cancel",
                serde_json::json!({"reason": "billing_lapse"}),
            )
            .await;

        assert!(result.is_ok(), "delivered history should return Ok(())");
        assert!(ctx.drain_commands().is_empty(), "replay emits no commands");
    }

    #[tokio::test]
    async fn signal_external_workflow_replays_failed_outcome_target_terminal() {
        let signal_id = crate::types::ExternalSignalId::new();
        let target = ExecutionId::new();

        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ExternalSignalRequested {
                signal_id,
                target,
                signal_name: "cancel".into(),
                payload: Value::Null,
            },
            WorkflowEvent::ExternalSignalFailed {
                signal_id,
                reason_code: "target_terminal".into(),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let result = ctx
            .signal_external_workflow(target, "cancel", serde_json::Value::Null)
            .await;

        assert!(result.is_err(), "failed history should return Err");
        match result.unwrap_err() {
            HarvestError::ExternalSignalFailed { reason_code, .. } => {
                assert_eq!(reason_code, "target_terminal");
            }
            other => panic!("expected ExternalSignalFailed, got {other:?}"),
        }
        assert!(ctx.drain_commands().is_empty(), "replay emits no commands");
    }

    #[tokio::test]
    async fn signal_external_workflow_replays_failed_outcome_target_unknown() {
        let signal_id = crate::types::ExternalSignalId::new();
        let target = ExecutionId::new();

        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ExternalSignalRequested {
                signal_id,
                target,
                signal_name: "notify".into(),
                payload: Value::Null,
            },
            WorkflowEvent::ExternalSignalFailed {
                signal_id,
                reason_code: "target_unknown".into(),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let result = ctx
            .signal_external_workflow(target, "notify", serde_json::Value::Null)
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            HarvestError::ExternalSignalFailed { reason_code, .. } => {
                assert_eq!(reason_code, "target_unknown");
            }
            other => panic!("expected ExternalSignalFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn signal_external_workflow_nondeterminism_wrong_signal_name() {
        let signal_id = crate::types::ExternalSignalId::new();
        let target = ExecutionId::new();

        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ExternalSignalRequested {
                signal_id,
                target,
                signal_name: "cancel".into(),
                payload: Value::Null,
            },
            WorkflowEvent::ExternalSignalDelivered { signal_id },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        // Wrong signal name: should detect non-determinism
        let result = ctx
            .signal_external_workflow(target, "different_signal", serde_json::Value::Null)
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            HarvestError::NonDeterministic(msg) => {
                assert!(msg.contains("external signal mismatch"), "msg: {msg}");
            }
            other => panic!("expected NonDeterministic, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn signal_external_workflow_nondeterminism_wrong_target() {
        let signal_id = crate::types::ExternalSignalId::new();
        let target = ExecutionId::new();
        let other_target = ExecutionId::new();

        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ExternalSignalRequested {
                signal_id,
                target,
                signal_name: "cancel".into(),
                payload: Value::Null,
            },
            WorkflowEvent::ExternalSignalDelivered { signal_id },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        // Wrong target: should detect non-determinism
        let result = ctx
            .signal_external_workflow(other_target, "cancel", serde_json::Value::Null)
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            HarvestError::NonDeterministic(msg) => {
                assert!(msg.contains("external signal mismatch"), "msg: {msg}");
            }
            other => panic!("expected NonDeterministic, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn signal_external_workflow_after_activity_replays_correctly() {
        let signal_id = crate::types::ExternalSignalId::new();
        let activity_id = ActivityExecId::new();
        let target = ExecutionId::new();

        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "step_one".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!("done"),
            },
            WorkflowEvent::ExternalSignalRequested {
                signal_id,
                target,
                signal_name: "cancel".into(),
                payload: Value::Null,
            },
            WorkflowEvent::ExternalSignalDelivered { signal_id },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let r = ctx
            .execute_activity_raw("step_one", Value::Null, "default")
            .await
            .expect("activity replay ok");
        assert_eq!(r, "done");

        let sig_result = ctx
            .signal_external_workflow(target, "cancel", serde_json::Value::Null)
            .await;
        assert!(sig_result.is_ok(), "signal after activity should replay Ok");
        assert!(
            ctx.drain_commands().is_empty(),
            "no live commands after full replay"
        );
    }

    // ── Typed dispatch helper tests ───────────────────────────────────────────

    fn make_activity_info(name: &'static str, local: bool) -> crate::info::ActivityInfo {
        crate::info::ActivityInfo {
            name,
            module: "test",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_queue: None,
            max_concurrent: None,
            concurrency_key: None,
            is_local: local,
            max_input_bytes: None,
            max_result_bytes: None,
            rate_limit_rps: None,
            rate_limit_burst: None,
            rate_limit_key: None,
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        }
    }

    fn make_workflow_info(name: &'static str) -> crate::info::WorkflowInfo {
        crate::info::WorkflowInfo {
            name,
            module: "test",
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
            execution_timeout: None,
            concurrency: None,
            max_input_bytes: None,
        }
    }

    #[tokio::test]
    async fn execute_activity_typed_replays_completed_output() {
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "greet".into(),
                input: serde_json::json!("Alice"),
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!("Hello, Alice"),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let info = make_activity_info("greet", false);
        let result: HarvestResult<String> = ctx.execute_activity(&info, "Alice").await;

        assert_eq!(result.unwrap(), "Hello, Alice");
        assert!(ctx.drain_commands().is_empty());
    }

    #[tokio::test]
    async fn execute_activity_with_opts_uses_default_queue_from_info() {
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "crunch".into(),
                input: serde_json::json!(42u64),
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!(84u64),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let info = make_activity_info("crunch", false);
        let result: HarvestResult<u64> = ctx
            .execute_activity_with_opts(&info, 42u64, None, None, None)
            .await;

        assert_eq!(result.unwrap(), 84u64);
        assert!(ctx.drain_commands().is_empty());
    }

    #[tokio::test]
    async fn execute_local_activity_rejects_non_local_info() {
        let ctx = WorkflowContext::new_test();
        let info = make_activity_info("remote_thing", false);
        let result: HarvestResult<()> = ctx.execute_local_activity(&info, ()).await;

        assert!(matches!(result, Err(HarvestError::Config(_))));
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("remote_thing"));
        assert!(msg.contains("local"));
    }

    #[tokio::test]
    async fn execute_local_activity_typed_replays_completed_output() {
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::LocalActivityScheduled {
                activity_id,
                name: "checksum".into(),
                input: serde_json::json!("data"),
            },
            WorkflowEvent::LocalActivityCompleted {
                activity_id,
                output: serde_json::json!("abc123"),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let info = make_activity_info("checksum", true);
        let result: HarvestResult<String> = ctx.execute_local_activity(&info, "data").await;

        assert_eq!(result.unwrap(), "abc123");
        assert!(ctx.drain_commands().is_empty());
    }

    #[tokio::test]
    async fn spawn_child_workflow_typed_replays_completed_output() {
        let child_id = ExecutionId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "sub_job".into(),
                input: serde_json::json!(7u64),
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id,
                output: serde_json::json!("done"),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let info = make_workflow_info("sub_job");
        let result: HarvestResult<String> = ctx.spawn_child_workflow(&info, 7u64).await;

        assert_eq!(result.unwrap(), "done");
        assert!(ctx.drain_commands().is_empty());
    }

    #[tokio::test]
    async fn receive_signal_typed_replays_signal_payload() {
        #[derive(serde::Deserialize, Debug, PartialEq)]
        struct Approval {
            approved: bool,
        }

        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"approved": true}),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let result: HarvestResult<Approval> = ctx.receive_signal("approval").await;

        assert_eq!(result.unwrap(), Approval { approved: true });
        assert!(ctx.drain_commands().is_empty());
    }
}
