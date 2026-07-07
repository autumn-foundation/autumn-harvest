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
use crate::error::{HarvestError, HarvestResult, NonDeterministicDetails, PayloadKind};
use crate::event::WorkflowEvent;
use crate::query::QueryRegistry;
use crate::replay::{HistoryMatch, HistoryMatcher, PatchMarkerMatch};
use crate::signal_handler::{BoxSignalHandler, SignalHandlerRegistry, invoke_signal_handler};
use crate::types::{
    ActivityExecId, ExecutionId, ExternalActivityToken, ExternalCancelId, ExternalSignalId,
    IdempotencyKey, SessionId, TimerId, UpdateId,
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

/// Default maximum byte length for the `current_details` string (issue #473).
/// Values longer than this cap are truncated to this length on the byte boundary.
pub const DEFAULT_CURRENT_DETAILS_CAP_BYTES: usize = 1024;

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

/// State needed by [`ActivityContext::run_transactional`] to bind user writes
/// to the same Postgres transaction as the `ActivityCompleted` event.
///
/// Attached to a regular (non-local) activity context by the worker before
/// dispatching the activity handler.  `None` on test contexts and local
/// activity contexts; calling `run_transactional` without this state returns
/// a descriptive error.
#[cfg(feature = "db")]
pub struct TransactionalState {
    /// Pool to acquire the transactional connection from.
    pub(crate) pool: ActivityCancellationPool,
    /// Workflow execution that owns this activity invocation.
    pub(crate) exec_id: crate::types::ExecutionId,
    /// Unique ID of this activity invocation attempt.
    pub(crate) activity_id: crate::types::ActivityExecId,
    /// Task queue row ID — used to lock and complete the task atomically.
    pub(crate) task_id: uuid::Uuid,
    /// Maximum serialized result size in bytes (0 = unlimited).  Checked
    /// inside the transaction so an oversized result is caught before
    /// `ActivityCompleted` is committed.
    pub(crate) max_result_bytes: u64,
}
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
        /// Worker session this activity belongs to (issue #606). `Some` when
        /// dispatched through `Session::execute_activity` or as the internal
        /// session-acquire/release activities; `None` for an ordinary
        /// activity. Never affects behavior when `None` — zero change for
        /// existing dispatch.
        session_id: Option<SessionId>,
        /// The session's host worker id, resolved from the session-acquire
        /// activity's recorded output (issue #606). When `Some`, the worker
        /// hard-pins this task's `sticky_worker_id` so it can never fail over
        /// to a different worker, even after the ordinary sticky lease
        /// expires. `None` for a non-session activity.
        session_worker_id: Option<String>,
        /// Per-call `schedule_to_start` override (issue #606). Used
        /// exclusively by the internal `__harvest_session_acquire` dispatch
        /// to bound session acquisition by `SessionOptions::acquisition_timeout`
        /// without adding a schedule-to-start override to the public
        /// `execute_activity` surface. `None` for every ordinary activity
        /// dispatch, which continues to use the activity's registered
        /// `default_schedule_to_start`.
        schedule_to_start_override: Option<std::time::Duration>,
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
    /// Record a deterministic side-effect value (issue #384).
    ///
    /// Emitted by the `WorkflowContext` deterministic primitives — `system_now()`,
    /// `new_uuid()`, `random_*()`, and `side_effect()` — when running live (past
    /// end of history). The worker persists this as a
    /// [`WorkflowEvent::SideEffectRecorded`] so subsequent replays return the
    /// recorded value. Like [`Self::RecordMarker`] it is a bookkeeping command:
    /// it carries no result channel and never drives a suspension.
    RecordSideEffect {
        /// Which built-in helper produced the value.
        kind: crate::event::SideEffectKind,
        /// Author-supplied dedup key for `side_effect()`; `None` for built-ins.
        name: Option<String>,
        /// The recorded JSON value.
        value: Value,
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
    /// Overwrite the `current_details` column on the execution row (issue #473).
    ///
    /// Emitted by [`WorkflowContext::set_current_details`] during live execution.
    /// Suppressed during replay so the DB write is idempotent across worker
    /// restarts. Last-write-wins: the worker takes the **last** `SetCurrentDetails`
    /// command from the drained list and persists only that value.
    /// No `WorkflowEvent` is appended — zero footprint in `harvest_events`.
    ///
    /// `explicit_clear` (issue #593, hardened post-review) is decided from the
    /// **caller's raw, pre-truncation** input: `true` only when the workflow
    /// author literally called `set_current_details("")`. This is deliberately
    /// *not* re-derived from `value.is_empty()`, because a non-empty input can
    /// truncate down to an empty string when `current_details_cap` is `0` (or
    /// smaller than the input's first UTF-8 character) -- that is a capacity
    /// artifact, not an author-intended clear, and must not silently erase an
    /// existing breadcrumb.
    SetCurrentDetails {
        /// The human-readable status string, already capped by the context.
        value: String,
        /// `true` iff the author's original (pre-cap) input was the empty
        /// string -- the only condition that should clear the column.
        explicit_clear: bool,
    },
    /// Spawn a child workflow in detached mode and return its `ExecutionId`
    /// immediately without suspending the parent.
    ///
    /// The worker resolves this command by:
    /// 1. Inserting a new execution row for the child with the recorded
    ///    `parent_close_policy` column set.
    /// 2. Enqueueing the child's first workflow task.
    /// 3. Appending `ChildWorkflowSpawnedDetached` to the **parent's** event
    ///    history so replay can return the same `child_id`.
    ///
    /// Unlike `StartChildWorkflow`, there is no `result_tx` channel — the
    /// parent never suspends waiting for the child's terminal result.
    SpawnDetachedChildWorkflow {
        /// The execution ID of the child. Generated by the context, recorded in
        /// `ChildWorkflowSpawnedDetached`, and returned to the caller.
        child_id: ExecutionId,
        /// Name of the child workflow handler.
        workflow_name: String,
        /// JSON input for the child.
        input: Value,
        /// Policy applied to this child when the parent reaches a terminal state.
        parent_close_policy: crate::types::ParentClosePolicy,
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
        /// Optional exactly-once delivery key, persisted in the
        /// `ExternalSignalRequested` event to dedup the target's signal insert.
        idempotency_key: Option<String>,
    },
    /// Request cancellation of a sibling workflow execution (issue #492).
    RequestCancelExternalWorkflow {
        /// Correlation ID shared across all three history events.
        cancel_id: ExternalCancelId,
        /// Target workflow execution to cancel.
        target: ExecutionId,
        /// Outcome channel: `Ok(())` on delivery (including already-terminal),
        /// `Err(reason_code)` on failure (target unknown after grace window).
        result_tx: oneshot::Sender<Result<(), String>>,
        /// When `true`, `ExternalCancelRequested` is already in history and must
        /// not be appended again (crash-recovery path).
        already_requested: bool,
    },
    /// Durably cancel the losing branches of a resolved `ctx.race()` (issue #600).
    ///
    /// Pushed once per race, in the same drained batch as the race's winner
    /// marker (`RecordMarker { name: "race_winner:{seq}", .. }"`). Bookkeeping:
    /// carries no result channel and never drives a suspension shape by
    /// itself — every suspension/terminal persist path treats it as
    /// bookkeeping alongside `RecordMarker`/`RecordSideEffect`/etc.
    ///
    /// The worker resolves it (in the *same* transaction that persists the
    /// winner marker, so a crash between the two can never leak a row) by:
    /// - `activities`: atomically transitioning each still-open task row to
    ///   `CANCELLED` and appending a synthetic `ActivityFailed { error: "lost
    ///   race to a sibling branch", .. }` for it (reusing the existing event
    ///   variant — no new `WorkflowEvent`), so every future replay resolves
    ///   that branch to a terminal instead of looping forever on
    ///   `ActivityInProgress`. Already-terminal activities (a genuine
    ///   completion raced the cancellation) are left untouched.
    /// - `children`: cancelling via the existing `cancel_workflow_execution_collect`
    ///   primitive (issue #492's machinery), which already appends the
    ///   necessary terminal event to the parent's history and is idempotent
    ///   against an already-terminal child.
    /// - `timers`: deleting the still-pending `harvest_timers` row.
    CancelRaceLosers {
        /// Activity execution IDs of losing activity branches still open
        /// (`PENDING`/`RUNNING`) at race-resolution time.
        activities: Vec<ActivityExecId>,
        /// Execution IDs of losing child-workflow branches to cancel.
        children: Vec<ExecutionId>,
        /// Timer IDs of losing timer branches to remove from `harvest_timers`.
        timers: Vec<TimerId>,
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
            Self::RecordSideEffect { kind, name, value } => f
                .debug_struct("RecordSideEffect")
                .field("kind", kind)
                .field("name", name)
                .field("value", value)
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
            Self::SetCurrentDetails {
                value,
                explicit_clear,
            } => f
                .debug_struct("SetCurrentDetails")
                .field("value", value)
                .field("explicit_clear", explicit_clear)
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
            Self::RequestCancelExternalWorkflow {
                cancel_id,
                target,
                already_requested,
                ..
            } => f
                .debug_struct("RequestCancelExternalWorkflow")
                .field("cancel_id", cancel_id)
                .field("target", target)
                .field("already_requested", already_requested)
                .finish_non_exhaustive(),
            Self::CancelRaceLosers {
                activities,
                children,
                timers,
            } => f
                .debug_struct("CancelRaceLosers")
                .field("activities", activities)
                .field("children", children)
                .field("timers", timers)
                .finish(),
            Self::SpawnDetachedChildWorkflow {
                child_id,
                workflow_name,
                parent_close_policy,
                ..
            } => f
                .debug_struct("SpawnDetachedChildWorkflow")
                .field("child_id", child_id)
                .field("workflow_name", workflow_name)
                .field("parent_close_policy", parent_close_policy)
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
// ctx.race() — deterministic race/select primitive (issue #600)
// ---------------------------------------------------------------------------

/// One branch of a [`RaceBuilder`] (issue #600).
enum RaceBranchKind {
    Activity {
        name: String,
        input: Value,
        queue: String,
        retry: Option<crate::policy::RetryPolicy>,
        start_to_close: Option<std::time::Duration>,
    },
    ChildWorkflow {
        workflow_name: String,
        input: Value,
    },
    /// Paired with exactly one [`RaceBranchKind::Signal`] branch and no other
    /// branches — see [`RaceBuilder::run`] for the supported-shape rules.
    Timer {
        duration_secs: u64,
    },
    /// Paired with exactly one [`RaceBranchKind::Timer`] branch and no other
    /// branches — see [`RaceBuilder::run`] for the supported-shape rules.
    Signal {
        signal_name: String,
    },
}

struct RaceBranch {
    kind: RaceBranchKind,
    label: Option<String>,
}

/// A still-open race branch discovered during `WorkflowContext::race_impl`'s
/// history check, carrying whatever durable resource id it will need if it
/// loses the race and must be cancelled.
struct RaceDispatch {
    /// Index into the original branch list.
    index: usize,
    activity_id: Option<ActivityExecId>,
    child_id: Option<ExecutionId>,
    /// `true` when this branch has never been dispatched before (its command
    /// must carry full scheduling parameters); `false` when it is already in
    /// history and only needs a `WaitForActivity` re-park.
    is_new: bool,
}

/// The winning branch of a resolved [`WorkflowContext::race`] (issue #600).
#[derive(Debug, Clone)]
pub struct RaceWinner {
    /// Index of the winning branch, in the order branches were added to the
    /// [`RaceBuilder`] -- **except** for the timer+signal shape, where this
    /// is a fixed role-based index (timer = `0`, signal = `1`) independent of
    /// `.timer()`/`.signal()` call order; see [`RaceBuilder`]'s docs.
    pub index: usize,
    /// The label attached via [`RaceBuilder::label`], if any.
    pub label: Option<String>,
    /// The winning branch's value: the activity/child-workflow output, the
    /// signal payload, or `Value::Null` for a timer branch.
    pub value: Value,
}

impl RaceWinner {
    /// Deserialize the winning branch's value into `O`.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Serialization`] if `O` cannot be deserialized
    /// from the winning branch's JSON value.
    pub fn decode<O: serde::de::DeserializeOwned>(&self) -> HarvestResult<O> {
        serde_json::from_value(self.value.clone()).map_err(HarvestError::Serialization)
    }
}

/// Builder for [`WorkflowContext::race`] (issue #600): await the first of
/// several concurrent ctx-managed awaitables and durably cancel the losers.
///
/// # Supported shapes
///
/// - **N activity branches** (`N >= 1`, via [`Self::activity`]/[`Self::activity_raw`]):
///   races activities; every losing activity is durably cancelled — its task
///   row is transitioned out of `PENDING`/`RUNNING` and a synthetic
///   `ActivityFailed { error: "lost race to a sibling branch" }` terminal is
///   recorded (reusing the existing event variant) so replay always resolves
///   that branch to a terminal instead of looping on `ActivityInProgress`.
/// - **N child-workflow branches** (`N >= 1`, via [`Self::child_workflow`]):
///   races child workflows; every losing child is durably cancelled via the
///   same `cancel_workflow_execution_collect` primitive the external-cancel
///   feature uses (issue #492).
/// - **Exactly one [`Self::timer`] + exactly one [`Self::signal`]**: a thin
///   wrapper around the fully-tested
///   [`WorkflowContext::receive_signal_timeout`]/`wait_for_signal_timeout`
///   primitive (issue #476). A losing signal simply stays observable to a
///   later signal wait (nothing to durably cancel); a losing timer's
///   still-armed durable timer row is removed by the worker exactly as it is
///   today. Unlike the two homogeneous shapes above, [`RaceWinner::index`]
///   for this shape is a **fixed, role-based** value (the timer branch is
///   always `0`, the signal branch is always `1`) rather than each branch's
///   position in the builder chain — reordering `.timer()`/`.signal()` calls
///   between deploys can never flip which index an in-flight execution
///   observes, because the underlying winner determination is itself decided
///   purely by recorded history order, independent of call order.
///
/// Mixing branch kinds outside of these three shapes (e.g. racing an activity
/// against a timer in the same call) returns [`HarvestError::Config`] from
/// [`Self::run`] — the worker's suspension-persistence layer does not yet
/// support a fully heterogeneous mixed-command batch (see the crate-level
/// determinism guide, HVG010, for the rationale). Bound an individual
/// activity with its own `start_to_close`/`schedule_to_close` timeout, or use
/// `receive_signal_timeout` directly, to express a deadline-bounded branch
/// instead.
///
/// # Determinism contract
///
/// The winning branch is recorded via the existing `MarkerRecorded` event
/// (mirroring `execute_activity_fan_out`'s marker — no new `WorkflowEvent`
/// variant): an "open" marker (`race:{seq}`) fixes the branch count on first
/// dispatch, and a "winner" marker (`race_winner:{seq}`) fixes the winning
/// index once decided. Every subsequent replay of the same history *verifies*
/// — rather than re-derives — the previously recorded winner, so a code
/// change that would flip the outcome is rejected as
/// [`HarvestError::NonDeterministic`] instead of silently diverging.
///
/// If multiple branches already have a terminal result recorded in history by
/// the time the race is (re-)evaluated (e.g. two activities both finished
/// before the workflow task got a chance to notice), the **lowest-indexed**
/// resolved branch wins — a documented, deterministic tie-break, mirroring
/// `receive_signal_timeout`'s signal-first tie-break for its own in-cycle
/// case.
///
/// # Example
///
/// Racing two activities and durably cancelling the loser, in five lines:
///
/// ```rust,ignore
/// let winner = ctx.race()
///     .activity(&fetch_primary_info(), input.clone())
///     .activity(&fetch_fallback_info(), input)
///     .run().await?;
/// let quote: Quote = winner.decode()?;
/// ```
pub struct RaceBuilder<'a> {
    ctx: &'a WorkflowContext,
    branches: Vec<RaceBranch>,
    /// First serialization failure encountered while building branches
    /// (`.activity`/`.child_workflow`), surfaced by [`Self::run`] instead of
    /// silently degrading the branch's input to `Value::Null`.
    pending_error: Option<HarvestError>,
}

impl RaceBuilder<'_> {
    fn fail(mut self, err: HarvestError) -> Self {
        if self.pending_error.is_none() {
            self.pending_error = Some(err);
        }
        self
    }

    /// Add a typed activity branch, using `info`'s registered queue/retry/timeout defaults.
    #[must_use]
    pub fn activity<I: serde::Serialize>(self, info: &crate::info::ActivityInfo, input: I) -> Self {
        match serde_json::to_value(input) {
            Ok(json_input) => {
                let queue = info.default_queue.unwrap_or("default").to_string();
                self.push(RaceBranchKind::Activity {
                    name: info.name.to_string(),
                    input: json_input,
                    queue,
                    retry: info.default_retry_policy.clone(),
                    start_to_close: info.default_start_to_close,
                })
            }
            Err(err) => self.fail(err.into()),
        }
    }

    /// Add an untyped activity branch by name.
    #[must_use]
    pub fn activity_raw(self, name: &str, input: Value, queue: &str) -> Self {
        self.push(RaceBranchKind::Activity {
            name: name.to_string(),
            input,
            queue: queue.to_string(),
            retry: None,
            start_to_close: None,
        })
    }

    /// Add a child-workflow branch.
    #[must_use]
    pub fn child_workflow<I: serde::Serialize>(
        self,
        info: &crate::info::WorkflowInfo,
        input: I,
    ) -> Self {
        match serde_json::to_value(input) {
            Ok(json_input) => self.child_workflow_raw(info.name, json_input),
            Err(err) => self.fail(err.into()),
        }
    }

    /// Add an untyped child-workflow branch by name.
    #[must_use]
    pub fn child_workflow_raw(self, workflow_name: &str, input: Value) -> Self {
        self.push(RaceBranchKind::ChildWorkflow {
            workflow_name: workflow_name.to_string(),
            input,
        })
    }

    /// Add a durable-timer branch. Must be paired with exactly one
    /// [`Self::signal`] branch and no other branches — see [`Self::run`].
    /// `timeout` is rounded **up** to whole seconds.
    #[must_use]
    pub fn timer(self, timeout: std::time::Duration) -> Self {
        let duration_secs = timeout
            .as_secs()
            .saturating_add(u64::from(timeout.subsec_nanos() > 0));
        self.push(RaceBranchKind::Timer { duration_secs })
    }

    /// Add a signal branch. Must be paired with exactly one [`Self::timer`]
    /// branch and no other branches — see [`Self::run`].
    #[must_use]
    pub fn signal(self, signal_name: &str) -> Self {
        self.push(RaceBranchKind::Signal {
            signal_name: signal_name.to_string(),
        })
    }

    /// Label the most recently added branch (surfaced on [`RaceWinner::label`]).
    #[must_use]
    pub fn label(mut self, label: &str) -> Self {
        if let Some(last) = self.branches.last_mut() {
            last.label = Some(label.to_string());
        }
        self
    }

    fn push(mut self, kind: RaceBranchKind) -> Self {
        self.branches.push(RaceBranch { kind, label: None });
        self
    }

    /// Resolve the race: await the first branch to complete and durably
    /// cancel the losers.
    ///
    /// # Errors
    ///
    /// - [`HarvestError::Serialization`] if a typed [`Self::activity`] or
    ///   [`Self::child_workflow`] branch's input could not be serialized
    ///   (surfaced here rather than silently running the branch with a
    ///   `null` input).
    /// - [`HarvestError::Config`] if zero branches were added, or if the
    ///   branch kinds don't form one of the three supported shapes (see the
    ///   type-level docs).
    /// - [`HarvestError::Cancelled`] if the workflow has been cancelled.
    /// - [`HarvestError::NonDeterministic`] if replay disagrees with a
    ///   previously recorded winner or branch count.
    /// - Whatever error the winning branch itself failed with.
    pub async fn run(self) -> HarvestResult<RaceWinner> {
        if let Some(err) = self.pending_error {
            return Err(err);
        }
        self.ctx.race_impl(self.branches).await
    }
}

/// Live-mode future for `ctx.race()` (issue #600): polls every still-pending
/// branch receiver in ascending index order on each poll, so a live in-cycle
/// tie (multiple receivers ready in the same poll) always resolves to the
/// lowest-indexed branch — the same deterministic tie-break `settle_race`
/// documents for the history-derived case. A dropped channel is removed and
/// treated as "will never resolve"; only when every receiver is gone does the
/// future resolve to [`HarvestError::Cancelled`].
struct RaceFirstFut {
    receivers: Vec<(usize, oneshot::Receiver<Result<Value, String>>)>,
}

impl std::future::Future for RaceFirstFut {
    type Output = HarvestResult<(usize, Result<Value, String>)>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        let mut i = 0;
        while i < this.receivers.len() {
            match std::pin::Pin::new(&mut this.receivers[i].1).poll(cx) {
                std::task::Poll::Ready(Ok(result)) => {
                    let (index, _) = this.receivers.remove(i);
                    return std::task::Poll::Ready(Ok((index, result)));
                }
                std::task::Poll::Ready(Err(_)) => {
                    this.receivers.remove(i);
                }
                std::task::Poll::Pending => {
                    i += 1;
                }
            }
        }
        if this.receivers.is_empty() {
            std::task::Poll::Ready(Err(HarvestError::Cancelled(
                "race: all branch channels dropped".to_string(),
            )))
        } else {
            std::task::Poll::Pending
        }
    }
}

// ---------------------------------------------------------------------------
// Search-attribute validation helpers
// ---------------------------------------------------------------------------

const SEARCH_ATTR_KEY_MAX_LEN: usize = 64;

const RESERVED_SEARCH_ATTR_KEYS: &[&str] = &[
    "exec_id",
    "workflow_name",
    "shard_id",
    "status",
    "run_id",
    // The six replay-non-determinism diagnostic keys (issue #603): reserved so
    // a workflow author's own business attribute can never collide with, and
    // be silently deleted by, `nd_search_attrs_clear_patch`'s recovery clear.
    "failure_cause",
    "event_index",
    "expected",
    "actual",
    "workflow_type",
    "build_id",
];

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
// WorkflowLogger  (issue #379)
// ---------------------------------------------------------------------------

/// Replay-aware logger scoped to a single workflow execution.
///
/// Obtained via [`WorkflowContext::logger`]. Suppresses all `tracing` events
/// while the executor is replaying recorded history (`is_replaying() == true`),
/// so that each `log_*` call fires at most once per execution regardless of
/// how many replay cycles occur.
///
/// Every emitted event carries the following structured fields:
/// - `workflow_id` — the business-level workflow identifier
/// - `execution_id` — the unique run UUID
/// - `workflow_type` — the registered workflow function name
/// - `replay = false` — confirms the event was not emitted during replay
pub struct WorkflowLogger<'ctx> {
    ctx: &'ctx WorkflowContext,
}

impl WorkflowLogger<'_> {
    /// Emit an INFO-level event. No-op when `ctx.is_replaying()` is `true`.
    pub fn info(&self, message: &str) {
        if !self.ctx.is_replaying() {
            tracing::info!(
                target: "autumn_harvest::context",
                workflow_id = self.ctx.workflow_id(),
                execution_id = %self.ctx.execution_id(),
                workflow_type = self.ctx.workflow_type(),
                replay = false,
                "{message}"
            );
        }
    }

    /// Emit a WARN-level event. No-op when `ctx.is_replaying()` is `true`.
    pub fn warn(&self, message: &str) {
        if !self.ctx.is_replaying() {
            tracing::warn!(
                target: "autumn_harvest::context",
                workflow_id = self.ctx.workflow_id(),
                execution_id = %self.ctx.execution_id(),
                workflow_type = self.ctx.workflow_type(),
                replay = false,
                "{message}"
            );
        }
    }

    /// Emit an ERROR-level event. No-op when `ctx.is_replaying()` is `true`.
    pub fn error(&self, message: &str) {
        if !self.ctx.is_replaying() {
            tracing::error!(
                target: "autumn_harvest::context",
                workflow_id = self.ctx.workflow_id(),
                execution_id = %self.ctx.execution_id(),
                workflow_type = self.ctx.workflow_type(),
                replay = false,
                "{message}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Worker sessions (issue #606)
// ---------------------------------------------------------------------------

/// Reserved activity name for a worker session's acquisition step.
///
/// Dispatched by [`WorkflowContext::create_session`] as an ordinary activity
/// (reusing the full replay/suspend/persist machinery) so its recorded
/// `ActivityCompleted { output }` — the host worker id — is what gives a
/// session's physical binding replay determinism, with no engine-level
/// special-casing in `replay.rs`. The worker intercepts this reserved name
/// before regular handler dispatch (see `process_activity_task`).
pub(crate) const SESSION_ACQUIRE_ACTIVITY_NAME: &str = "__harvest_session_acquire";

/// Reserved activity name for a worker session's release step
/// ([`Session::complete`]). Hard-pinned to the session's host worker exactly
/// like a member activity, so only the host frees its own in-process slot.
pub(crate) const SESSION_RELEASE_ACTIVITY_NAME: &str = "__harvest_session_release";

/// Returns `true` when `name` is one of the two reserved worker-session
/// internal activity names (issue #606): `__harvest_session_acquire` or
/// `__harvest_session_release`.
///
/// Both are always registered in `HandlerRegistry::activities` (so the
/// enqueue-time handler lookup never hard-fails on them) with
/// `default_queue: None`, but they are dispatched on the *caller-supplied*
/// session queue (`SessionOptions::queue`) at the point `create_session`/
/// `Session::complete` is called, never on a fixed queue of their own. A
/// preflight/readiness check deriving "queues this deployment needs a
/// worker listening on" from every registered activity's
/// `default_queue.unwrap_or("default")` must exclude these two names —
/// otherwise a deployment with no session-based workflow at all is
/// spuriously reported as requiring a worker on `"default"`.
#[must_use]
pub fn is_reserved_session_activity_name(name: &str) -> bool {
    name == SESSION_ACQUIRE_ACTIVITY_NAME || name == SESSION_RELEASE_ACTIVITY_NAME
}

/// Default session acquisition timeout.
///
/// Bounds how long [`WorkflowContext::create_session`] waits for a worker
/// with a free session slot before failing with
/// [`HarvestError::SessionAcquireTimeout`]. Override via
/// [`SessionOptions::with_acquisition_timeout`].
pub const DEFAULT_SESSION_ACQUISITION_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(30);

/// Saturating `Duration` → milliseconds conversion for error reporting
/// (a multi-year timeout would otherwise overflow `u64` millis on `as_millis`).
fn duration_to_millis_saturating(d: std::time::Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Options for [`WorkflowContext::create_session`] (issue #606).
///
/// # Example
///
/// ```rust,ignore
/// let session = ctx.create_session(
///     SessionOptions::new("gpu-workers").with_acquisition_timeout(Duration::from_secs(60))
/// ).await?;
/// ```
#[derive(Debug, Clone)]
pub struct SessionOptions {
    /// The task queue the session-acquire activity is scheduled on. Member
    /// activities dispatched via [`Session::execute_activity`] default to
    /// this same queue unless their [`crate::info::ActivityInfo`] specifies
    /// its own.
    pub queue: String,
    /// Maximum time to wait for a worker with a free session slot
    /// (`WorkerConfig::max_concurrent_sessions`) before failing with
    /// [`HarvestError::SessionAcquireTimeout`]. Defaults to
    /// [`DEFAULT_SESSION_ACQUISITION_TIMEOUT`].
    pub acquisition_timeout: std::time::Duration,
}

impl SessionOptions {
    /// Session options targeting `queue`, with the default acquisition
    /// timeout ([`DEFAULT_SESSION_ACQUISITION_TIMEOUT`]).
    #[must_use]
    pub fn new(queue: impl Into<String>) -> Self {
        Self {
            queue: queue.into(),
            acquisition_timeout: DEFAULT_SESSION_ACQUISITION_TIMEOUT,
        }
    }

    /// Override the acquisition timeout.
    #[must_use]
    pub const fn with_acquisition_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.acquisition_timeout = timeout;
        self
    }
}

impl Default for SessionOptions {
    /// Targets the `"default"` queue with [`DEFAULT_SESSION_ACQUISITION_TIMEOUT`].
    fn default() -> Self {
        Self::new("default")
    }
}

/// A handle to an open worker session (issue #606) returned by
/// [`WorkflowContext::create_session`].
///
/// Activities dispatched through [`Self::execute_activity`] /
/// [`Self::execute_activity_raw`] are guaranteed to run on the single worker
/// that acquired the session, for its entire lifetime — see the "Fan-out vs
/// worker sessions" decision matrix in the crate docs for when to reach for
/// a session instead of a plain activity, a local activity, or claim-check
/// payload offloading (issue #524).
///
/// Borrows the owning [`WorkflowContext`] for its lifetime, mirroring
/// [`RaceBuilder`]'s shape — a session cannot outlive the workflow function
/// invocation that created it.
pub struct Session<'a> {
    ctx: &'a WorkflowContext,
    id: SessionId,
    host_worker_id: String,
    queue: String,
}

impl Session<'_> {
    /// This session's deterministic identity.
    #[must_use]
    pub const fn id(&self) -> SessionId {
        self.id
    }

    /// The worker id hosting this session.
    #[must_use]
    pub fn host_worker_id(&self) -> &str {
        &self.host_worker_id
    }

    /// Execute a typed activity through this session, hard-pinned to the
    /// session's host worker.
    ///
    /// Uses `info`'s registered retry policy and start-to-close timeout, and
    /// its registered queue if set — otherwise falls back to the session's
    /// own queue (the same default `execute_activity` uses relative to
    /// `"default"`).
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Serialization`] if `input`/the result cannot
    /// be (de)serialized. Returns [`HarvestError::SessionBroken`] if the
    /// session's host worker died or drained before this activity completed.
    /// Propagates all errors from [`Self::execute_activity_raw`].
    pub async fn execute_activity<I, O>(
        &self,
        info: &crate::info::ActivityInfo,
        input: I,
    ) -> HarvestResult<O>
    where
        I: serde::Serialize,
        O: serde::de::DeserializeOwned,
    {
        let json_input = serde_json::to_value(input).map_err(HarvestError::Serialization)?;
        let queue = info.default_queue.unwrap_or(self.queue.as_str());
        let raw = self
            .ctx
            .execute_activity_raw_full(
                info.name,
                json_input,
                queue,
                info.default_retry_policy.clone(),
                info.default_start_to_close,
                Some(self.id),
                Some(self.host_worker_id.clone()),
                None,
            )
            .await
            .map_err(|err| self.broken_session_error(err))?;
        serde_json::from_value(raw).map_err(HarvestError::Serialization)
    }

    /// Execute an untyped activity by name through this session, hard-pinned
    /// to the session's host worker.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::SessionBroken`] if the session's host worker
    /// died or drained before this activity completed. Propagates all other
    /// errors from the underlying activity dispatch.
    pub async fn execute_activity_raw(
        &self,
        name: &str,
        input: Value,
        queue: &str,
    ) -> HarvestResult<Value> {
        self.ctx
            .execute_activity_raw_full(
                name,
                input,
                queue,
                None,
                None,
                Some(self.id),
                Some(self.host_worker_id.clone()),
                None,
            )
            .await
            .map_err(|err| self.broken_session_error(err))
    }

    /// Rewrites a `SessionBroken`-typed `ActivityFailed` into
    /// [`HarvestError::SessionBroken`], mirroring
    /// [`WorkflowContext::create_session`]'s identical mapping for the
    /// acquire step. Any other error passes through unchanged.
    ///
    /// This is the actually-reachable place a `SessionBroken` outcome is
    /// first observed (member-activity or `complete()` failure, unlike
    /// `create_session`'s own defensive mapping, which can never fire since
    /// the acquire task never carries `session_id`), so the
    /// `harvest.session.acquisition{outcome="broken"}` metric is emitted
    /// here -- guarded on `!is_replaying()` so a replay of the same recorded
    /// failure doesn't re-increment it.
    fn broken_session_error(&self, err: HarvestError) -> HarvestError {
        match err {
            HarvestError::ActivityFailed {
                error_type, source, ..
            } if error_type == crate::failure::ERROR_TYPE_SESSION_BROKEN => {
                if !self.ctx.is_replaying() {
                    self.ctx.metrics.record_session_acquisition(
                        &self.queue,
                        crate::telemetry::SessionAcquisitionOutcome::Broken,
                    );
                }
                HarvestError::SessionBroken {
                    session_id: self.id,
                    reason: source.to_string(),
                }
            }
            other => other,
        }
    }

    /// End the session, releasing the host worker's session slot.
    ///
    /// Dispatches the internal session-release activity, hard-pinned to the
    /// host (only the host worker can free its own in-process slot).
    /// Idempotent from the workflow's perspective — like any other activity,
    /// a crash-and-replay before this completes safely re-dispatches once.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::SessionBroken`] if the host worker died or
    /// drained before release could complete — the slot is still reclaimed
    /// by the broken-session scanner in that case.
    pub async fn complete(self) -> HarvestResult<()> {
        self.ctx
            .execute_activity_raw_full(
                SESSION_RELEASE_ACTIVITY_NAME,
                Value::from(self.id.to_string()),
                &self.queue,
                None,
                None,
                Some(self.id),
                Some(self.host_worker_id.clone()),
                None,
            )
            .await
            .map_err(|err| self.broken_session_error(err))?;
        Ok(())
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
    /// Monotonically increasing counter for naming fan-out count markers.
    /// Each `execute_activity_fan_out*` call increments this once so each
    /// fan-out group has a stable, unique marker name across replays.
    fan_out_seq: Mutex<u32>,
    /// Monotonically increasing counter for naming signal-timeout race timers
    /// (issue #476). Each `wait_for_signal_timeout` call increments this once
    /// so each race has a stable, unique timer ID across replays.
    signal_timeout_seq: Mutex<u32>,
    /// Monotonically increasing counter for naming `ctx.race()` markers
    /// (issue #600). Each `race()` call increments this once so each race has
    /// stable, unique `race:{seq}` / `race_winner:{seq}` marker names across
    /// replays, mirroring `fan_out_seq`.
    race_seq: Mutex<u32>,
    /// Monotonically increasing counter for naming worker-session identity
    /// markers (issue #606). Each `create_session()` call increments this once
    /// so each session has a stable, unique `session:{seq}` marker name across
    /// replays, mirroring `fan_out_seq`/`race_seq`.
    session_seq: Mutex<u32>,
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
    /// In-memory push-based signal handlers (issue #546), not persisted to
    /// history. Registration is idempotent -- the first registration wins on
    /// each replay, mirroring `update_registry`.
    signal_registry: Mutex<SignalHandlerRegistry>,
    /// Cancellation reason captured from a `WorkflowCancelled` event in history,
    /// if any. When set, `is_cancelled()` returns true and `check_cancellation()`
    /// yields [`HarvestError::Cancelled`]. Cooperative: the workflow function is
    /// expected to consult these at strategic points to run cleanup logic.
    cancellation_reason: Option<String>,
    /// When `true`, activity and local-activity dispatch compares the input
    /// payload against what was recorded in history, in addition to the name.
    /// Set by the `WorkflowReplayer` to detect non-deterministic input changes.
    strict_replay: bool,
    /// When `true`, we are replaying a running workflow execution as a deploy-time
    /// canary, allowing it to suspend at the end of its history without throwing
    /// non-determinism errors.
    canary_mode: bool,
    // ── Payload size caps (issue #252) ────────────────────────────────
    /// Logical workflow type name for use in `PayloadTooLarge` errors.
    /// Empty string when not known (legacy contexts, update handlers).
    workflow_name: String,
    /// Business-level workflow identifier (e.g. "subscription-123").
    /// Empty when not known (legacy contexts, testing, update handlers).
    /// Set by the worker via `with_workflow_id` before executing the handler.
    workflow_id: String,
    /// The build ID of the worker executing this workflow.
    build_id: Option<String>,
    /// In-memory holder of structured non-determinism details.
    nd_details: Mutex<Option<NonDeterministicDetails>>,
    /// Global cap on activity input payloads (bytes). Checked at schedule time.
    payload_max_activity_input: u64,
    /// Global cap on workflow/child-workflow input payloads (bytes).
    payload_max_workflow_input: u64,
    /// Global cap on `side_effect` value payloads (bytes).
    /// Uses the workflow-input cap as a reasonable default.
    payload_max_side_effect: u64,
    /// Global cap on signal payloads sent via `signal_external_workflow`.
    payload_max_signal: u64,
    /// Offload threshold (bytes) when a [`PayloadStore`](crate::payload_store::PayloadStore)
    /// is registered (issue #524). `Some(t)` means a payload-bearing field larger
    /// than `t` will be offloaded into a tiny reference envelope, so the #252
    /// size cap is not tripped by it. `None` means no store registered — caps are
    /// enforced exactly as before.
    payload_offload_threshold: Option<u64>,
    /// Per-activity input cap overrides: `activity_name → max_bytes`.
    /// When an entry exists, the effective cap is `max(global, override)`.
    activity_input_cap_overrides: HashMap<String, u64>,
    /// First non-determinism error observed by an infallible deterministic
    /// primitive (`system_now`, `new_uuid`, `random_*`). Those helpers return a
    /// plain value (not a `Result`), so they cannot surface a divergence to the
    /// caller directly. They record the first divergence here and the executor
    /// converts the workflow outcome to `Failed` after the handler returns, so
    /// the [`WorkflowReplayer`](crate::testing::WorkflowReplayer) reports a
    /// structured `SideEffectDrift` non-determinism rather than a silent pass
    /// (issue #384).
    deferred_nd_error: Mutex<Option<String>>,
    /// Maximum byte length for `current_details` strings (issue #473). Values
    /// longer than this cap are truncated to the cap boundary on a UTF-8
    /// character boundary. Configurable via `HarvestBuilder::with_current_details_cap`.
    current_details_cap: usize,
    /// Ambient string key-value context attached at workflow start and propagated
    /// automatically to all activities and child workflows (issue #481).
    /// Immutable after construction; read via `header()` / `headers()`.
    context_headers: std::sync::Arc<HashMap<String, String>>,
    /// Frozen output of the most recent prior COMPLETED run of the same schedule (issue #488).
    /// `None` for manual starts, first scheduled run, or when no prior run succeeded.
    /// Resolved once at workflow start and frozen into the `WorkflowStarted` event.
    last_completion_result: Option<serde_json::Value>,
    /// Frozen error from the most recent terminal run if it ended `FAILED` or `TIMED_OUT` (issue #488).
    /// `None` when the most recent terminal run `COMPLETED` (recovery) or for manual starts.
    last_error: Option<String>,
    /// Nominal scheduled fire-time (logical slot) frozen in `WorkflowStarted` (issue #508).
    /// `Some` for scheduled / backfilled / caught-up runs; `None` for manual/ad-hoc starts
    /// and pre-#508 histories (which deserialize the absent field to `None`).
    scheduled_time: Option<DateTime<Utc>>,
    /// Total virtual seconds elapsed from durable timers (issue #526).
    /// `None` = production behavior (`ctx.now()` always returns `start_time`).
    /// `Some` = test harness advancing-clock mode; incremented each time a
    /// durable timer resolves from history so `ctx.now()` reflects virtual elapsed time.
    #[cfg(any(test, feature = "testing"))]
    timer_clock_elapsed_secs: Option<std::sync::atomic::AtomicU64>,
    /// Metrics recorder for user-emitted custom business metrics (issue #532).
    /// Defaults to [`NoOpMetrics`](crate::telemetry::NoOpMetrics) when the
    /// worker has no telemetry configured.  Workflow metrics are replay-safe:
    /// [`metrics()`](Self::metrics) returns a suppressed handle while
    /// `is_replaying()` is `true`.
    metrics: std::sync::Arc<dyn crate::telemetry::MetricsRecorder>,
}

impl WorkflowContext {
    // ── Internal Helpers ──────────────────────────────────────────────────

    fn nd_error(
        &self,
        reason: String,
        event_index: Option<i32>,
        expected: Option<String>,
        actual: Option<String>,
    ) -> HarvestError {
        let details = NonDeterministicDetails {
            event_index,
            expected: expected.clone(),
            actual: actual.clone(),
            workflow_type: Some(self.workflow_name.clone()),
            build_id: self.build_id.clone(),
        };
        if let Ok(mut slot) = self.nd_details.lock() {
            slot.get_or_insert(details);
        }
        HarvestError::non_deterministic(
            reason,
            event_index,
            expected,
            actual,
            Some(self.workflow_name.clone()),
            self.build_id.clone(),
        )
    }

    fn check_strict_replay_no_match(&self, actual_event: &str) -> HarvestResult<()> {
        if self.strict_replay {
            if self.canary_mode && self.match_history(|m| m.position() >= m.len()) {
                return Ok(());
            }
            return Err(self.nd_error(
                format!("early completion mismatch: expected <end of history>, got {actual_event}"),
                self.match_history(|m| i32::try_from(m.position()).ok()),
                None,
                None,
            ));
        }
        Ok(())
    }

    fn match_history<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut HistoryMatcher) -> R,
    {
        let result = {
            let mut matcher = self.matcher.lock().expect("matcher lock poisoned");
            f(&mut matcher)
        };
        self.pump_signal_handlers();
        result
    }

    /// Dispatches every push-based signal handler whose target signal has
    /// newly become claimable (issue #546 post-ship hardening).
    ///
    /// Runs after every [`match_history`](Self::match_history) call so a
    /// handler fires exactly when the workflow's own code-driven cursor
    /// progression passes its recorded position -- never ahead of it. An
    /// eager, cursor-independent full-history scan (the original
    /// `drain_signal_events`) could otherwise dispatch a handler for a
    /// signal recorded *after* an activity or timer the workflow hadn't
    /// reached yet in this replay cycle, silently reordering observable side
    /// effects relative to history. [`HistoryMatcher::claim_pending_signal`]
    /// only inspects signals already drained into `pending_signals` by the
    /// same cursor-bound sweep every other `match_*` call opens with
    /// (`prepare_match`), so it can never reach ahead of wherever the
    /// workflow's code has actually driven the matcher so far.
    ///
    /// Claims across all registered handler names are collected and sorted
    /// by event index *before* any dispatch, so two differently-named
    /// handlers fire in true historical order relative to each other, not
    /// just self-consistently within one name -- this is why registration
    /// itself does not dispatch inline (see
    /// [`register_and_dispatch_signal_handler`](Self::register_and_dispatch_signal_handler)):
    /// a pump triggered from a single registration call could only ever see
    /// the handlers registered so far, not ones about to register on the
    /// next line.
    fn pump_signal_handlers(&self) {
        let names = self
            .signal_registry
            .lock()
            .expect("signal_registry lock poisoned")
            .list_names();
        if names.is_empty() {
            return;
        }

        let mut claims: Vec<(usize, String, Value)> = Vec::new();
        {
            let mut matcher = self.matcher.lock().expect("matcher lock poisoned");
            for name in &names {
                for (idx, payload) in matcher.claim_pending_signal(name) {
                    claims.push((idx, name.clone(), payload));
                }
            }
        }
        if claims.is_empty() {
            return;
        }
        claims.sort_by_key(|(idx, ..)| *idx);

        for (_, name, payload) in claims {
            let handler = self
                .signal_registry
                .lock()
                .expect("signal_registry lock poisoned")
                .get(&name);
            if let Some(handler) = handler {
                invoke_signal_handler(&handler, &name, payload);
            }
        }
    }

    /// Forces one [`pump_signal_handlers`](Self::pump_signal_handlers) sweep
    /// via [`match_history`](Self::match_history)'s post-hook.
    ///
    /// Every other trigger for the pump is a byproduct of some other
    /// cursor-advancing call (an activity/timer/signal match). A workflow
    /// cycle that registers a handler and then does nothing else that
    /// touches history before completing would otherwise never flush a
    /// signal already recorded and claimable at that point. The executor
    /// calls this once per cycle, right after the handler function returns
    /// -- and, on the strict/canary replay paths, *before* the subsequent
    /// `history_has_unconsumed_events()` check, so a signal a registered
    /// handler was going to claim is never mistaken for genuinely unconsumed
    /// history (issue #546 post-ship hardening). A no-op when no handlers
    /// are registered (the overwhelmingly common case).
    pub(crate) fn flush_pending_signal_handlers(&self) {
        self.match_history(|_| ());
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

    #[must_use]
    pub fn for_replay_with_state_and_history_policy(
        exec_id: ExecutionId,
        events: Vec<WorkflowEvent>,
        state: SharedState,
        history_policy: WorkflowHistoryPolicy,
    ) -> Self {
        // Extract start_time, carryover, and scheduled slot from WorkflowStarted (first event).
        let (start_time, last_completion_result, last_error, scheduled_time) = events
            .first()
            .and_then(|e| match e {
                WorkflowEvent::WorkflowStarted {
                    timestamp,
                    last_completion_result,
                    last_error,
                    scheduled_time,
                    ..
                } => Some((
                    *timestamp,
                    last_completion_result.clone(),
                    last_error.clone(),
                    *scheduled_time,
                )),
                _ => None,
            })
            .unwrap_or_else(|| (Utc::now(), None, None, None));

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
            fan_out_seq: Mutex::new(0),
            signal_timeout_seq: Mutex::new(0),
            race_seq: Mutex::new(0),
            session_seq: Mutex::new(0),
            state,
            query_registry: Mutex::new(QueryRegistry::new()),
            declarative_queries: Mutex::new(std::collections::HashMap::new()),
            update_registry: Mutex::new(UpdateRegistry::new()),
            declarative_updates: Mutex::new(std::collections::HashMap::new()),
            signal_registry: Mutex::new(SignalHandlerRegistry::new()),
            cancellation_reason,
            strict_replay: false,
            canary_mode: false,
            workflow_name: String::new(),
            workflow_id: String::new(),
            build_id: None,
            nd_details: Mutex::new(None),
            payload_max_activity_input: DEFAULT_MAX_ACTIVITY_INPUT_BYTES,
            payload_max_workflow_input: DEFAULT_MAX_WORKFLOW_INPUT_BYTES,
            payload_max_side_effect: DEFAULT_MAX_WORKFLOW_INPUT_BYTES,
            payload_max_signal: DEFAULT_MAX_SIGNAL_PAYLOAD_BYTES,
            payload_offload_threshold: None,
            activity_input_cap_overrides: HashMap::new(),
            deferred_nd_error: Mutex::new(None),
            current_details_cap: DEFAULT_CURRENT_DETAILS_CAP_BYTES,
            context_headers: std::sync::Arc::new(HashMap::new()),
            last_completion_result,
            last_error,
            scheduled_time,
            #[cfg(any(test, feature = "testing"))]
            timer_clock_elapsed_secs: None,
            metrics: std::sync::Arc::new(crate::telemetry::NoOpMetrics),
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

    /// Like [`for_replay_strict_with_state`] but also sets `canary_mode` to `true`.
    #[must_use]
    pub fn for_replay_canary_with_state(
        exec_id: ExecutionId,
        events: Vec<WorkflowEvent>,
        state: SharedState,
    ) -> Self {
        let mut ctx = Self::for_replay_with_state(exec_id, events, state);
        ctx.strict_replay = true;
        ctx.canary_mode = true;
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
            fan_out_seq: Mutex::new(0),
            signal_timeout_seq: Mutex::new(0),
            race_seq: Mutex::new(0),
            session_seq: Mutex::new(0),
            state,
            query_registry: Mutex::new(QueryRegistry::new()),
            declarative_queries: Mutex::new(std::collections::HashMap::new()),
            update_registry: Mutex::new(UpdateRegistry::new()),
            declarative_updates: Mutex::new(std::collections::HashMap::new()),
            signal_registry: Mutex::new(SignalHandlerRegistry::new()),
            cancellation_reason,
            strict_replay: false,
            canary_mode: false,
            workflow_name: String::new(),
            workflow_id: String::new(),
            build_id: None,
            nd_details: Mutex::new(None),
            payload_max_activity_input: DEFAULT_MAX_ACTIVITY_INPUT_BYTES,
            payload_max_workflow_input: DEFAULT_MAX_WORKFLOW_INPUT_BYTES,
            payload_max_side_effect: DEFAULT_MAX_WORKFLOW_INPUT_BYTES,
            payload_max_signal: DEFAULT_MAX_SIGNAL_PAYLOAD_BYTES,
            payload_offload_threshold: None,
            activity_input_cap_overrides: HashMap::new(),
            deferred_nd_error: Mutex::new(None),
            current_details_cap: DEFAULT_CURRENT_DETAILS_CAP_BYTES,
            context_headers: std::sync::Arc::new(HashMap::new()),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
            #[cfg(any(test, feature = "testing"))]
            timer_clock_elapsed_secs: None,
            metrics: std::sync::Arc::new(crate::telemetry::NoOpMetrics),
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
            fan_out_seq: Mutex::new(0),
            signal_timeout_seq: Mutex::new(0),
            race_seq: Mutex::new(0),
            session_seq: Mutex::new(0),
            state: empty_shared_state(),
            query_registry: Mutex::new(QueryRegistry::new()),
            declarative_queries: Mutex::new(std::collections::HashMap::new()),
            update_registry: Mutex::new(UpdateRegistry::new()),
            declarative_updates: Mutex::new(std::collections::HashMap::new()),
            signal_registry: Mutex::new(SignalHandlerRegistry::new()),
            cancellation_reason: None,
            strict_replay: false,
            canary_mode: false,
            workflow_name: String::new(),
            workflow_id: String::new(),
            build_id: None,
            nd_details: Mutex::new(None),
            payload_max_activity_input: DEFAULT_MAX_ACTIVITY_INPUT_BYTES,
            payload_max_workflow_input: DEFAULT_MAX_WORKFLOW_INPUT_BYTES,
            payload_max_side_effect: DEFAULT_MAX_WORKFLOW_INPUT_BYTES,
            payload_max_signal: DEFAULT_MAX_SIGNAL_PAYLOAD_BYTES,
            payload_offload_threshold: None,
            activity_input_cap_overrides: HashMap::new(),
            deferred_nd_error: Mutex::new(None),
            current_details_cap: DEFAULT_CURRENT_DETAILS_CAP_BYTES,
            context_headers: std::sync::Arc::new(HashMap::new()),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
            #[cfg(any(test, feature = "testing"))]
            timer_clock_elapsed_secs: None,
            metrics: std::sync::Arc::new(crate::telemetry::NoOpMetrics),
        }
    }

    /// Enable the advancing virtual clock (issue #526, test harness only).
    ///
    /// When set, `ctx.now()` reflects cumulative durable-timer duration consumed
    /// from history, rather than the fixed `WorkflowStarted` timestamp.
    /// Only the test harness (`WorkflowTestEnv`) sets this; production entry
    /// points leave it `None`, preserving byte-for-byte identical behaviour.
    #[cfg(any(test, feature = "testing"))]
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn with_advancing_timer_clock(mut self) -> Self {
        self.timer_clock_elapsed_secs = Some(std::sync::atomic::AtomicU64::new(0));
        self
    }

    /// Advance the virtual timer clock by `duration_secs` (no-op in production).
    ///
    /// Called at each `TimerStarted` resolution site in `timer()` and
    /// `wait_for_signal_timeout()`.  `TestRunOutcome::final_now()` independently
    /// sums the same `TimerStarted { duration_secs }` events from the recorded
    /// history — the two mechanisms must remain in sync: every call here
    /// corresponds to exactly one `TimerStarted` event appended to history.
    #[cfg(any(test, feature = "testing"))]
    fn advance_timer_clock(&self, duration_secs: u64) {
        if let Some(ref atomic) = self.timer_clock_elapsed_secs {
            atomic.fetch_add(duration_secs, std::sync::atomic::Ordering::Relaxed);
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

    /// Set the large-payload offload threshold (issue #524). When set, a
    /// payload-bearing field larger than `threshold` bytes will be offloaded at
    /// persist time, so the #252 size cap is not enforced against it (the inline
    /// representation becomes a tiny reference envelope).
    #[must_use]
    pub const fn with_payload_offload_threshold(mut self, threshold: Option<u64>) -> Self {
        self.payload_offload_threshold = threshold;
        self
    }

    /// Whether a payload of `observed` bytes will be offloaded rather than stored
    /// inline, and therefore must NOT be rejected by the #252 size cap.
    const fn offload_will_apply(&self, observed: u64) -> bool {
        matches!(self.payload_offload_threshold, Some(t) if observed > t)
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

    /// Set the maximum byte length for `current_details` strings (issue #473).
    ///
    /// Overrides [`DEFAULT_CURRENT_DETAILS_CAP_BYTES`]. The executor calls this
    /// with the value from [`crate::builder::BuiltHarvest::max_current_details_bytes`].
    #[must_use]
    pub const fn with_current_details_cap(mut self, cap_bytes: usize) -> Self {
        self.current_details_cap = cap_bytes;
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

    /// Set the business-level workflow identifier (e.g. `"subscription-123"`).
    ///
    /// Called by the worker so that [`WorkflowLogger`] events can carry the
    /// correlation key that operators use to search Loki / Elastic for a
    /// specific workflow run.
    #[must_use]
    pub fn with_workflow_id(mut self, id: impl Into<String>) -> Self {
        self.workflow_id = id.into();
        self
    }

    /// Set the worker build ID.
    #[must_use]
    pub fn with_build_id(mut self, build_id: Option<String>) -> Self {
        self.build_id = build_id;
        self
    }

    // ── Accessors ─────────────────────────────────────────────────────

    /// Deterministic "wall clock" — returns the `WorkflowStarted` timestamp
    /// so that all replays produce the same result.
    ///
    /// In the test harness with the advancing clock enabled (issue #526),
    /// each durable timer that fires from history advances this by its duration,
    /// so time-aware logic can be unit-tested without a real database.
    #[must_use]
    pub fn now(&self) -> DateTime<Utc> {
        #[cfg(any(test, feature = "testing"))]
        if let Some(ref atomic) = self.timer_clock_elapsed_secs {
            let elapsed = atomic.load(std::sync::atomic::Ordering::Relaxed);
            let delta = chrono::Duration::seconds(
                i64::try_from(elapsed)
                    .unwrap_or(i64::MAX / 1000)
                    .min(i64::MAX / 1000),
            );
            return self.start_time + delta;
        }
        self.start_time
    }

    /// The nominal scheduled fire-time (logical slot) this run is responsible for,
    /// or `None` for manual / ad-hoc API starts and pre-#508 histories (issue #508).
    ///
    /// This is the **pre-jitter logical slot** (`scheduled_for`), NOT the
    /// `effective_fire_time` (post-jitter) and NOT the execution start wall-clock
    /// (use [`now()`](Self::now) for that). It is frozen into the `WorkflowStarted`
    /// event at start time and replays deterministically to the identical value.
    ///
    /// Use this to implement time-aware logic in scheduled / backfilled workflows
    /// without hand-plumbing dates through the static workflow input:
    ///
    /// ```rust,ignore
    /// #[workflow]
    /// async fn daily_aggregation(ctx: &WorkflowContext, _: ()) -> Result<(), String> {
    ///     let date = ctx.scheduled_time()
    ///         .unwrap_or_else(|| ctx.now())  // fallback for manual triggers
    ///         .date_naive();
    ///     // ... aggregate data for `date` ...
    ///     Ok(())
    /// }
    /// ```
    #[must_use]
    pub const fn scheduled_time(&self) -> Option<DateTime<Utc>> {
        self.scheduled_time
    }

    /// The unique execution (run) ID for this workflow.
    #[must_use]
    pub const fn execution_id(&self) -> ExecutionId {
        self.exec_id
    }

    /// The business-level workflow identifier set at workflow start.
    ///
    /// Returns an empty string when the context was created without an explicit
    /// workflow ID (e.g. in unit tests via [`Self::new_test`]).
    #[must_use]
    pub fn workflow_id(&self) -> &str {
        &self.workflow_id
    }

    /// The logical workflow type name (the function name decorated with `#[workflow]`).
    ///
    /// Returns an empty string when not explicitly set (update handler contexts).
    #[must_use]
    pub fn workflow_type(&self) -> &str {
        &self.workflow_name
    }

    /// The worker build ID of the worker executing this workflow.
    #[must_use]
    pub fn build_id(&self) -> Option<&str> {
        self.build_id.as_deref()
    }

    /// Returns `true` if all admitted update handlers have completed or failed.
    ///
    /// # Panics
    ///
    /// Panics if the internal history matcher lock is poisoned.
    #[must_use]
    pub fn all_handlers_finished(&self) -> bool {
        self.matcher
            .lock()
            .expect("matcher lock poisoned")
            .all_handlers_finished()
    }

    /// Returns the number of admitted update handlers that have not completed or failed.
    ///
    /// # Panics
    ///
    /// Panics if the internal history matcher lock is poisoned.
    #[must_use]
    pub fn unfinished_update_handler_count(&self) -> usize {
        self.matcher
            .lock()
            .expect("matcher lock poisoned")
            .unfinished_update_handler_count()
    }

    // ── Last-completion-result carryover (issue #488) ─────────────────────────

    /// Returns the deserialized output of the most recent prior COMPLETED run of the same
    /// scheduled workflow, or `None` on the first run, when no prior run has succeeded,
    /// or when this is a manual (non-scheduled) start.
    ///
    /// The value is frozen into the `WorkflowStarted` event at start time, so replay
    /// always returns the same result regardless of which worker processes the task.
    ///
    /// # Limitations
    /// - A prior COMPLETED run whose output serializes to JSON `null` (e.g. a workflow
    ///   returning `()` or `Option::None`) is reported here as `None` (indistinguishable
    ///   from "no prior run"). Incremental/cursor jobs — the intended use case — return a
    ///   structured cursor, never `null`, so this does not affect them.
    /// - Carryover assumes **non-overlapping** execution (the default
    ///   `max_active_runs = 1` / `OverlapPolicy::Skip`). The carryover source is the
    ///   highest *earlier* slot that has reached a terminal state; if a schedule is
    ///   configured with `max_active_runs > 1` so a later slot can start while an earlier
    ///   slot is still RUNNING, the later run observes the most recent *terminal* earlier
    ///   slot and may re-process the in-flight slot's range. Use `max_active_runs = 1` for
    ///   cursor-style incremental jobs.
    ///
    /// # Errors
    /// Returns `HarvestError::Deserialize` if the stored JSON cannot be deserialized into `T`.
    pub fn last_completion_result<T>(&self) -> crate::error::HarvestResult<Option<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        self.last_completion_result.as_ref().map_or_else(
            || Ok(None),
            |v| {
                serde_json::from_value(v.clone())
                    .map(Some)
                    .map_err(Into::into)
            },
        )
    }

    /// Returns the error message from the most recent terminal run of the same schedule if
    /// it ended `FAILED` or `TIMED_OUT`, or `None` if that run completed successfully or
    /// this is a manual (non-scheduled) start.
    ///
    /// Mirrors Temporal's `GetLastError()` cron primitive. Use this together with
    /// [`last_completion_result`](Self::last_completion_result) to implement the
    /// "did we recover from a failure?" branch in incremental jobs.
    #[must_use]
    pub fn last_error(&self) -> Option<String> {
        self.last_error.clone()
    }

    // ── Context headers (issue #481) ──────────────────────────────────────────

    /// Attach ambient context headers to this workflow context (builder-style).
    ///
    /// Headers are fixed at workflow-start time and propagated automatically to
    /// all activity and child-workflow dispatches without touching input types.
    /// Typically called by the framework after loading the execution row; author
    /// code reads headers via [`header`](Self::header) / [`headers`](Self::headers).
    #[must_use]
    pub fn with_context_headers(mut self, headers: HashMap<String, String>) -> Self {
        self.context_headers = std::sync::Arc::new(headers);
        self
    }

    // ── Custom metrics (issue #532) ───────────────────────────────────────────

    /// Attach a metrics recorder to this context (builder-style, called by the worker).
    ///
    /// The worker passes `registry.telemetry().metrics.clone()` here so that
    /// `ctx.metrics()` forwards to the same backend as engine metrics.
    /// Tests may inject a counting recorder via this method to assert emission counts.
    #[must_use]
    pub fn with_metrics(
        mut self,
        metrics: std::sync::Arc<dyn crate::telemetry::MetricsRecorder>,
    ) -> Self {
        self.metrics = metrics;
        self
    }

    /// Obtain a **replay-safe** custom-metrics handle for this workflow execution.
    ///
    /// Calls on the returned [`UserMetrics`](crate::telemetry::UserMetrics) handle
    /// are **suppressed** while the workflow is replaying recorded history
    /// (`is_replaying() == true`), so a counter inside workflow logic increments
    /// the backend **exactly once** per logical occurrence regardless of how many
    /// replay cycles the executor runs.
    ///
    /// ```rust,ignore
    /// #[workflow]
    /// async fn process_order(ctx: &WorkflowContext, order: Order) -> Result<(), String> {
    ///     ctx.metrics().counter("orders_accepted", 1, &[("tier", &order.tier)]);
    ///     ctx.execute_activity(&charge_card_info(), order.amount).await?;
    ///     ctx.metrics().counter("orders_completed", 1, &[("tier", &order.tier)]);
    ///     Ok(())
    /// }
    /// ```
    #[must_use]
    pub fn metrics(&self) -> crate::telemetry::UserMetrics<'_> {
        crate::telemetry::UserMetrics::new(&*self.metrics, self.is_replaying())
    }

    /// Return the value of the named context header, or `None` if not set.
    ///
    /// Returns `None` (never panics) when `key` was never attached, including
    /// for executions that were started before this feature was deployed.
    #[must_use]
    pub fn header(&self, key: &str) -> Option<&str> {
        self.context_headers.get(key).map(String::as_str)
    }

    /// Return the full context header map for this execution.
    ///
    /// The map is empty for executions started before this feature shipped.
    #[must_use]
    pub fn headers(&self) -> &HashMap<String, String> {
        &self.context_headers
    }

    /// Retrieve and clear the structured non-determinism details.
    #[must_use]
    pub fn take_nd_details(&self) -> Option<NonDeterministicDetails> {
        self.nd_details.lock().ok().and_then(|mut slot| slot.take())
    }

    /// The current cursor position in the history events during replay.
    #[must_use]
    pub fn replay_position(&self) -> usize {
        self.match_history(|m| m.position())
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
            .is_replaying()
    }

    // ── Replay-safe logging (issue #379) ──────────────────────────────

    /// Return a replay-aware logger scoped to this workflow execution.
    ///
    /// The logger suppresses all output when [`Self::is_replaying`] is `true`
    /// so that each log statement fires at most once per workflow execution,
    /// regardless of how many replay cycles the executor performs.
    ///
    /// Every event emitted carries `workflow_id`, `execution_id`,
    /// `workflow_type`, and `replay = false` as structured fields, enabling
    /// log correlation in Loki / Elastic / OpenTelemetry backends.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use autumn_harvest::context::WorkflowContext;
    /// # fn example(ctx: &WorkflowContext) {
    /// ctx.logger().info("payment started");
    /// ctx.logger().warn("retrying payment");
    /// ctx.logger().error("payment failed");
    /// # }
    /// ```
    #[must_use]
    pub const fn logger(&self) -> WorkflowLogger<'_> {
        WorkflowLogger { ctx: self }
    }

    /// Emit an INFO-level log event, suppressed during replay.
    ///
    /// Equivalent to `ctx.logger().info(message)`.
    pub fn log_info(&self, message: &str) {
        self.logger().info(message);
    }

    /// Emit a WARN-level log event, suppressed during replay.
    ///
    /// Equivalent to `ctx.logger().warn(message)`.
    pub fn log_warn(&self, message: &str) {
        self.logger().warn(message);
    }

    /// Emit an ERROR-level log event, suppressed during replay.
    ///
    /// Equivalent to `ctx.logger().error(message)`.
    pub fn log_error(&self, message: &str) {
        self.logger().error(message);
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
    ///   `shard_id`, `status`, `run_id`, `failure_cause`, `event_index`,
    ///   `expected`, `actual`, `workflow_type`, `build_id` (the last six are
    ///   the replay-non-determinism diagnostic keys, issue #603).
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

            HistoryMatch::Diverged {
                expected,
                actual,
                event_index,
            } => Err(self.nd_error(
                format!("side effect mismatch: expected {expected}, got {actual}"),
                event_index,
                Some(expected),
                Some(actual),
            )),

            HistoryMatch::Failed { .. }
            | HistoryMatch::TimedOut { .. }
            | HistoryMatch::ActivityInProgress { .. }
            | HistoryMatch::AwaitingExternalCompletion { .. }
            | HistoryMatch::ChildInProgress { .. }
            | HistoryMatch::LocalActivityInProgress { .. }
            | HistoryMatch::ExternalSignalInProgress { .. }
            | HistoryMatch::ExternalSignalFailed { .. }
            | HistoryMatch::ExternalCancelInProgress { .. }
            | HistoryMatch::ExternalCancelFailed { .. }
            | HistoryMatch::DetachedChildSpawned { .. } => {
                unreachable!("match_side_effect only returns Matched, Diverged or NoMatch")
            }

            HistoryMatch::NoMatch => {
                self.check_strict_replay_no_match(&format!("SideEffectRecorded({id})"))?;

                let result = f();
                let output = serde_json::to_value(&result)?;

                // Enforce side-effect payload cap before recording.
                // SideEffectRecorded events are written via pre_suspension_events_from_commands
                // using plain store::append_events (no offloader), so the offload bypass must
                // NOT apply here even when a PayloadStore is configured.
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

                self.push_command(WorkflowCommand::RecordSideEffect {
                    kind: crate::event::SideEffectKind::Custom,
                    name: Some(id.to_string()),
                    value: output,
                });

                Ok(result)
            }
        }
    }

    // ── Deterministic built-in primitives (issue #384) ────────────────────────

    /// Record the first non-determinism error seen by an infallible primitive.
    ///
    /// Only the first error is retained; later divergences in the same execution
    /// cycle do not overwrite it. The executor drains this via
    /// [`take_deferred_nd_error`](Self::take_deferred_nd_error).
    fn record_deferred_nd(&self, msg: String) {
        {
            let mut slot = self
                .deferred_nd_error
                .lock()
                .expect("deferred_nd_error lock poisoned");
            if slot.is_none() {
                *slot = Some(msg);
            }
        }
        if let Ok(mut details_slot) = self.nd_details.lock() {
            details_slot.get_or_insert_with(|| NonDeterministicDetails {
                event_index: i32::try_from(self.match_history(|m| m.position())).ok(),
                expected: None,
                actual: None,
                workflow_type: Some(self.workflow_name.clone()),
                build_id: self.build_id.clone(),
            });
        }
    }

    /// Take the first deferred non-determinism error recorded by an infallible
    /// primitive (`system_now`, `new_uuid`, `random_*`), if any.
    ///
    /// Called by the executor after the workflow handler returns so a divergence
    /// absorbed by a plain-value primitive still fails the replay cleanly.
    ///
    /// # Panics
    ///
    /// Panics if the internal `deferred_nd_error` mutex is poisoned.
    #[must_use]
    pub fn take_deferred_nd_error(&self) -> Option<String> {
        self.deferred_nd_error
            .lock()
            .expect("deferred_nd_error lock poisoned")
            .take()
    }

    /// Shared lowering for the infallible built-in primitives.
    ///
    /// Matches a [`WorkflowEvent::SideEffectRecorded`] of `kind` at the current
    /// cursor during replay and returns the recorded value; on the first live
    /// run it invokes `f`, records the value, and emits a `RecordSideEffect`
    /// command. On a genuine divergence it records a deferred non-determinism
    /// error (the executor converts the outcome to `Failed`) and falls back to a
    /// freshly computed value so the rest of the cycle can still run.
    fn capture_builtin<F, T>(&self, kind: crate::event::SideEffectKind, f: F) -> T
    where
        F: FnOnce() -> T,
        T: serde::Serialize + serde::de::DeserializeOwned,
    {
        self.capture_builtin_validated(kind, |_| true, f)
    }

    /// Like [`capture_builtin`](Self::capture_builtin) but additionally validates
    /// a replayed value against `is_valid`.
    ///
    /// All random helpers lower onto the same `SideEffectKind::Random`, so a code
    /// change that swaps one helper for another at a call site (e.g. `random_u64`
    /// → `random_f64`) would otherwise deserialize the recorded JSON without
    /// complaint — a `42` integer reads back as `42.0`, silently violating the new
    /// helper's documented domain. `is_valid` lets a helper reject a replayed
    /// value that falls outside its contract (e.g. `random_f64`'s `[0, 1)`),
    /// recording it as a deferred non-determinism error instead.
    fn capture_builtin_validated<F, V, T>(
        &self,
        kind: crate::event::SideEffectKind,
        is_valid: V,
        f: F,
    ) -> T
    where
        F: FnOnce() -> T,
        V: FnOnce(&T) -> bool,
        T: serde::Serialize + serde::de::DeserializeOwned,
    {
        let history_match = self.match_history(|m| m.match_side_effect_event(kind, None));

        match history_match {
            HistoryMatch::Matched { output } => match serde_json::from_value::<T>(output) {
                Ok(value) if is_valid(&value) => value,
                Ok(_) => {
                    self.record_deferred_nd(format!(
                        "side-effect drift mismatch: expected SideEffectRecorded({}) within its \
                         documented domain, got an out-of-domain replayed value",
                        kind.as_str()
                    ));
                    f()
                }
                Err(e) => {
                    self.record_deferred_nd(format!(
                        "side-effect drift mismatch: expected SideEffectRecorded({}), \
                         got an undeserialisable recorded value ({e})",
                        kind.as_str()
                    ));
                    f()
                }
            },
            HistoryMatch::Diverged {
                expected, actual, ..
            } => {
                self.record_deferred_nd(format!(
                    "side-effect drift mismatch: expected {expected}, got {actual}"
                ));
                f()
            }
            HistoryMatch::NoMatch => {
                if self.strict_replay {
                    self.record_deferred_nd(format!(
                        "side-effect drift mismatch: expected <end of history>, \
                         got SideEffectRecorded({})",
                        kind.as_str()
                    ));
                }
                let result = f();
                // Built-in values (timestamps, UUIDs, u64/f64) always serialise.
                let value = serde_json::to_value(&result)
                    .expect("built-in side-effect value must serialise");
                self.push_command(WorkflowCommand::RecordSideEffect {
                    kind,
                    name: None,
                    value,
                });
                result
            }
            _ => unreachable!("match_side_effect_event only returns Matched, Diverged or NoMatch"),
        }
    }

    /// Deterministic wall-clock read, captured once and replayed verbatim.
    ///
    /// Unlike [`now`](Self::now) — which returns the fixed `WorkflowStarted`
    /// timestamp (the workflow-logical start clock) — `system_now` captures the
    /// *current* wall-clock instant the first time it executes at this point in
    /// the workflow, freezes it into history as a
    /// [`WorkflowEvent::SideEffectRecorded`], and returns that exact instant on
    /// every subsequent replay. Use it for "is this event older than 24h *now*?"
    /// style decisions inside a long-running workflow.
    ///
    /// This is the safe, replay-deterministic alternative to calling
    /// `chrono::Utc::now()` directly (guardrail HVG001).
    ///
    /// # Panics
    ///
    /// Panics if the internal matcher or commands mutex is poisoned.
    #[must_use]
    pub fn system_now(&self) -> DateTime<Utc> {
        let millis = self.capture_builtin(crate::event::SideEffectKind::Now, || {
            Utc::now().timestamp_millis()
        });
        DateTime::from_timestamp_millis(millis).unwrap_or(self.start_time)
    }

    /// Deterministic wall-clock read as a [`std::time::SystemTime`].
    ///
    /// Identical capture semantics to [`system_now`](Self::system_now); use this
    /// when you need a `SystemTime` rather than a `chrono` value. Each call
    /// records its own [`WorkflowEvent::SideEffectRecorded`] event.
    ///
    /// # Panics
    ///
    /// Panics if the internal matcher or commands mutex is poisoned.
    #[must_use]
    pub fn system_time_now(&self) -> std::time::SystemTime {
        let millis = self.capture_builtin(crate::event::SideEffectKind::Now, || {
            Utc::now().timestamp_millis()
        });
        let millis_u64 = u64::try_from(millis.max(0)).unwrap_or(0);
        std::time::UNIX_EPOCH + std::time::Duration::from_millis(millis_u64)
    }

    /// Deterministic UUID (version 7), captured once and replayed verbatim.
    ///
    /// Mints a fresh time-ordered `UUIDv7` on the first live execution, records it
    /// in history, and returns the same value on every replay. This is the safe,
    /// replay-deterministic alternative to calling `Uuid::new_v4()` /
    /// `Uuid::now_v7()` directly inside a workflow (guardrail HVG002) — ideal for
    /// idempotency keys.
    ///
    /// # Panics
    ///
    /// Panics if the internal matcher or commands mutex is poisoned.
    #[must_use]
    pub fn new_uuid(&self) -> uuid::Uuid {
        self.capture_builtin(crate::event::SideEffectKind::Uuid, uuid::Uuid::now_v7)
    }

    /// Deterministic random `u64`, captured once and replayed verbatim.
    ///
    /// The draw is **not** cryptographically secure — it is intended for
    /// sampling and idempotency-key style work. Security-grade entropy belongs in
    /// a regular activity (see issue #384 "Out of Scope").
    ///
    /// # Panics
    ///
    /// Panics if the internal matcher or commands mutex is poisoned.
    #[must_use]
    pub fn random_u64(&self) -> u64 {
        self.capture_builtin(crate::event::SideEffectKind::Random, || {
            rand::random::<u64>()
        })
    }

    /// Deterministic random `f64` in the half-open range `[0, 1)`, captured once
    /// and replayed verbatim.
    ///
    /// On replay a recorded value outside `[0, 1)` (e.g. because the call site was
    /// changed from `random_u64()`, whose draw shares the same
    /// `SideEffectKind::Random`) is rejected as a deferred non-determinism error
    /// rather than silently returning an out-of-contract value.
    ///
    /// # Panics
    ///
    /// Panics if the internal matcher or commands mutex is poisoned.
    #[must_use]
    pub fn random_f64(&self) -> f64 {
        self.capture_builtin_validated(
            crate::event::SideEffectKind::Random,
            |v: &f64| (0.0..1.0).contains(v),
            rand::random::<f64>,
        )
    }

    /// Deterministic random value drawn uniformly from `range`, captured once and
    /// replayed verbatim.
    ///
    /// Mirrors [`rand::Rng::gen_range`]; works for any range whose element type
    /// implements [`rand::distributions::uniform::SampleUniform`] and round-trips
    /// through JSON (e.g. `0..100`, `1.0..2.0`).
    ///
    /// On replay, if the recorded value falls outside the current `range` bounds a
    /// deferred non-determinism error is recorded (the executor converts this to a
    /// `WorkflowFailed` outcome), preventing a range-narrowing code change from
    /// silently returning an out-of-bounds value.
    ///
    /// # Panics
    ///
    /// Panics if `range` is empty, or if the internal matcher/commands mutex is
    /// poisoned.
    pub fn random_range<T, R>(&self, range: R) -> T
    where
        T: serde::Serialize
            + serde::de::DeserializeOwned
            + rand::distributions::uniform::SampleUniform
            + PartialOrd,
        R: rand::distributions::uniform::SampleRange<T> + std::ops::RangeBounds<T>,
    {
        use crate::event::SideEffectKind;

        let history_match =
            self.match_history(|m| m.match_side_effect_event(SideEffectKind::Random, None));

        match history_match {
            HistoryMatch::Matched { output } => match serde_json::from_value::<T>(output) {
                Ok(value) => {
                    if range.contains(&value) {
                        value
                    } else {
                        self.record_deferred_nd(
                            "side-effect drift mismatch: expected SideEffectRecorded(random) \
                             within the current range, got an out-of-range replayed value"
                                .to_string(),
                        );
                        rand::Rng::gen_range(&mut rand::thread_rng(), range)
                    }
                }
                Err(e) => {
                    self.record_deferred_nd(format!(
                        "side-effect drift mismatch: expected SideEffectRecorded(random), \
                         got an undeserialisable recorded value ({e})"
                    ));
                    rand::Rng::gen_range(&mut rand::thread_rng(), range)
                }
            },
            HistoryMatch::Diverged {
                expected, actual, ..
            } => {
                self.record_deferred_nd(format!(
                    "side-effect drift mismatch: expected {expected}, got {actual}"
                ));
                rand::Rng::gen_range(&mut rand::thread_rng(), range)
            }
            HistoryMatch::NoMatch => {
                if self.strict_replay {
                    self.record_deferred_nd(
                        "side-effect drift mismatch: expected <end of history>, \
                         got SideEffectRecorded(random)"
                            .to_string(),
                    );
                }
                let result = rand::Rng::gen_range(&mut rand::thread_rng(), range);
                let value = serde_json::to_value(&result)
                    .expect("built-in side-effect value must serialise");
                self.push_command(WorkflowCommand::RecordSideEffect {
                    kind: SideEffectKind::Random,
                    name: None,
                    value,
                });
                result
            }
            _ => unreachable!("match_side_effect_event only returns Matched, Diverged or NoMatch"),
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
    /// For the common two-state before/after change prefer
    /// [`patched`](Self::patched) — `version()` is the escape hatch for
    /// gates with **more than two** concurrent versions.
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
                name: crate::replay::version_marker_name(change_id),
                details: Value::from(u64::from(max)),
            });
        }

        version
    }

    /// Boolean two-state code-evolution gate (issue #687) — the ergonomic
    /// default over the multi-version [`version`](Self::version) escape hatch.
    ///
    /// Returns `true` when this execution is on the patched code path and
    /// `false` when it is replaying pre-patch history. Backed by the **same**
    /// `MarkerRecorded` event `ctx.version()` uses (`patch:{patch_id}` — no
    /// new `WorkflowEvent` variant, no migration), so the append-only history
    /// contract is untouched.
    ///
    /// - During **live execution** (past the end of recorded history):
    ///   records a `patch:{patch_id}` marker and returns `true`.
    /// - During **replay** with the marker at the cursor: consumes it and
    ///   returns `true`.
    /// - During **replay** without the marker (a pre-patch run): returns
    ///   `false` without advancing the cursor, so the recorded event still
    ///   matches the old branch's next command.
    ///
    /// # Three-phase lifecycle
    ///
    /// **Deploy 1 — introduce the patch.** Fence the change:
    ///
    /// ```rust,ignore
    /// if ctx.patched("billing-v2") {
    ///     ctx.execute_activity(&compute_tax_v2_info(), input).await?;
    /// } else {
    ///     ctx.execute_activity(&compute_tax_v1_info(), input).await?;
    /// }
    /// ```
    ///
    /// Pre-patch executions keep replaying the old branch deterministically;
    /// new executions record the marker and take the new branch (with the
    /// signal-with-start exception below).
    ///
    /// **Deploy 2 — deprecate.** Once every *pre-patch* run has drained (see
    /// the "Patched gates" section of
    /// `docs/runbooks/version-gate-retirement.md` — note that the runbook's
    /// `harvest version-usage` / `version-gate-retirement --check` CLI
    /// tooling only sees `version:` markers and does **not** cover patch
    /// gates; use that section's raw SQL drain queries instead), replace the
    /// branch with [`deprecate_patch`](Self::deprecate_patch) +
    /// unconditional new code:
    ///
    /// ```rust,ignore
    /// ctx.deprecate_patch("billing-v2");
    /// ctx.execute_activity(&compute_tax_v2_info(), input).await?;
    /// ```
    ///
    /// Marker-bearing (phase-1) histories replay cleanly: the deprecation
    /// makes their marker transparent wherever it sits. New executions record
    /// nothing.
    ///
    /// **Deploy 3 — remove.** Once every *marker-bearing* run has drained,
    /// delete the `deprecate_patch` call entirely.
    ///
    /// # Signal-with-start / trailing-signal caveat
    ///
    /// A fresh execution whose first-task history ends in **un-awaited
    /// signals** at the gate point takes the **old** branch: `patched()`
    /// returns `false` and records **no** marker — forever, deterministically
    /// for that execution. Canonically this is **every signal-with-start
    /// run**, whose signal is staged before first dispatch, so the history at
    /// the gate is `[WorkflowStarted, SignalReceived]`. This is deliberate,
    /// conservative parity with [`version`](Self::version): after draining
    /// the trailing signals the history is indistinguishable from a phase-0
    /// run parked at a first-line `wait_for_signal`, so the ambiguity
    /// resolves to the old branch. Consequence: drain verification before
    /// deploy 2 must use the "no marker" inverse query in the runbook's
    /// "Patched gates" section — a marker-presence query can never find
    /// these runs.
    ///
    /// # Per-call-site answer
    ///
    /// `patched()` answers per **call site**, not per run: an in-flight
    /// phase-0 execution resuming under phase-1 code with two gated sites can
    /// get `false` at site 1 (recorded history) and `true` at site 2 (live
    /// frontier). Don't split one logical change across multiple gates of the
    /// same id; the answer is per call site.
    ///
    /// # Interop with `version()`
    ///
    /// A history that recorded `version:{patch_id}` under the old
    /// [`version`](Self::version) API is observed as patched — presence alone
    /// decides, regardless of the recorded number, because `version()` only
    /// ever records a marker when it returned `max` on live execution. You
    /// can migrate a two-version `ctx.version(id, 1, 2)` gate to
    /// `ctx.patched(id)` in place. `version()` remains the explicit escape
    /// hatch for gates with **more than two** concurrent versions.
    ///
    /// **Shared namespace warning:** patch ids and version change-ids share
    /// **one namespace** — [`deprecate_patch`](Self::deprecate_patch)
    /// interop-consumes `version:{id}` markers, so never call
    /// `deprecate_patch(id)` while a `ctx.version(id, ..)` gate for the same
    /// id is still in the code: the still-live gate would then read `min`
    /// (its marker was consumed) and take the wrong branch, diverging as
    /// non-determinism.
    ///
    /// # Residual-`patched` footgun
    ///
    /// After `deprecate_patch(id)`, a *residual* `patched(id)` call later in
    /// the body stays deterministic (`true` for phase-1 histories, `false`
    /// for phase-0 histories) — but it also returns `false` for **new
    /// executions** started post-deprecation, and records nothing. If you
    /// still branch on it, new runs take the *old* branch. The fix is to
    /// delete the residual call when you deprecate. One exception keeps the
    /// live cycle consistent with replay: a marker recorded **earlier in the
    /// same cycle** (by a `patched(id)` or `version(id, ..)` call before the
    /// `deprecate_patch(id)`) counts as present, so a
    /// `patched(id)` → `deprecate_patch(id)` → `patched(id)` sandwich yields
    /// `(true, true)` on both the live pass and every replay pass.
    ///
    /// # Panics
    ///
    /// Panics if `patch_id` is empty (a programmer error, mirroring the
    /// [`version`](Self::version) argument asserts), or if the internal
    /// matcher or commands mutex is poisoned.
    pub fn patched(&self, patch_id: &str) -> bool {
        assert!(!patch_id.is_empty(), "patch id must not be empty");
        match self.match_history(|m| m.match_patch_marker(patch_id)) {
            PatchMarkerMatch::Recorded => true,
            PatchMarkerMatch::Absent => false,
            PatchMarkerMatch::NewlyPatched => {
                // NewlyPatched is only ever returned when the matcher is past
                // recorded history (live frontier) by construction — the
                // marker is recorded exactly once per call site, never during
                // a replay pass.
                debug_assert!(
                    !self.is_replaying(),
                    "NewlyPatched must only be returned on the live frontier"
                );
                self.push_command(WorkflowCommand::RecordMarker {
                    name: crate::replay::patch_marker_name(patch_id),
                    details: Value::from(1u64),
                });
                true
            }
        }
    }

    /// Deprecate a [`patched`](Self::patched) gate (issue #687) — deploy 2 of
    /// the three-phase lifecycle documented on `patched`.
    ///
    /// Makes every recorded `patch:{patch_id}` (or interop
    /// `version:{patch_id}`) marker transparent to replay, wherever it sits
    /// in history: a phase-1 run recorded the marker at the old `patched()`
    /// call position, while this call usually sits earlier in the body, so
    /// positional matching cannot apply — without deprecation the orphaned
    /// marker would trip the next command's match as a divergence.
    ///
    /// Emits **no** commands and appends **no** events — on a live execution
    /// this is a pure no-op. Idempotent within a replay cycle.
    ///
    /// Only call this once every pre-patch execution has drained (see the
    /// "Patched gates" section of
    /// `docs/runbooks/version-gate-retirement.md` — the runbook's
    /// `version-usage` / retirement-check CLI tooling does **not** see
    /// `patch:` markers; use that section's raw SQL drain queries): a
    /// phase-0 history replayed against unconditional new code diverges, by
    /// design.
    ///
    /// **Shared namespace warning:** patch ids and version change-ids share
    /// **one namespace** — this call also consumes `version:{patch_id}`
    /// markers (interop). Never call `deprecate_patch(id)` while a
    /// `ctx.version(id, ..)` gate for the same id is still in the code: the
    /// still-live gate would read `min` after its marker was consumed and
    /// take the wrong branch, diverging as non-determinism.
    ///
    /// # Panics
    ///
    /// Panics if `patch_id` is empty (a programmer error, mirroring the
    /// [`version`](Self::version) argument asserts), or if the internal
    /// matcher mutex is poisoned.
    pub fn deprecate_patch(&self, patch_id: &str) {
        assert!(!patch_id.is_empty(), "patch id must not be empty");
        self.match_history(|m| {
            m.deprecate_patch(patch_id);
        });
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
        self.execute_activity_raw_full(name, input, queue, None, None, None, None, None)
            .await
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
        self.execute_activity_raw_full(
            name,
            input,
            queue,
            retry_policy_override,
            start_to_close_override,
            None,
            None,
            None,
        )
        .await
    }

    /// The full activity-dispatch state machine underlying
    /// [`execute_activity_raw`](Self::execute_activity_raw),
    /// [`execute_activity_raw_with_opts`](Self::execute_activity_raw_with_opts),
    /// and [`Session`]'s session-scoped activity dispatch (issue #606).
    ///
    /// `session_id`/`session_worker_id`/`schedule_to_start_override` are
    /// `None` for every ordinary (non-session) call site — passing `None`
    /// for all three is byte-identical to the pre-#606 behavior of the two
    /// public wrappers above.
    #[allow(clippy::too_many_lines, clippy::too_many_arguments)]
    async fn execute_activity_raw_full(
        &self,
        name: &str,
        input: Value,
        queue: &str,
        retry_policy_override: Option<crate::policy::RetryPolicy>,
        start_to_close_override: Option<std::time::Duration>,
        session_id: Option<SessionId>,
        session_worker_id: Option<String>,
        schedule_to_start_override: Option<std::time::Duration>,
    ) -> HarvestResult<Value> {
        let history_match = if self.strict_replay {
            self.match_history(|m| m.match_activity_strict(name, &input))
        } else {
            self.match_history(|m| m.match_activity(name))
        };

        match history_match {
            HistoryMatch::Matched { output } => Ok(output),
            HistoryMatch::Failed {
                error,
                attempt,
                error_type,
                details,
            } => Err(HarvestError::ActivityFailed {
                name: name.to_string(),
                attempt,
                error_type,
                details,
                source: error.into(),
            }),
            HistoryMatch::TimedOut { timeout_type } => Err(HarvestError::Timeout {
                timeout_type,
                task_name: name.to_string(),
            }),
            HistoryMatch::Diverged {
                expected,
                actual,
                event_index,
            } => Err(self.nd_error(
                format!("activity mismatch: expected {expected}, got {actual}"),
                event_index,
                Some(expected),
                Some(actual),
            )),
            HistoryMatch::ActivityInProgress { activity_id } => {
                let (tx, rx) = oneshot::channel();
                self.push_command(WorkflowCommand::WaitForActivity {
                    activity_id,
                    result_tx: tx,
                });
                match rx.await {
                    Ok(Ok(output)) => Ok(output),
                    Ok(Err(error)) => Err(HarvestError::activity_failed(name, 1, &error)),
                    Err(_) => Err(HarvestError::Cancelled(format!(
                        "activity '{name}' cancelled: result channel dropped"
                    ))),
                }
            }
            HistoryMatch::AwaitingExternalCompletion { .. }
            | HistoryMatch::ChildInProgress { .. }
            | HistoryMatch::LocalActivityInProgress { .. }
            | HistoryMatch::ExternalSignalInProgress { .. }
            | HistoryMatch::ExternalSignalFailed { .. }
            | HistoryMatch::ExternalCancelInProgress { .. }
            | HistoryMatch::ExternalCancelFailed { .. }
            | HistoryMatch::DetachedChildSpawned { .. } => {
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
                if effective_cap > 0
                    && observed > effective_cap
                    && !self.offload_will_apply(observed)
                {
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
                    session_id,
                    session_worker_id,
                    schedule_to_start_override,
                    result_tx: tx,
                });
                match rx.await {
                    Ok(Ok(output)) => Ok(output),
                    Ok(Err(error)) => Err(HarvestError::activity_failed(name, 1, &error)),
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

            HistoryMatch::Failed {
                error,
                attempt,
                error_type,
                details,
            } => Err(HarvestError::ActivityFailed {
                name: name.to_string(),
                attempt,
                error_type,
                details,
                source: error.into(),
            }),

            HistoryMatch::TimedOut { timeout_type } => Err(HarvestError::Timeout {
                timeout_type,
                task_name: name.to_string(),
            }),

            HistoryMatch::Diverged {
                expected,
                actual,
                event_index,
            } => Err(self.nd_error(
                format!("local activity mismatch: expected {expected}, got {actual}"),
                event_index,
                Some(expected),
                Some(actual),
            )),

            HistoryMatch::AwaitingExternalCompletion { .. }
            | HistoryMatch::ActivityInProgress { .. }
            | HistoryMatch::ChildInProgress { .. }
            | HistoryMatch::ExternalSignalInProgress { .. }
            | HistoryMatch::ExternalSignalFailed { .. }
            | HistoryMatch::ExternalCancelInProgress { .. }
            | HistoryMatch::ExternalCancelFailed { .. }
            | HistoryMatch::DetachedChildSpawned { .. } => {
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
                    return Err(self.nd_error(
                        format!("local activity '{name}' scheduled but terminal not in history"),
                        self.match_history(|m| i32::try_from(m.position()).ok()),
                        Some("LocalActivityCompleted".to_string()),
                        Some("LocalActivityInProgress".to_string()),
                    ));
                }

                // If the recorded failure count already covers all retry
                // attempts, return the last error immediately — no handler
                // execution is needed and no command should be pushed.
                let max_attempts = retry_policy.as_ref().map_or(1, |p| p.max_attempts);
                if failed_attempts >= max_attempts {
                    let error = last_error.unwrap_or_else(|| {
                        format!("local activity '{name}' failed after {failed_attempts} attempts")
                    });
                    return Err(HarvestError::activity_failed(name, failed_attempts, &error));
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
                    Ok(Err(error)) => Err(HarvestError::activity_failed(
                        name,
                        failed_attempts.max(1),
                        &error,
                    )),
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
                // Local activities are written with plain store::append_events (no
                // offloader), so the offload bypass must NOT apply here even when a
                // PayloadStore is configured. Always enforce the cap.
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
                    Ok(Err(error)) => Err(HarvestError::activity_failed(name, 1, &error)),
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
            HistoryMatch::Matched { .. } => {
                // Advance the virtual clock so ctx.now() reflects elapsed time.
                // TestRunOutcome::final_now() independently sums TimerStarted
                // duration_secs from the recorded history — both must stay in sync.
                #[cfg(any(test, feature = "testing"))]
                self.advance_timer_clock(duration_secs);
                Ok(())
            }

            HistoryMatch::Diverged {
                expected,
                actual,
                event_index,
            } => Err(self.nd_error(
                format!("timer mismatch: expected {expected}, got {actual}"),
                event_index,
                Some(expected),
                Some(actual),
            )),

            HistoryMatch::Failed { .. }
            | HistoryMatch::TimedOut { .. }
            | HistoryMatch::ActivityInProgress { .. }
            | HistoryMatch::AwaitingExternalCompletion { .. }
            | HistoryMatch::ChildInProgress { .. }
            | HistoryMatch::LocalActivityInProgress { .. }
            | HistoryMatch::ExternalSignalInProgress { .. }
            | HistoryMatch::ExternalSignalFailed { .. }
            | HistoryMatch::ExternalCancelInProgress { .. }
            | HistoryMatch::ExternalCancelFailed { .. }
            | HistoryMatch::DetachedChildSpawned { .. } => {
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
    #[allow(clippy::too_many_lines)]
    pub async fn spawn_child_workflow_raw(
        &self,
        workflow_name: &str,
        input: Value,
    ) -> HarvestResult<Value> {
        let history_match = self.match_history(|m| m.match_child_workflow(workflow_name, &input));

        match history_match {
            HistoryMatch::Matched { output } => Ok(output),
            HistoryMatch::Failed {
                error,
                attempt,
                error_type,
                details,
            } => Err(HarvestError::ActivityFailed {
                name: format!("child-workflow:{workflow_name}"),
                attempt,
                error_type,
                details,
                source: error.into(),
            }),
            HistoryMatch::TimedOut { .. }
            | HistoryMatch::ActivityInProgress { .. }
            | HistoryMatch::AwaitingExternalCompletion { .. }
            | HistoryMatch::LocalActivityInProgress { .. }
            | HistoryMatch::ExternalSignalInProgress { .. }
            | HistoryMatch::ExternalSignalFailed { .. }
            | HistoryMatch::ExternalCancelInProgress { .. }
            | HistoryMatch::ExternalCancelFailed { .. }
            | HistoryMatch::DetachedChildSpawned { .. } => {
                unreachable!("child workflows do not time out in match_child_workflow")
            }
            HistoryMatch::Diverged {
                expected,
                actual,
                event_index,
            } => Err(self.nd_error(
                format!("child workflow mismatch: expected {expected}, got {actual}"),
                event_index,
                Some(expected),
                Some(actual),
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
                    return Err(self.nd_error(
                        format!(
                            "child workflow '{workflow_name}' started but terminal not in history"
                        ),
                        self.match_history(|m| i32::try_from(m.position()).ok()),
                        Some("ChildWorkflowCompleted".to_string()),
                        Some("ChildWorkflowInProgress".to_string()),
                    ));
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
                    Ok(Err(error)) => Err(HarvestError::activity_failed(
                        format!("child-workflow:{workflow_name}"),
                        1,
                        &error,
                    )),
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
                // ChildWorkflowStarted is written to the parent's history via
                // append_single_event (plain), so the offload bypass must NOT apply here.
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
                    Ok(Err(error)) => Err(HarvestError::activity_failed(
                        format!("child-workflow:{workflow_name}"),
                        1,
                        &error,
                    )),
                    Err(_) => Err(HarvestError::Cancelled(format!(
                        "child workflow '{workflow_name}' cancelled: result channel dropped"
                    ))),
                }
            }
        }
    }

    // ── Detached child workflow ───────────────────────────────────────────────

    /// Spawn a child workflow in **detached** mode and return its [`ExecutionId`]
    /// immediately.
    ///
    /// Unlike [`spawn_child_workflow_raw`](Self::spawn_child_workflow_raw), the
    /// parent **does not suspend** awaiting the child's terminal result. The
    /// child runs independently. When the parent eventually reaches a terminal
    /// state (Completed, Failed, Cancelled, Terminated, or execution-timeout)
    /// the executor applies `parent_close_policy` to any still-running children
    /// spawned via this method.
    ///
    /// # Parent-close policy semantics
    ///
    /// | Policy | Effect when parent closes |
    /// |---|---|
    /// | `Abandon` | Child continues running — no cascade |
    /// | `RequestCancel` | Executor cancels the child; child observes `ctx.is_cancelled()` |
    /// | `Terminate` | Executor force-fails the child with `"ParentClosed"` error |
    ///
    /// The default policy is `RequestCancel`. Use `Abandon` for "fire-and-forget"
    /// fan-out and long-lived monitor patterns.
    ///
    /// # Shard restriction
    ///
    /// The child is placed on the **same shard** as the parent. Cross-shard
    /// detached spawns are not supported in this release; pass an
    /// `ExecutionId` from a different shard and you will receive
    /// [`HarvestError::Config`].
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::PayloadTooLarge`] when the serialised `input`
    /// exceeds the configured workflow-input cap.
    /// Returns [`HarvestError::NonDeterministic`] in strict-replay mode when
    /// history does not contain a matching `ChildWorkflowSpawnedDetached` event.
    ///
    /// # Panics
    ///
    /// Panics if the internal replay matcher mutex is poisoned.
    pub fn spawn_child_workflow_detached_raw(
        &self,
        workflow_name: &str,
        input: Value,
        parent_close_policy: crate::types::ParentClosePolicy,
    ) -> HarvestResult<ExecutionId> {
        let history_match = self.match_history(|m| {
            m.match_detached_child_spawn(workflow_name, &input, parent_close_policy)
        });

        match history_match {
            HistoryMatch::DetachedChildSpawned { child_id } => {
                // Replaying: return the recorded child ID.
                Ok(child_id)
            }
            HistoryMatch::NoMatch => {
                self.check_strict_replay_no_match(&format!(
                    "ChildWorkflowSpawnedDetached({workflow_name})"
                ))?;

                // Enforce child-workflow input payload cap before scheduling.
                // ChildWorkflowSpawnedDetached is written via pre_suspension_events_from_commands
                // using plain store::append_events (no offloader), so the offload bypass must
                // NOT apply here even when a PayloadStore is configured.
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

                let child_id = ExecutionId::new_for_shard(self.exec_id.shard());
                self.push_command(WorkflowCommand::SpawnDetachedChildWorkflow {
                    child_id,
                    workflow_name: workflow_name.to_string(),
                    input,
                    parent_close_policy,
                });
                Ok(child_id)
            }
            HistoryMatch::Diverged {
                expected,
                actual,
                event_index,
            } => Err(self.nd_error(
                format!("detached child workflow mismatch: expected {expected}, got {actual}"),
                event_index,
                Some(expected),
                Some(actual),
            )),
            _ => unreachable!(
                "match_detached_child_spawn only returns DetachedChildSpawned, NoMatch, or Diverged"
            ),
        }
    }

    /// Typed wrapper around
    /// [`spawn_child_workflow_detached_raw`](Self::spawn_child_workflow_detached_raw).
    ///
    /// Serializes the input using `serde_json` and returns the child
    /// [`ExecutionId`] immediately. The parent does **not** suspend.
    ///
    /// See [`spawn_child_workflow_detached_raw`](Self::spawn_child_workflow_detached_raw)
    /// for the full contract and policy semantics.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Serialization`] if `input` cannot be serialized.
    /// Propagates all errors from
    /// [`spawn_child_workflow_detached_raw`](Self::spawn_child_workflow_detached_raw).
    pub fn spawn_child_workflow_detached<I>(
        &self,
        info: &crate::info::WorkflowInfo,
        input: I,
        parent_close_policy: crate::types::ParentClosePolicy,
    ) -> HarvestResult<ExecutionId>
    where
        I: serde::Serialize,
    {
        let json_input = serde_json::to_value(input).map_err(HarvestError::Serialization)?;
        self.spawn_child_workflow_detached_raw(info.name, json_input, parent_close_policy)
    }

    // ── Typed dispatch helpers ────────────────────────────────────────────────

    /// Execute an activity using its [`ActivityInfo`] for name, queue, and defaults.
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

    /// Execute a local activity inline on the workflow worker using its [`ActivityInfo`].
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

    /// Spawn a child workflow and await its result using a [`WorkflowInfo`].
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
            HistoryMatch::Diverged {
                expected,
                actual,
                event_index,
            } => Err(self.nd_error(
                format!("signal mismatch: expected {expected}, got {actual}"),
                event_index,
                Some(expected),
                Some(actual),
            )),
            HistoryMatch::Failed { .. }
            | HistoryMatch::TimedOut { .. }
            | HistoryMatch::ActivityInProgress { .. }
            | HistoryMatch::AwaitingExternalCompletion { .. }
            | HistoryMatch::ChildInProgress { .. }
            | HistoryMatch::LocalActivityInProgress { .. }
            | HistoryMatch::ExternalSignalInProgress { .. }
            | HistoryMatch::ExternalSignalFailed { .. }
            | HistoryMatch::ExternalCancelInProgress { .. }
            | HistoryMatch::ExternalCancelFailed { .. }
            | HistoryMatch::DetachedChildSpawned { .. } => {
                let actual = format!("{history_match:?}");
                Err(self.nd_error(
                    "signal history contains unexpected failure".to_string(),
                    self.match_history(|m| i32::try_from(m.position()).ok()),
                    Some("WaitForSignal".to_string()),
                    Some(actual),
                ))
            }
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

    /// Wait for a named signal, but give up after `timeout` and return `None`.
    ///
    /// This is the durable signal-or-deadline primitive for human-in-the-loop
    /// and callback-driven flows (issue #476): approval gates, payment
    /// confirmations, webhook callbacks with an SLA. Resolves to
    /// `Ok(Some(payload))` when the signal arrives before the deadline and
    /// `Ok(None)` when the durable timer fires first.
    ///
    /// # Determinism contract
    ///
    /// The race composes the existing `TimerStarted`/`TimerFired` and
    /// `SignalReceived` events — no new event variant. The winner is decided
    /// by **recorded history order**: whichever of `SignalReceived` or
    /// `TimerFired` appears first in history wins on every replay, regardless
    /// of wall-clock timing on the replaying worker. A history containing both
    /// events always replays to the same branch.
    ///
    /// If the timer wins, no signal payload is consumed: a later delivery of
    /// that signal remains observable by a subsequent `receive_signal*` /
    /// `wait_for_signal*` call.
    ///
    /// `timeout` is rounded **up** to whole seconds (durable timers are
    /// second-granular).
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::NonDeterministic`] if replay history diverges
    /// from the requested race, or [`HarvestError::Cancelled`] if the workflow
    /// task is dropped before a live resolution arrives.
    ///
    /// # Panics
    ///
    /// Panics if the internal matcher or commands mutex is poisoned.
    pub async fn wait_for_signal_timeout(
        &self,
        signal_name: &str,
        timeout: std::time::Duration,
    ) -> HarvestResult<Option<Value>> {
        self.wait_for_signal_timeout_with_timer_id(signal_name, timeout)
            .await
            .map(|(value, _timer_id)| value)
    }

    /// Same race as [`Self::wait_for_signal_timeout`], but also returns the
    /// deterministic durable timer ID the race armed. Used internally by
    /// [`Self::race_timer_signal_impl`] (issue #600) to durably cancel the
    /// losing timer branch when the signal wins, without re-deriving (and
    /// thereby double-incrementing) the private `signal_timeout_seq` counter.
    async fn wait_for_signal_timeout_with_timer_id(
        &self,
        signal_name: &str,
        timeout: std::time::Duration,
    ) -> HarvestResult<(Option<Value>, String)> {
        use crate::replay::SignalOrTimerMatch;

        // Deterministic timer ID: the counter increments on every call (live
        // and replay alike), so the Nth race in workflow code always carries
        // the same ID as the Nth recorded race timer.
        let seq = {
            let mut seq = self
                .signal_timeout_seq
                .lock()
                .expect("signal_timeout_seq lock poisoned");
            *seq += 1;
            *seq
        };
        let timer_id = format!("__signal_timeout:{seq}:{signal_name}");
        // Round up so a sub-second timeout still arms a durable timer
        // (saturating: Duration::MAX must not overflow u64 seconds).
        let duration_secs = timeout
            .as_secs()
            .saturating_add(u64::from(timeout.subsec_nanos() > 0));

        let history_match = self.match_history(|m| {
            m.match_signal_or_timer(signal_name, &timer_id, Some(duration_secs))
        });

        let result = match history_match {
            SignalOrTimerMatch::SignalWon { payload } => Some(payload),
            SignalOrTimerMatch::TimerWon => {
                // Advance the virtual clock (same coupling as timer() above).
                // Signal-won path advances nothing — no TimerStarted event recorded.
                #[cfg(any(test, feature = "testing"))]
                self.advance_timer_clock(duration_secs);
                None
            }
            SignalOrTimerMatch::Diverged {
                expected,
                actual,
                event_index,
            } => {
                return Err(self.nd_error(
                    format!("signal-or-timeout mismatch: expected {expected}, got {actual}"),
                    event_index,
                    Some(expected),
                    Some(actual),
                ));
            }
            outcome @ (SignalOrTimerMatch::NoMatch | SignalOrTimerMatch::InProgress) => {
                if matches!(outcome, SignalOrTimerMatch::NoMatch) {
                    self.check_strict_replay_no_match(&format!(
                        "SignalOrTimer({signal_name}, {timer_id})"
                    ))?;
                } else if self.strict_replay {
                    // Strict replay (WorkflowReplayer) always gets complete
                    // histories — an unresolved race is a fixture problem.
                    return Err(self.nd_error(
                        format!("signal-or-timeout race '{signal_name}' started but unresolved in history"),
                        self.match_history(|m| i32::try_from(m.position()).ok()),
                        Some("SignalOrTimerResolved".to_string()),
                        Some("SignalOrTimerInProgress".to_string()),
                    ));
                }

                // First live run (NoMatch) or re-park after a spurious wake
                // (InProgress): start/refresh the durable timer — the worker
                // dedupes the timer row by timer_id, so re-emitting is safe —
                // and register the signal wait. The first event the worker
                // records resolves the race on the next replay.
                let (timer_tx, timer_rx) = oneshot::channel();
                let (signal_tx, signal_rx) = oneshot::channel();
                self.push_command(WorkflowCommand::StartTimer {
                    timer_id: TimerId::new(&timer_id),
                    duration_secs,
                    result_tx: timer_tx,
                });
                self.push_command(WorkflowCommand::WaitForSignal {
                    signal_name: signal_name.to_string(),
                    result_tx: signal_tx,
                });

                SignalOrTimerRaceFut {
                    signal_name: signal_name.to_string(),
                    signal_rx,
                    timer_rx,
                    signal_gone: false,
                    timer_gone: false,
                }
                .await?
            }
        };
        Ok((result, timer_id))
    }

    /// Wait for a signal with a deadline and deserialize its payload into `O`.
    ///
    /// This is the typed alternative to
    /// [`wait_for_signal_timeout`](Self::wait_for_signal_timeout), mirroring
    /// the existing [`receive_signal`](Self::receive_signal) /
    /// [`wait_for_signal`](Self::wait_for_signal) pairing. Resolves to
    /// `Ok(Some(payload))` when the signal arrives before the deadline and
    /// `Ok(None)` when the deadline fires first.
    ///
    /// ```rust,ignore
    /// // Await approval, else auto-reject after 24 hours:
    /// match ctx.receive_signal_timeout::<Decision>("approval", Duration::from_secs(86_400)).await? {
    ///     Some(decision) => apply(decision),
    ///     None => auto_reject(),
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Serialization`] if the signal payload cannot be
    /// deserialized. Propagates all errors from
    /// [`wait_for_signal_timeout`](Self::wait_for_signal_timeout).
    pub async fn receive_signal_timeout<O>(
        &self,
        signal_name: &str,
        timeout: std::time::Duration,
    ) -> HarvestResult<Option<O>>
    where
        O: serde::de::DeserializeOwned,
    {
        match self.wait_for_signal_timeout(signal_name, timeout).await? {
            Some(raw) => Ok(Some(serde_json::from_value(raw)?)),
            None => Ok(None),
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
        self.signal_external_workflow_with_idempotency(target, signal_name, payload, None)
            .await
    }

    /// Send a named signal to another workflow with an opt-in exactly-once
    /// delivery key.
    ///
    /// When `idempotency_key` is `Some`, the target's delivery insert is
    /// deduplicated against `uq_harvest_signals_idem`, so the cross-shard outbox
    /// and any crash-recovery re-delivery land at most one `SignalReceived`
    /// event. The key is persisted in the `ExternalSignalRequested` event and
    /// reused verbatim on replay/recovery; `None` is legacy at-least-once.
    ///
    /// # Errors
    ///
    /// Same as [`signal_external_workflow`](Self::signal_external_workflow).
    ///
    /// # Panics
    ///
    /// Panics if the internal matcher or commands mutex is poisoned.
    pub async fn signal_external_workflow_with_idempotency<P: serde::Serialize>(
        &self,
        target: ExecutionId,
        signal_name: &str,
        payload: P,
        idempotency_key: impl Into<Option<String>>,
    ) -> HarvestResult<()> {
        use crate::replay::HistoryMatch;

        let idempotency_key = idempotency_key.into();
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

            HistoryMatch::Diverged {
                expected,
                actual,
                event_index,
            } => Err(self.nd_error(
                format!("external signal mismatch: expected {expected}, got {actual}"),
                event_index,
                Some(expected),
                Some(actual),
            )),

            // Crash-recovery: ExternalSignalRequested is already durable; re-attempt delivery
            // using the recorded payload so the target receives the same data regardless of
            // any code changes to the payload expression between the crash and recovery.
            HistoryMatch::ExternalSignalInProgress {
                signal_id,
                payload: recorded_payload,
                idempotency_key: recorded_idempotency_key,
            } => {
                // Reuse the recorded key (not the current argument) so a code
                // change to the key expression cannot diverge an in-flight
                // delivery that the outbox may resolve.
                self.dispatch_signal_command(
                    target,
                    signal_name,
                    recorded_payload,
                    signal_id,
                    true,
                    recorded_idempotency_key,
                )
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
                    idempotency_key,
                )
                .await
            }

            HistoryMatch::Failed { .. }
            | HistoryMatch::ActivityInProgress { .. }
            | HistoryMatch::AwaitingExternalCompletion { .. }
            | HistoryMatch::ChildInProgress { .. }
            | HistoryMatch::LocalActivityInProgress { .. }
            | HistoryMatch::TimedOut { .. }
            | HistoryMatch::ExternalCancelInProgress { .. }
            | HistoryMatch::ExternalCancelFailed { .. }
            | HistoryMatch::DetachedChildSpawned { .. } => {
                unreachable!(
                    "match_external_signal never returns Failed, ActivityInProgress, \
                     AwaitingExternalCompletion, ChildInProgress, LocalActivityInProgress, \
                     TimedOut, or DetachedChildSpawned"
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
        idempotency_key: Option<String>,
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
            idempotency_key,
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

    /// Request cancellation of a sibling workflow execution (issue #492).
    ///
    /// Deterministic, replay-safe primitive. On the first live call the worker
    /// appends `ExternalCancelRequested`, attempts to cancel the target via
    /// `execution::cancel_workflow_execution`, and appends either
    /// `ExternalCancelDelivered` or `ExternalCancelFailed`. On replay the recorded
    /// outcome is returned immediately without re-contacting the target.
    ///
    /// # Cancel semantics vs signal
    ///
    /// Unlike `signal_external_workflow`, an already-terminal target is a
    /// **no-op success** (`ExternalCancelDelivered`): the goal (target not running)
    /// is already met. Only a target that cannot be found within the grace window
    /// resolves as `ExternalCancelFailed { reason_code: "target_unknown" }`.
    ///
    /// # Self-cancel
    ///
    /// Passing `self.exec_id()` as `target` returns `HarvestError::ExternalCancelFailed`
    /// with `reason_code = "self_cancel"` immediately (deterministic, same every replay).
    ///
    /// # Errors
    ///
    /// - [`HarvestError::ExternalCancelFailed`] with `reason_code = "self_cancel"` when
    ///   `target == self.exec_id()`.
    /// - [`HarvestError::ExternalCancelFailed`] with `reason_code = "target_unknown"`
    ///   if no execution with `target` is found within the grace window.
    /// - [`HarvestError::NonDeterministic`] if the history at this position does not
    ///   match the requested target.
    /// - [`HarvestError::Cancelled`] if the result channel is dropped before the worker
    ///   resolves this command.
    ///
    /// # Panics
    ///
    /// Panics if the internal matcher or commands mutex is poisoned.
    pub async fn request_cancel_external_workflow(&self, target: ExecutionId) -> HarvestResult<()> {
        use crate::replay::HistoryMatch;

        // Self-cancel is always an immediate deterministic error. This path records
        // no history, so the `cancel_id` must be a stable sentinel (nil UUID) rather
        // than a fresh v4 — otherwise a workflow that surfaces the error (e.g.
        // `e.to_string()` into its output) would diverge on replay.
        if target == self.exec_id {
            return Err(HarvestError::ExternalCancelFailed {
                cancel_id: ExternalCancelId::from_uuid(uuid::Uuid::nil()),
                target,
                reason_code: "self_cancel".to_string(),
            });
        }

        let history_match = self.match_history(|m| m.match_external_cancel(target));

        match history_match {
            HistoryMatch::Matched { .. } => Ok(()),

            HistoryMatch::ExternalCancelFailed {
                cancel_id,
                reason_code,
            } => Err(HarvestError::ExternalCancelFailed {
                cancel_id,
                target,
                reason_code,
            }),

            HistoryMatch::Diverged {
                expected,
                actual,
                event_index,
            } => Err(self.nd_error(
                format!("external cancel mismatch: expected {expected}, got {actual}"),
                event_index,
                Some(expected),
                Some(actual),
            )),

            // Crash-recovery: ExternalCancelRequested is already durable; re-attempt delivery.
            HistoryMatch::ExternalCancelInProgress { cancel_id } => {
                self.dispatch_cancel_command(target, cancel_id, true).await
            }

            // First live call: generate a new cancel_id and dispatch.
            HistoryMatch::NoMatch => {
                self.check_strict_replay_no_match(&format!(
                    "ExternalCancelRequested(target={target})"
                ))?;
                self.dispatch_cancel_command(target, ExternalCancelId::new(), false)
                    .await
            }

            HistoryMatch::Failed { .. }
            | HistoryMatch::ActivityInProgress { .. }
            | HistoryMatch::AwaitingExternalCompletion { .. }
            | HistoryMatch::ChildInProgress { .. }
            | HistoryMatch::LocalActivityInProgress { .. }
            | HistoryMatch::TimedOut { .. }
            | HistoryMatch::DetachedChildSpawned { .. }
            | HistoryMatch::ExternalSignalInProgress { .. }
            | HistoryMatch::ExternalSignalFailed { .. } => {
                unreachable!(
                    "match_external_cancel never returns Failed, ActivityInProgress, \
                     AwaitingExternalCompletion, ChildInProgress, LocalActivityInProgress, \
                     TimedOut, DetachedChildSpawned, ExternalSignalInProgress, or ExternalSignalFailed"
                )
            }
        }
    }

    /// Push a `RequestCancelExternalWorkflow` command and await its resolution.
    ///
    /// Shared by the crash-recovery (`already_requested = true`) and first-call
    /// (`already_requested = false`) dispatch paths.
    async fn dispatch_cancel_command(
        &self,
        target: ExecutionId,
        cancel_id: ExternalCancelId,
        already_requested: bool,
    ) -> HarvestResult<()> {
        let (tx, rx) = oneshot::channel();
        self.push_command(WorkflowCommand::RequestCancelExternalWorkflow {
            cancel_id,
            target,
            result_tx: tx,
            already_requested,
        });
        match rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(reason_code)) => Err(HarvestError::ExternalCancelFailed {
                cancel_id,
                target,
                reason_code,
            }),
            Err(_) => Err(HarvestError::Cancelled(format!(
                "cancel of {target}: result channel dropped"
            ))),
        }
    }

    // ── Fan-out / parallel activities (issue #359) ───────────────────────────

    /// Generate the next fan-out sequence number for marker naming.
    ///
    /// **Numbers are never given back, even if the call that allocated one
    /// goes on to fail before recording a marker** (e.g. a fresh dispatch's
    /// payload-cap validation fails, or the workflow catches a fan-out
    /// error and runs another fan-out). An earlier revision tried releasing
    /// and reusing the number in that case; that let a *different*,
    /// unrelated fan-out call site inherit the failed call's number, so on
    /// replay the failed call's `peek_fan_out_count` could match against
    /// the *other* call's marker/count (or even, in the collect-all-error
    /// case, its recorded children) — a **silent misattribution**, strictly
    /// worse than the plain "expected `fan_out:N`, got `fan_out:M`" divergence
    /// burning the number produces. A caught-and-continued fan-out failure
    /// simply not replaying cleanly is an accepted instance of the broader,
    /// pre-existing, engine-wide limitation documented on
    /// `known_limitation_early_config_dependent_failure_does_not_replay_cleanly`
    /// (`tests/replayer_tests.rs`) — not something a numbering trick should
    /// try to paper over.
    fn next_fan_out_seq(&self) -> u32 {
        let mut seq = self.fan_out_seq.lock().expect("fan_out_seq lock poisoned");
        *seq += 1;
        *seq
    }

    /// Record or verify the fan-out count in event history.
    ///
    /// On **live execution** (past end of history): pushes a `RecordMarker`
    /// command so future replays can verify the collection length, and
    /// returns `Ok(true)` — this is the very first time this fan-out group
    /// has ever been dispatched, which callers use to gate one-time,
    /// pre-dispatch validation that must never re-run on replay.
    ///
    /// On **replay**: matches the `MarkerRecorded` event and compares the
    /// recorded count with the current count, returning `Ok(false)` on a
    /// match. Returns a [`HarvestError::NonDeterministic`] when they differ.
    ///
    /// Used as-is by the activity fan-out methods, which have no pre-dispatch
    /// validation step to sequence around the marker. Child fan-out instead
    /// uses [`peek_fan_out_count`](Self::peek_fan_out_count) +
    /// [`record_fan_out_marker`](Self::record_fan_out_marker) so the marker
    /// is recorded only *after* payload validation succeeds — see those
    /// methods' docs for why.
    fn check_fan_out_count(&self, seq: u32, count: usize) -> HarvestResult<bool> {
        let fresh_dispatch = self.peek_fan_out_count(seq, count)?;
        if fresh_dispatch {
            self.record_fan_out_marker(seq, count);
        }
        Ok(fresh_dispatch)
    }

    /// Check the fan-out count against event history **without** pushing the
    /// `RecordMarker` command on a fresh dispatch.
    ///
    /// Splitting this out of [`check_fan_out_count`](Self::check_fan_out_count)
    /// lets a caller run additional validation (e.g. the child fan-out
    /// payload-cap pre-check) *between* determining "this is a fresh
    /// dispatch" and actually committing the `fan_out:{n}` marker to the
    /// command list — so a validation failure discovered in between never
    /// leaves a persisted marker with no corresponding dispatch attempt. A
    /// terminal execution whose history contains a `fan_out:{n}` marker but
    /// no children would otherwise be replayed by matching the marker
    /// (`fresh_dispatch == false`) and then diverging when it tries to
    /// re-derive a `ChildWorkflowStarted` that was never recorded.
    fn peek_fan_out_count(&self, seq: u32, count: usize) -> HarvestResult<bool> {
        let marker_result = self.match_history(|m| m.match_fan_out_marker(seq, count));
        match marker_result {
            HistoryMatch::NoMatch => Ok(true),
            HistoryMatch::Matched { .. } => Ok(false),
            HistoryMatch::Diverged {
                expected,
                actual,
                event_index,
            } => Err(self.nd_error(
                format!(
                    "fan_out #{seq}: history has {expected} but current code supplies {actual} — \
                         the input collection changed size between deploy and replay; \
                         use ctx.version() to guard this fan-out if the size change is intentional"
                ),
                event_index,
                Some(expected),
                Some(actual),
            )),
            _ => unreachable!("match_fan_out_marker only returns Matched, NoMatch, or Diverged"),
        }
    }

    /// Push the `RecordMarker { name: "fan_out:{n}" }` command. Only called
    /// on a fresh dispatch (`peek_fan_out_count` returned `true`), and only
    /// once the caller has confirmed the whole group can actually be
    /// attempted (see [`peek_fan_out_count`](Self::peek_fan_out_count)).
    fn record_fan_out_marker(&self, seq: u32, count: usize) {
        self.push_command(WorkflowCommand::RecordMarker {
            name: format!("fan_out:{seq}"),
            details: Value::from(count as u64),
        });
    }

    /// Record or verify a condition-skip decision for a DAG node in event history.
    ///
    /// Called by the unified-DAG dispatch loop when a node's condition predicate
    /// returns `false`.  On **live execution** (past end of history) it pushes a
    /// `RecordMarker` command so future replays know the branch decision.  On
    /// **replay** it matches the existing `MarkerRecorded` event; if the event
    /// at the cursor position is something else (the condition returned a
    /// different value than during the first run), a `NonDeterministic` error is
    /// returned.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::NonDeterministic`] when the recorded history
    /// diverges from the current code — the condition predicate is not a pure
    /// function of upstream outputs, or a code change altered the branch
    /// decision for an in-flight execution.
    pub fn dag_skip_marker(
        &self,
        task_index: usize,
        activity_name: &str,
        upstreams: &[usize],
    ) -> HarvestResult<()> {
        let marker_name = format!("dag_skip:{task_index}");
        let match_result =
            self.match_history(|m| m.match_named_marker(&marker_name, activity_name, upstreams));
        match match_result {
            HistoryMatch::NoMatch => {
                self.push_command(WorkflowCommand::RecordMarker {
                    name: marker_name,
                    details: serde_json::json!({
                        "task": activity_name,
                        "reason": "condition_false",
                        "upstreams": upstreams,
                    }),
                });
                Ok(())
            }
            HistoryMatch::Matched { .. } => Ok(()),
            HistoryMatch::Diverged {
                expected,
                actual,
                event_index,
            } => Err(self.nd_error(
                format!(
                    "dag condition skip for task {task_index} ({activity_name}) diverged from \
                     history — the condition predicate must be a pure function of upstream outputs \
                     and must not change across deploys for in-flight executions; \
                     use ctx.version() to guard branch changes: \
                     expected {expected}, got {actual}"
                ),
                event_index,
                Some(expected),
                Some(actual),
            )),
            _ => unreachable!("match_named_marker only returns Matched, NoMatch, or Diverged"),
        }
    }

    /// Execute N activities **in parallel** (fail-fast variant).
    ///
    /// Dispatches every `(name, input, queue)` tuple concurrently and returns
    /// a `Vec` of outputs in the **same order as the input slice**, regardless
    /// of completion order.  Returns on the **first** activity failure — all
    /// sibling activities are still recorded in history and their results are
    /// replayed correctly, but the workflow function receives only the first
    /// error.
    ///
    /// # Replay safety
    ///
    /// A `MarkerRecorded { name: "fan_out:{n}", details: <count> }` event is
    /// appended immediately before the activity events on the first live run.
    /// On replay the count is verified; if the input collection has grown or
    /// shrunk since the original run,
    /// [`HarvestError::NonDeterministic`] is returned before any activity is
    /// dispatched.
    ///
    /// The input collection **must** be derived from already-recorded state
    /// (workflow input, prior activity outputs, signals) — never from
    /// non-deterministic sources such as the system clock or a random number.
    ///
    /// # Cancellation
    ///
    /// Checks `is_cancelled()` before dispatching.  Returns
    /// [`HarvestError::Cancelled`] immediately when the workflow has been
    /// cancelled.
    ///
    /// # Errors
    ///
    /// - [`HarvestError::NonDeterministic`] if `activities.len()` differs
    ///   from the count recorded in history.
    /// - [`HarvestError::Cancelled`] if the workflow was cancelled.
    /// - [`HarvestError::ActivityFailed`] (or other activity error) on the
    ///   first failure in the group.
    ///
    /// # Panics
    ///
    /// Panics if the internal matcher or commands mutex is poisoned.
    pub async fn execute_activity_fan_out_raw(
        &self,
        activities: Vec<(String, Value, String)>,
    ) -> HarvestResult<Vec<Value>> {
        self.fan_out_raw_impl(activities, None, None).await
    }

    async fn fan_out_raw_impl(
        &self,
        activities: Vec<(String, Value, String)>,
        retry: Option<crate::policy::RetryPolicy>,
        timeout: Option<std::time::Duration>,
    ) -> HarvestResult<Vec<Value>> {
        self.check_cancellation()?;

        let seq = self.next_fan_out_seq();
        let count = activities.len();
        let _fresh_dispatch = self.check_fan_out_count(seq, count)?;

        if activities.is_empty() {
            return Ok(Vec::new());
        }

        let futures: Vec<_> = activities
            .into_iter()
            .map(|(name, input, queue)| {
                let retry = retry.clone();
                async move {
                    self.execute_activity_raw_with_opts(&name, input, &queue, retry, timeout)
                        .await
                }
            })
            .collect();

        futures::future::try_join_all(futures).await
    }

    /// Execute N activities **in parallel** (collect-all variant).
    ///
    /// Dispatches every `(name, input, queue)` tuple concurrently and returns
    /// a `Vec<Result<Value, String>>` in the **same order as the input slice**.
    /// Unlike [`execute_activity_fan_out_raw`](Self::execute_activity_fan_out_raw),
    /// **all** activities run to completion regardless of failures — per-slot
    /// errors are captured in the returned `Err` variants rather than aborting
    /// the fan-out early.
    ///
    /// # Replay safety
    ///
    /// Same count-marker semantics as the fail-fast variant.
    ///
    /// # Cancellation
    ///
    /// Checks `is_cancelled()` before dispatching.  Returns
    /// `Err(HarvestError::Cancelled)` immediately when the workflow has been
    /// cancelled.
    ///
    /// # Errors
    ///
    /// - [`HarvestError::NonDeterministic`] if `activities.len()` differs
    ///   from the count recorded in history.
    /// - [`HarvestError::Cancelled`] if the workflow was cancelled.
    ///
    /// Individual per-slot failures are returned as `Err(String)` inside the
    /// `Vec`; the outer `Result` only fails for engine-level errors.
    ///
    /// # Panics
    ///
    /// Panics if the internal matcher or commands mutex is poisoned.
    pub async fn execute_activity_fan_out_collect_raw(
        &self,
        activities: Vec<(String, Value, String)>,
    ) -> HarvestResult<Vec<Result<Value, String>>> {
        self.fan_out_collect_raw_impl(activities, None, None).await
    }

    async fn fan_out_collect_raw_impl(
        &self,
        activities: Vec<(String, Value, String)>,
        retry: Option<crate::policy::RetryPolicy>,
        timeout: Option<std::time::Duration>,
    ) -> HarvestResult<Vec<Result<Value, String>>> {
        self.check_cancellation()?;

        let seq = self.next_fan_out_seq();
        let count = activities.len();
        let _fresh_dispatch = self.check_fan_out_count(seq, count)?;

        if activities.is_empty() {
            return Ok(Vec::new());
        }

        let futures: Vec<_> = activities
            .into_iter()
            .map(|(name, input, queue)| {
                let retry = retry.clone();
                async move {
                    match self
                        .execute_activity_raw_with_opts(&name, input, &queue, retry, timeout)
                        .await
                    {
                        Ok(v) => Ok(Ok(v)),
                        Err(
                            e
                            @ (HarvestError::ActivityFailed { .. } | HarvestError::Timeout { .. }),
                        ) => Ok(Err(e.to_string())),
                        Err(e) => Err(e),
                    }
                }
            })
            .collect();

        futures::future::try_join_all(futures).await
    }

    /// Typed fail-fast fan-out: run the same activity for every input in
    /// `inputs` in parallel and return the outputs in input order.
    ///
    /// All slots share the same `ActivityInfo` (name, queue, retry defaults).
    /// Use [`execute_activity_fan_out_raw`](Self::execute_activity_fan_out_raw)
    /// for heterogeneous activity names.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Serialization`] if any input cannot be
    /// serialized.  Propagates all errors from
    /// [`execute_activity_fan_out_raw`](Self::execute_activity_fan_out_raw).
    pub async fn execute_activity_fan_out<I, O>(
        &self,
        info: &crate::info::ActivityInfo,
        inputs: Vec<I>,
    ) -> HarvestResult<Vec<O>>
    where
        I: serde::Serialize,
        O: serde::de::DeserializeOwned,
    {
        if info.is_local {
            return Err(HarvestError::Config(format!(
                "activity '{}' is marked local = true; fan-out requires remote activities",
                info.name
            )));
        }
        let queue = info.default_queue.unwrap_or("default").to_string();
        let activities = inputs
            .into_iter()
            .map(|i| {
                let json_input = serde_json::to_value(i)?;
                Ok((info.name.to_string(), json_input, queue.clone()))
            })
            .collect::<Result<Vec<_>, serde_json::Error>>()?;

        let raw_results = self
            .fan_out_raw_impl(
                activities,
                info.default_retry_policy.clone(),
                info.default_start_to_close,
            )
            .await?;
        raw_results
            .into_iter()
            .map(|v| serde_json::from_value(v).map_err(HarvestError::Serialization))
            .collect()
    }

    /// Typed collect-all fan-out: run the same activity for every input in
    /// `inputs` in parallel and return per-slot `Result<O, String>` in input order.
    ///
    /// All slots share the same `ActivityInfo`.  Use
    /// [`execute_activity_fan_out_collect_raw`](Self::execute_activity_fan_out_collect_raw)
    /// for heterogeneous activity names.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Serialization`] if any input cannot be
    /// serialized.  Propagates engine-level errors from
    /// [`execute_activity_fan_out_collect_raw`](Self::execute_activity_fan_out_collect_raw).
    pub async fn execute_activity_fan_out_collect<I, O>(
        &self,
        info: &crate::info::ActivityInfo,
        inputs: Vec<I>,
    ) -> HarvestResult<Vec<Result<O, String>>>
    where
        I: serde::Serialize,
        O: serde::de::DeserializeOwned,
    {
        if info.is_local {
            return Err(HarvestError::Config(format!(
                "activity '{}' is marked local = true; fan-out requires remote activities",
                info.name
            )));
        }
        let queue = info.default_queue.unwrap_or("default").to_string();
        let activities = inputs
            .into_iter()
            .map(|i| {
                let json_input = serde_json::to_value(i)?;
                Ok((info.name.to_string(), json_input, queue.clone()))
            })
            .collect::<Result<Vec<_>, serde_json::Error>>()?;

        let raw_results = self
            .fan_out_collect_raw_impl(
                activities,
                info.default_retry_policy.clone(),
                info.default_start_to_close,
            )
            .await?;
        let typed: Vec<Result<O, String>> = raw_results
            .into_iter()
            .map(|slot| match slot {
                Ok(v) => serde_json::from_value::<O>(v)
                    .map(Ok)
                    .map_err(HarvestError::Serialization),
                Err(e) => Ok(Err(e)),
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(typed)
    }

    // ── Fan-out / parallel child workflows (issue #601) ──────────────────

    /// Validate every child's serialized input against the payload cap
    /// *before any child is dispatched*, on a fresh (first-time) fan-out
    /// dispatch only.
    ///
    /// Without this pre-check, `spawn_child_workflow_raw`'s own per-child
    /// cap enforcement (checked lazily inside its `NoMatch` branch) could
    /// let earlier children in the batch push their `StartChildWorkflow`
    /// command and suspend before a later, oversized child aborts the
    /// whole `try_join_all` with `PayloadTooLarge`. Checking every input up
    /// front makes the dispatch all-or-nothing: either every child is
    /// scheduled, or none are.
    ///
    /// Called with the result of
    /// [`peek_fan_out_count`](Self::peek_fan_out_count) — a validation
    /// failure here means the caller must **not** call
    /// [`record_fan_out_marker`](Self::record_fan_out_marker), so a fresh
    /// dispatch that fails the cap check never leaves a persisted
    /// `fan_out:{n}` marker with no corresponding `ChildWorkflowStarted`
    /// events; a later replay attempt then simply re-derives the same
    /// failure from scratch instead of diverging against a marker that
    /// promised children which were never recorded. Only run when
    /// `fresh_dispatch` is `true` — a replayed fan-out group's children are
    /// matched against already-recorded history and must never re-derive a
    /// pass/fail verdict from the *current* `payload_max_workflow_input`,
    /// which may have changed since the original run.
    fn validate_child_payload_caps(
        &self,
        fresh_dispatch: bool,
        children: &[(String, Value)],
    ) -> HarvestResult<()> {
        if !fresh_dispatch || self.payload_max_workflow_input == 0 {
            return Ok(());
        }
        for (_, input) in children {
            let observed = serde_json::to_string(input).map_or(0, |s| s.len() as u64);
            if observed > self.payload_max_workflow_input {
                return Err(HarvestError::PayloadTooLarge {
                    kind: PayloadKind::ChildWorkflowInput,
                    observed_bytes: observed,
                    cap_bytes: self.payload_max_workflow_input,
                    workflow_type: self.workflow_name.clone(),
                    activity_name: None,
                });
            }
        }
        Ok(())
    }

    /// Spawn N child workflows **in parallel** (fail-fast variant).
    ///
    /// Dispatches every `(workflow_name, input)` pair concurrently — all N
    /// children are scheduled (each gets its own `ExecutionId` on the
    /// parent's shard) before any is awaited — and returns a `Vec` of
    /// outputs in the **same order as the input slice**, regardless of
    /// completion order. Returns on the **first** child failure — sibling
    /// children still complete and are recorded in history, but the
    /// workflow function receives only the first error.
    ///
    /// # Replay safety
    ///
    /// A `MarkerRecorded { name: "fan_out:{n}", details: <count> }` event is
    /// appended immediately before the child events on the first live run —
    /// the identical marker mechanism used by
    /// [`execute_activity_fan_out_raw`](Self::execute_activity_fan_out_raw).
    /// Both share one sequence counter, so `fan_out:{n}` numbering stays
    /// deterministic when activity and child fan-outs are mixed in one
    /// workflow. On replay the count is verified; if the input collection
    /// has grown or shrunk since the original run,
    /// [`HarvestError::NonDeterministic`] is returned before any child is
    /// spawned.
    ///
    /// The input collection **must** be derived from already-recorded state
    /// (workflow input, prior activity outputs, signals) — never from
    /// non-deterministic sources such as the system clock or a random number.
    ///
    /// # Cancellation
    ///
    /// Checks `is_cancelled()` before dispatching. Returns
    /// [`HarvestError::Cancelled`] immediately when the workflow has been
    /// cancelled. Fanned-out children are **awaited** (not detached), so
    /// cancelling the parent after dispatch does **not** propagate to
    /// already-in-flight children — `ParentClosePolicy` (issue #347) is a
    /// detached-child mechanism only; an awaited child (fan-out or a plain
    /// `spawn_child_workflow_raw` call) can outlive a cancelled or
    /// terminated parent, exactly like today's single-child spawn. Use
    /// [`spawn_child_workflow_detached_raw`](Self::spawn_child_workflow_detached_raw)
    /// with an explicit `ParentClosePolicy` if children must be torn down
    /// when the parent closes.
    ///
    /// # Errors
    ///
    /// - [`HarvestError::NonDeterministic`] if `children.len()` differs
    ///   from the count recorded in history.
    /// - [`HarvestError::Cancelled`] if the workflow was cancelled.
    /// - [`HarvestError::ActivityFailed`] (child failures surface with a
    ///   `child-workflow:{name}` activity name) on the first failure in the
    ///   group.
    ///
    /// # Panics
    ///
    /// Panics if the internal matcher or commands mutex is poisoned.
    pub async fn spawn_child_workflow_fan_out_raw(
        &self,
        children: Vec<(String, Value)>,
    ) -> HarvestResult<Vec<Value>> {
        self.check_cancellation()?;

        let seq = self.next_fan_out_seq();
        let count = children.len();
        let fresh_dispatch = self.peek_fan_out_count(seq, count)?;
        self.validate_child_payload_caps(fresh_dispatch, &children)?;
        if fresh_dispatch {
            self.record_fan_out_marker(seq, count);
        }

        if children.is_empty() {
            return Ok(Vec::new());
        }

        let futures: Vec<_> = children
            .into_iter()
            .map(|(workflow_name, input)| async move {
                self.spawn_child_workflow_raw(&workflow_name, input).await
            })
            .collect();

        futures::future::try_join_all(futures).await
    }

    /// Spawn N child workflows **in parallel** (collect-all variant).
    ///
    /// Dispatches every `(workflow_name, input)` pair concurrently and
    /// returns a `Vec<Result<Value, String>>` in the **same order as the
    /// input slice**. Unlike
    /// [`spawn_child_workflow_fan_out_raw`](Self::spawn_child_workflow_fan_out_raw),
    /// **all** children run to completion regardless of failures — per-slot
    /// errors are captured in the returned `Err` variants rather than
    /// aborting the fan-out early.
    ///
    /// # Replay safety
    ///
    /// Same count-marker semantics as the fail-fast variant.
    ///
    /// # Cancellation
    ///
    /// Checks `is_cancelled()` before dispatching. Returns
    /// `Err(HarvestError::Cancelled)` immediately when the workflow has been
    /// cancelled.
    ///
    /// # Errors
    ///
    /// - [`HarvestError::NonDeterministic`] if `children.len()` differs
    ///   from the count recorded in history.
    /// - [`HarvestError::Cancelled`] if the workflow was cancelled.
    ///
    /// Individual per-slot child failures are returned as `Err(String)`
    /// inside the `Vec`; the outer `Result` only fails for engine-level
    /// errors (non-determinism, cancellation).
    ///
    /// # Panics
    ///
    /// Panics if the internal matcher or commands mutex is poisoned.
    pub async fn spawn_child_workflow_fan_out_collect_raw(
        &self,
        children: Vec<(String, Value)>,
    ) -> HarvestResult<Vec<Result<Value, String>>> {
        self.check_cancellation()?;

        let seq = self.next_fan_out_seq();
        let count = children.len();
        let fresh_dispatch = self.peek_fan_out_count(seq, count)?;
        self.validate_child_payload_caps(fresh_dispatch, &children)?;
        if fresh_dispatch {
            self.record_fan_out_marker(seq, count);
        }

        if children.is_empty() {
            return Ok(Vec::new());
        }

        let futures: Vec<_> = children
            .into_iter()
            .map(|(workflow_name, input)| async move {
                match self.spawn_child_workflow_raw(&workflow_name, input).await {
                    Ok(v) => Ok(Ok(v)),
                    // Mirrors the activity fan-out sibling's `ActivityFailed | Timeout`
                    // classification for defensive symmetry. `spawn_child_workflow_raw`
                    // cannot currently produce `Timeout` (its `HistoryMatch::TimedOut`
                    // arm is `unreachable!()`), but matching it here means a future
                    // child-level timeout path degrades to a per-slot failure instead
                    // of silently reverting to aborting the whole collect-all batch.
                    Err(
                        e @ (HarvestError::ActivityFailed { .. } | HarvestError::Timeout { .. }),
                    ) => Ok(Err(e.to_string())),
                    Err(e) => Err(e),
                }
            })
            .collect();

        futures::future::try_join_all(futures).await
    }

    /// Typed fail-fast fan-out: spawn the same child workflow type for every
    /// input in `inputs` in parallel and return the outputs in input order.
    ///
    /// All slots share the same `WorkflowInfo` (workflow name). Use
    /// [`spawn_child_workflow_fan_out_raw`](Self::spawn_child_workflow_fan_out_raw)
    /// for heterogeneous child workflow types.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Serialization`] if any input cannot be
    /// serialized. Propagates all errors from
    /// [`spawn_child_workflow_fan_out_raw`](Self::spawn_child_workflow_fan_out_raw).
    pub async fn spawn_child_workflow_fan_out<I, O>(
        &self,
        info: &crate::info::WorkflowInfo,
        inputs: Vec<I>,
    ) -> HarvestResult<Vec<O>>
    where
        I: serde::Serialize,
        O: serde::de::DeserializeOwned,
    {
        let children = inputs
            .into_iter()
            .map(|i| {
                let json_input = serde_json::to_value(i)?;
                Ok((info.name.to_string(), json_input))
            })
            .collect::<Result<Vec<_>, serde_json::Error>>()?;

        let raw_results = self.spawn_child_workflow_fan_out_raw(children).await?;
        raw_results
            .into_iter()
            .map(|v| serde_json::from_value(v).map_err(HarvestError::Serialization))
            .collect()
    }

    /// Typed collect-all fan-out: spawn the same child workflow type for
    /// every input in `inputs` in parallel and return per-slot
    /// `Result<O, String>` in input order.
    ///
    /// All slots share the same `WorkflowInfo`. Use
    /// [`spawn_child_workflow_fan_out_collect_raw`](Self::spawn_child_workflow_fan_out_collect_raw)
    /// for heterogeneous child workflow types.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Serialization`] if any input cannot be
    /// serialized. Propagates engine-level errors from
    /// [`spawn_child_workflow_fan_out_collect_raw`](Self::spawn_child_workflow_fan_out_collect_raw).
    pub async fn spawn_child_workflow_fan_out_collect<I, O>(
        &self,
        info: &crate::info::WorkflowInfo,
        inputs: Vec<I>,
    ) -> HarvestResult<Vec<Result<O, String>>>
    where
        I: serde::Serialize,
        O: serde::de::DeserializeOwned,
    {
        let children = inputs
            .into_iter()
            .map(|i| {
                let json_input = serde_json::to_value(i)?;
                Ok((info.name.to_string(), json_input))
            })
            .collect::<Result<Vec<_>, serde_json::Error>>()?;

        let raw_results = self
            .spawn_child_workflow_fan_out_collect_raw(children)
            .await?;
        let typed: Vec<Result<O, String>> = raw_results
            .into_iter()
            .map(|slot| match slot {
                Ok(v) => serde_json::from_value::<O>(v)
                    .map(Ok)
                    .map_err(HarvestError::Serialization),
                Err(e) => Ok(Err(e)),
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(typed)
    }

    // ── Race / select (issue #600) ───────────────────────────────────────────

    /// Generate the next race sequence number for marker naming (mirrors
    /// `next_fan_out_seq`).
    fn next_race_seq(&self) -> u32 {
        let mut seq = self.race_seq.lock().expect("race_seq lock poisoned");
        *seq += 1;
        *seq
    }

    /// Start building a deterministic race between several ctx-managed
    /// awaitables (issue #600) — see [`RaceBuilder`] for the supported shapes,
    /// determinism contract, and cancellation semantics.
    #[must_use]
    pub const fn race(&self) -> RaceBuilder<'_> {
        RaceBuilder {
            ctx: self,
            branches: Vec::new(),
            pending_error: None,
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn race_impl(&self, branches: Vec<RaceBranch>) -> HarvestResult<RaceWinner> {
        self.check_cancellation()?;

        if branches.is_empty() {
            return Err(HarvestError::Config(
                "ctx.race() requires at least one branch".to_string(),
            ));
        }

        let all_activity = branches
            .iter()
            .all(|b| matches!(b.kind, RaceBranchKind::Activity { .. }));
        let all_child = branches
            .iter()
            .all(|b| matches!(b.kind, RaceBranchKind::ChildWorkflow { .. }));
        let is_timer_signal_pair = branches.len() == 2
            && branches
                .iter()
                .any(|b| matches!(b.kind, RaceBranchKind::Timer { .. }))
            && branches
                .iter()
                .any(|b| matches!(b.kind, RaceBranchKind::Signal { .. }));

        if is_timer_signal_pair {
            return self.race_timer_signal_impl(branches).await;
        }

        if !all_activity && !all_child {
            return Err(HarvestError::Config(
                "ctx.race() only supports a homogeneous race of activity branches, a \
                 homogeneous race of child-workflow branches, or exactly one timer branch \
                 paired with exactly one signal branch in this release — mixing branch kinds \
                 (e.g. an activity racing a timer) is out of scope for issue #600's initial \
                 slice because the worker's suspension-persistence layer does not yet support \
                 a fully heterogeneous mixed-command batch. Bound an individual activity with \
                 its own start_to_close/schedule_to_close timeout, or use \
                 receive_signal_timeout for a signal-or-deadline race, instead."
                    .to_string(),
            ));
        }

        let seq = self.next_race_seq();
        let count = branches.len();
        let open_marker = format!("race:{seq}");
        // Deferred rather than pushed immediately: if a branch that is about
        // to be freshly dispatched fails its payload-cap check below, this
        // race must leave zero durable trace of the cycle (see the
        // validation pass after Phase A) -- otherwise a terminal failure
        // mapped from that error would persist this marker with no
        // corresponding branch dispatch event, and replay would diverge
        // expecting ActivityScheduled/ChildWorkflowStarted but finding the
        // terminal event instead.
        let needs_open_marker =
            match self.match_history(|m| m.match_u64_marker(&open_marker, count as u64)) {
                HistoryMatch::Matched { .. } => false,
                HistoryMatch::NoMatch => true,
                HistoryMatch::Diverged {
                    expected,
                    actual,
                    event_index,
                } => {
                    return Err(self.nd_error(
                    format!(
                        "race #{seq}: history has {expected} but current code supplies {actual} \
                         — the branch count changed between deploy and replay; use ctx.version() \
                         to guard this race if the change is intentional"
                    ),
                    event_index,
                    Some(expected),
                    Some(actual),
                ));
                }
                _ => unreachable!("match_u64_marker only returns Matched, NoMatch, or Diverged"),
            };

        // Phase A: synchronously check every branch's history state without
        // pushing any command yet, so a branch that already has a terminal
        // recorded (the common case on any replay after the race resolves)
        // never re-emits a stray dispatch/wait command for a cancelled sibling.
        let mut resolved: Vec<(usize, HarvestResult<Value>)> = Vec::new();
        let mut to_dispatch: Vec<RaceDispatch> = Vec::new();

        for (index, branch) in branches.iter().enumerate() {
            match &branch.kind {
                RaceBranchKind::Activity { name, input, .. } => {
                    let history_match = if self.strict_replay {
                        self.match_history(|m| m.match_activity_strict(name, input))
                    } else {
                        self.match_history(|m| m.match_activity(name))
                    };
                    match history_match {
                        HistoryMatch::Matched { output } => resolved.push((index, Ok(output))),
                        HistoryMatch::Failed {
                            error,
                            attempt,
                            error_type,
                            details,
                        } => resolved.push((
                            index,
                            Err(HarvestError::ActivityFailed {
                                name: name.clone(),
                                attempt,
                                error_type,
                                details,
                                source: error.into(),
                            }),
                        )),
                        HistoryMatch::TimedOut { timeout_type } => resolved.push((
                            index,
                            Err(HarvestError::Timeout {
                                timeout_type,
                                task_name: name.clone(),
                            }),
                        )),
                        HistoryMatch::Diverged {
                            expected,
                            actual,
                            event_index,
                        } => {
                            return Err(self.nd_error(
                                format!(
                                    "race #{seq} branch {index} ({name}): activity mismatch: \
                                     expected {expected}, got {actual}"
                                ),
                                event_index,
                                Some(expected),
                                Some(actual),
                            ));
                        }
                        HistoryMatch::ActivityInProgress { activity_id } => {
                            to_dispatch.push(RaceDispatch {
                                index,
                                activity_id: Some(activity_id),
                                child_id: None,
                                is_new: false,
                            });
                        }
                        HistoryMatch::NoMatch => {
                            to_dispatch.push(RaceDispatch {
                                index,
                                activity_id: Some(self.next_activity_id()),
                                child_id: None,
                                is_new: true,
                            });
                        }
                        _ => unreachable!("match_activity never returns this variant"),
                    }
                }
                RaceBranchKind::ChildWorkflow {
                    workflow_name,
                    input,
                } => {
                    let history_match =
                        self.match_history(|m| m.match_child_workflow(workflow_name, input));
                    match history_match {
                        HistoryMatch::Matched { output } => resolved.push((index, Ok(output))),
                        HistoryMatch::Failed {
                            error,
                            attempt,
                            error_type,
                            details,
                        } => resolved.push((
                            index,
                            Err(HarvestError::ActivityFailed {
                                name: format!("child-workflow:{workflow_name}"),
                                attempt,
                                error_type,
                                details,
                                source: error.into(),
                            }),
                        )),
                        HistoryMatch::Diverged {
                            expected,
                            actual,
                            event_index,
                        } => {
                            return Err(self.nd_error(
                                format!(
                                    "race #{seq} branch {index} ({workflow_name}): child \
                                     workflow mismatch: expected {expected}, got {actual}"
                                ),
                                event_index,
                                Some(expected),
                                Some(actual),
                            ));
                        }
                        HistoryMatch::ChildInProgress { child_id } => {
                            to_dispatch.push(RaceDispatch {
                                index,
                                activity_id: None,
                                child_id: Some(child_id),
                                is_new: false,
                            });
                        }
                        HistoryMatch::NoMatch => {
                            to_dispatch.push(RaceDispatch {
                                index,
                                activity_id: None,
                                child_id: Some(ExecutionId::new()),
                                is_new: true,
                            });
                        }
                        _ => unreachable!("match_child_workflow never returns this variant"),
                    }
                }
                RaceBranchKind::Timer { .. } | RaceBranchKind::Signal { .. } => {
                    unreachable!("timer/signal branches only occur in the paired shape")
                }
            }
        }

        // Validate payload caps for every branch about to be freshly
        // dispatched *before* pushing any bookkeeping for this cycle (the
        // open marker below, or a branch's own schedule/start command in
        // Phase B). An oversized branch must abort this race with zero
        // durable trace -- see the comment on `needs_open_marker` above.
        for dispatch in &to_dispatch {
            if !dispatch.is_new {
                continue;
            }
            match &branches[dispatch.index].kind {
                RaceBranchKind::Activity { name, input, .. } => {
                    let effective_cap = {
                        let global = self.payload_max_activity_input;
                        self.activity_input_cap_overrides
                            .get(name)
                            .copied()
                            .map_or(global, |ov| global.max(ov))
                    };
                    let observed = serde_json::to_string(input).map_or(0, |s| s.len() as u64);
                    if effective_cap > 0
                        && observed > effective_cap
                        && !self.offload_will_apply(observed)
                    {
                        return Err(HarvestError::PayloadTooLarge {
                            kind: PayloadKind::ActivityInput,
                            observed_bytes: observed,
                            cap_bytes: effective_cap,
                            workflow_type: self.workflow_name.clone(),
                            activity_name: Some(name.clone()),
                        });
                    }
                }
                RaceBranchKind::ChildWorkflow { input, .. } => {
                    let observed = serde_json::to_string(input).map_or(0, |s| s.len() as u64);
                    if self.payload_max_workflow_input > 0
                        && observed > self.payload_max_workflow_input
                    {
                        return Err(HarvestError::PayloadTooLarge {
                            kind: PayloadKind::ChildWorkflowInput,
                            observed_bytes: observed,
                            cap_bytes: self.payload_max_workflow_input,
                            workflow_type: self.workflow_name.clone(),
                            activity_name: None,
                        });
                    }
                }
                RaceBranchKind::Timer { .. } | RaceBranchKind::Signal { .. } => {
                    unreachable!("timer/signal branches only occur in the paired shape")
                }
            }
        }

        if needs_open_marker {
            self.push_command(WorkflowCommand::RecordMarker {
                name: open_marker,
                details: Value::from(count as u64),
            });
        }

        if !resolved.is_empty() {
            return self.settle_race(seq, &branches, resolved, &to_dispatch);
        }

        // Nothing resolved yet — dispatch every branch and await the first
        // live resolution. Branches are pushed in ascending index order, so
        // the race future's poll-order tie-break (lowest index first)
        // matches the documented tie-break used when multiple branches are
        // already resolved by the time a later replay cycle checks them.
        let mut receivers: Vec<(usize, oneshot::Receiver<Result<Value, String>>)> =
            Vec::with_capacity(to_dispatch.len());
        for dispatch in &to_dispatch {
            let branch = &branches[dispatch.index];
            let (tx, rx) = oneshot::channel();
            match &branch.kind {
                RaceBranchKind::Activity {
                    name,
                    input,
                    queue,
                    retry,
                    start_to_close,
                } => {
                    let activity_id = dispatch
                        .activity_id
                        .expect("activity dispatch always carries an activity_id");
                    if dispatch.is_new {
                        // Payload cap already validated in the pre-dispatch pass above.
                        self.push_command(WorkflowCommand::ScheduleActivity {
                            activity_id,
                            name: name.clone(),
                            input: input.clone(),
                            queue: queue.clone(),
                            retry_policy_override: retry.clone(),
                            start_to_close_override: *start_to_close,
                            session_id: None,
                            session_worker_id: None,
                            schedule_to_start_override: None,
                            result_tx: tx,
                        });
                    } else {
                        self.push_command(WorkflowCommand::WaitForActivity {
                            activity_id,
                            result_tx: tx,
                        });
                    }
                }
                RaceBranchKind::ChildWorkflow {
                    workflow_name,
                    input,
                } => {
                    let child_id = dispatch
                        .child_id
                        .expect("child dispatch always carries a child_id");
                    // Payload cap already validated in the pre-dispatch pass above
                    // when dispatch.is_new (ChildWorkflowStarted is written to the
                    // parent's history via append_single_event, so the offload
                    // bypass never applies to this kind).
                    self.push_command(WorkflowCommand::StartChildWorkflow {
                        child_id,
                        workflow_name: workflow_name.clone(),
                        input: input.clone(),
                        result_tx: tx,
                    });
                }
                RaceBranchKind::Timer { .. } | RaceBranchKind::Signal { .. } => {
                    unreachable!("timer/signal branches only occur in the paired shape")
                }
            }
            receivers.push((dispatch.index, rx));
        }

        let (winner_index, winner_raw) = RaceFirstFut { receivers }.await?;
        let winner_result = match winner_raw {
            Ok(value) => Ok(value),
            Err(error) => match &branches[winner_index].kind {
                RaceBranchKind::Activity { name, .. } => {
                    Err(HarvestError::activity_failed(name, 1, &error))
                }
                RaceBranchKind::ChildWorkflow { workflow_name, .. } => {
                    Err(HarvestError::activity_failed(
                        format!("child-workflow:{workflow_name}"),
                        1,
                        &error,
                    ))
                }
                RaceBranchKind::Timer { .. } | RaceBranchKind::Signal { .. } => {
                    unreachable!("timer/signal branches only occur in the paired shape")
                }
            },
        };

        self.settle_race(
            seq,
            &branches,
            vec![(winner_index, winner_result)],
            &to_dispatch,
        )
    }

    /// Decide (and, on the first cycle a winner is known, durably record) the
    /// winning branch of a race whose branch-level history states are already
    /// known (`resolved`), given the set of still-open branches (`to_dispatch`)
    /// that must be durably cancelled if they are not the winner.
    fn settle_race(
        &self,
        seq: u32,
        branches: &[RaceBranch],
        mut resolved: Vec<(usize, HarvestResult<Value>)>,
        to_dispatch: &[RaceDispatch],
    ) -> HarvestResult<RaceWinner> {
        let winner_marker_name = format!("race_winner:{seq}");
        let winner_index = if let Some(recorded) =
            self.match_history(|m| m.peek_u64_marker(&winner_marker_name))
        {
            let recorded_index = usize::try_from(recorded).unwrap_or(usize::MAX);
            if !resolved.iter().any(|(i, _)| *i == recorded_index) {
                return Err(self.nd_error(
                    format!(
                        "race #{seq}: previously recorded winner (branch {recorded_index}) has \
                         no resolved terminal on this replay"
                    ),
                    None,
                    Some(format!("winner={recorded_index}")),
                    Some("no matching resolved branch".to_string()),
                ));
            }
            recorded_index
        } else {
            // First cycle a winner is known: lowest-indexed resolved branch
            // wins (documented tie-break), the marker is recorded, and every
            // still-open sibling branch is durably cancelled.
            let winner_index = resolved
                .iter()
                .map(|(i, _)| *i)
                .min()
                .expect("resolved is non-empty (checked by callers)");
            self.push_command(WorkflowCommand::RecordMarker {
                name: winner_marker_name,
                details: Value::from(winner_index as u64),
            });

            let mut activities = Vec::new();
            let mut children = Vec::new();
            for dispatch in to_dispatch {
                if dispatch.index == winner_index {
                    continue;
                }
                if let Some(id) = dispatch.activity_id {
                    activities.push(id);
                }
                if let Some(id) = dispatch.child_id {
                    children.push(id);
                }
            }
            if !activities.is_empty() || !children.is_empty() {
                self.push_command(WorkflowCommand::CancelRaceLosers {
                    activities,
                    children,
                    timers: Vec::new(),
                });
            }
            winner_index
        };

        let pos = resolved
            .iter()
            .position(|(i, _)| *i == winner_index)
            .expect("winner_index is always present in resolved (checked above)");
        let (_, winner_result) = resolved.remove(pos);
        winner_result.map(|value| RaceWinner {
            index: winner_index,
            label: branches[winner_index].label.clone(),
            value,
        })
    }

    /// Thin wrapper around [`wait_for_signal_timeout`](Self::wait_for_signal_timeout)
    /// (issue #476) for the exactly-one-timer + exactly-one-signal race shape.
    /// A losing signal simply stays observable (nothing to durably cancel); a
    /// losing timer's durable row is removed by the worker exactly as it is
    /// today for `wait_for_signal_timeout`'s signal-won branch.
    async fn race_timer_signal_impl(&self, branches: Vec<RaceBranch>) -> HarvestResult<RaceWinner> {
        // Fixed, role-based indices (NOT each branch's position in the builder
        // chain): the winner for this shape is decided entirely by
        // wait_for_signal_timeout's own recorded-history-order determinism
        // contract, independent of whether `.timer()` or `.signal()` was
        // called first. Deriving RaceWinner.index from `.position()` in
        // `branches` instead would let a pure builder-call reorder (same
        // signal_name/duration_secs, different declaration order) silently
        // flip the index an in-flight execution observes on replay, with no
        // NonDeterministic error -- since no marker records the winner for
        // this shape (unlike the activity/child-workflow shapes above).
        const TIMER_INDEX: usize = 0;
        const SIGNAL_INDEX: usize = 1;

        let (signal_name, signal_label) = branches
            .iter()
            .find_map(|b| match &b.kind {
                RaceBranchKind::Signal { signal_name } => {
                    Some((signal_name.clone(), b.label.clone()))
                }
                _ => None,
            })
            .expect("validated by caller: exactly one signal branch");
        let (duration_secs, timer_label) = branches
            .iter()
            .find_map(|b| match &b.kind {
                RaceBranchKind::Timer { duration_secs } => Some((*duration_secs, b.label.clone())),
                _ => None,
            })
            .expect("validated by caller: exactly one timer branch");

        let timeout = std::time::Duration::from_secs(duration_secs);
        let (payload, timer_id) = self
            .wait_for_signal_timeout_with_timer_id(&signal_name, timeout)
            .await?;
        if payload.is_some() {
            // Signal won: the durable timer row armed above is now a stale
            // loser that would otherwise sit PENDING until its original
            // deadline -- blocking retention (`harvest_timers.fired = false`
            // rows keep an otherwise-terminal execution alive) and risking an
            // unrelated stray `TimerFired` on a later wake. `CancelRaceLosers`
            // already knows how to durably delete a losing timer row
            // (mirroring the activity/child-workflow shapes' loser cleanup);
            // reuse it here with no activities/children. Unlike those shapes
            // this never appends an event, so it is safe to push on every
            // resolution of this race, not just the first (there is no
            // marker to gate on for this shape).
            self.push_command(WorkflowCommand::CancelRaceLosers {
                activities: Vec::new(),
                children: Vec::new(),
                timers: vec![TimerId::new(&timer_id)],
            });
        }
        Ok(payload.map_or_else(
            || RaceWinner {
                index: TIMER_INDEX,
                label: timer_label,
                value: Value::Null,
            },
            |payload| RaceWinner {
                index: SIGNAL_INDEX,
                label: signal_label,
                value: payload,
            },
        ))
    }

    // ── Worker sessions (issue #606) ────────────────────────────────────────

    /// Generate the next worker-session sequence number for marker naming
    /// (mirrors `next_fan_out_seq`/`next_race_seq`).
    fn next_session_seq(&self) -> u32 {
        let mut seq = self.session_seq.lock().expect("session_seq lock poisoned");
        *seq += 1;
        *seq
    }

    /// Resolve a worker session's deterministic identity.
    ///
    /// On the **first live dispatch** (past end of history): generates a
    /// fresh [`SessionId`] and pushes a `RecordMarker { name: "session:{seq}"
    /// }` command so every future replay recovers the identical id. On
    /// **replay**: returns the id previously recorded at this cursor
    /// position.
    ///
    /// This is the entire determinism contract for worker sessions — **no
    /// new `WorkflowEvent` variant is introduced**. The session's *physical
    /// worker binding* is resolved separately (via the session-acquire
    /// activity's recorded output) and is non-replayed runtime routing
    /// state, exactly like activity placement today.
    fn resolve_session_id(&self, seq: u32) -> HarvestResult<SessionId> {
        let marker_result = self.match_history(|m| m.match_session_marker(seq));
        match marker_result {
            HistoryMatch::NoMatch => {
                self.check_strict_replay_no_match(&format!("MarkerRecorded(session:{seq})"))?;
                let session_id = SessionId::new();
                self.push_command(WorkflowCommand::RecordMarker {
                    name: format!("session:{seq}"),
                    details: Value::from(session_id.to_string()),
                });
                Ok(session_id)
            }
            HistoryMatch::Matched { output } => {
                let uuid_str = output.as_str().ok_or_else(|| {
                    self.nd_error(
                        format!("session #{seq}: recorded marker value is not a string"),
                        None,
                        None,
                        None,
                    )
                })?;
                uuid_str.parse::<SessionId>().map_err(|e| {
                    self.nd_error(
                        format!(
                            "session #{seq}: recorded marker value '{uuid_str}' is not a \
                             valid UUID: {e}"
                        ),
                        None,
                        None,
                        None,
                    )
                })
            }
            HistoryMatch::Diverged {
                expected,
                actual,
                event_index,
            } => Err(self.nd_error(
                format!(
                    "session #{seq}: history has {expected} but current code supplies {actual}"
                ),
                event_index,
                Some(expected),
                Some(actual),
            )),
            _ => unreachable!("match_session_marker only returns Matched, NoMatch, or Diverged"),
        }
    }

    /// Open a worker session (issue #606): route a group of activities to a
    /// single physical worker for the life of the session, so they can share
    /// machine-local state (a downloaded file, a warmed cache, GPU memory).
    ///
    /// Blocks (suspends the workflow) until a worker with a free session slot
    /// (`WorkerConfig::max_concurrent_sessions`) acquires the session, or
    /// fails with [`HarvestError::SessionAcquireTimeout`] if none does within
    /// `options.acquisition_timeout`.
    ///
    /// # Determinism
    ///
    /// The session's identity is a [`SessionId`] recorded once via the
    /// existing `MarkerRecorded` mechanism — **no new `WorkflowEvent`
    /// variant**. The session's *physical worker binding* is resolved from
    /// the internal `__harvest_session_acquire` activity's recorded output,
    /// exactly like any other activity result: replay recovers the same
    /// host worker id regardless of which (if any) worker is available at
    /// replay time.
    ///
    /// # Errors
    ///
    /// - [`HarvestError::Cancelled`] if the workflow has been cancelled.
    /// - [`HarvestError::SessionAcquireTimeout`] if no worker acquires the
    ///   session within `options.acquisition_timeout`.
    /// - [`HarvestError::SessionBroken`] if the session's host worker dies or
    ///   drains before the acquire activity's result is recorded (surfaced
    ///   identically to a broken session discovered later, mid-pipeline).
    pub async fn create_session(&self, options: SessionOptions) -> HarvestResult<Session<'_>> {
        self.check_cancellation()?;

        let seq = self.next_session_seq();
        let session_id = self.resolve_session_id(seq)?;
        let host_worker_id = self
            .dispatch_session_acquire(session_id, &options)
            .await
            .map_err(|err| match err {
                HarvestError::Timeout {
                    timeout_type: crate::error::TimeoutType::ScheduleToStart,
                    ..
                } => {
                    // Only the live discovery of the timeout should count --
                    // a replay of the same recorded terminal outcome must not
                    // re-increment the metric on every subsequent cycle.
                    if !self.is_replaying() {
                        self.metrics.record_session_acquisition(
                            &options.queue,
                            crate::telemetry::SessionAcquisitionOutcome::TimedOut,
                        );
                    }
                    HarvestError::SessionAcquireTimeout {
                        session_id,
                        queue: options.queue.clone(),
                        timeout_ms: duration_to_millis_saturating(options.acquisition_timeout),
                    }
                }
                HarvestError::ActivityFailed {
                    error_type, source, ..
                } if error_type == crate::failure::ERROR_TYPE_SESSION_BROKEN => {
                    // Defensive only -- the acquire task itself never carries
                    // `session_id`, so the broken-session scanner can never
                    // target it; this arm cannot actually be reached today.
                    // The `Broken` metric is emitted from the genuinely
                    // reachable path instead: `Session::broken_session_error`
                    // (member-activity/`complete()` failures).
                    HarvestError::SessionBroken {
                        session_id,
                        reason: source.to_string(),
                    }
                }
                other => other,
            })?;

        Ok(Session {
            ctx: self,
            id: session_id,
            host_worker_id,
            queue: options.queue,
        })
    }

    /// Dispatch the internal session-acquire activity and resolve it to a
    /// host worker id. Not exposed to workflow authors — [`Self::create_session`]
    /// is the public entry point.
    async fn dispatch_session_acquire(
        &self,
        session_id: SessionId,
        options: &SessionOptions,
    ) -> HarvestResult<String> {
        let raw = self
            .execute_activity_raw_full(
                SESSION_ACQUIRE_ACTIVITY_NAME,
                serde_json::json!(session_id.to_string()),
                &options.queue,
                None,
                None,
                // The acquire task is never hard-pinned to a worker -- it has
                // no resolved host yet; discovering one is its entire job.
                None,
                None,
                Some(options.acquisition_timeout),
            )
            .await?;
        raw.as_str().map(str::to_string).ok_or_else(|| {
            self.nd_error(
                format!(
                    "session {session_id} acquire activity returned a non-string worker id: \
                     {raw:?}"
                ),
                None,
                None,
                None,
            )
        })
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

            HistoryMatch::Failed {
                error,
                attempt,
                error_type,
                details,
            } => Err(HarvestError::ActivityFailed {
                name: name.to_string(),
                attempt,
                error_type,
                details,
                source: error.into(),
            }),

            HistoryMatch::TimedOut { timeout_type } => Err(HarvestError::Timeout {
                timeout_type,
                task_name: name.to_string(),
            }),

            HistoryMatch::Diverged {
                expected,
                actual,
                event_index,
            } => Err(self.nd_error(
                format!("external activity mismatch: expected {expected}, got {actual}"),
                event_index,
                Some(expected),
                Some(actual),
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
                    Ok(Err(error)) => Err(HarvestError::activity_failed(name, 1, &error)),
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
                    Ok(Err(error)) => Err(HarvestError::activity_failed(name, 1, &error)),
                    Err(_) => Err(HarvestError::Cancelled(format!(
                        "external activity '{name}' cancelled: result channel dropped"
                    ))),
                }
            }

            HistoryMatch::ActivityInProgress { .. }
            | HistoryMatch::ChildInProgress { .. }
            | HistoryMatch::LocalActivityInProgress { .. }
            | HistoryMatch::ExternalSignalInProgress { .. }
            | HistoryMatch::ExternalSignalFailed { .. }
            | HistoryMatch::ExternalCancelInProgress { .. }
            | HistoryMatch::ExternalCancelFailed { .. }
            | HistoryMatch::DetachedChildSpawned { .. } => {
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
            HistoryMatch::Diverged {
                expected,
                actual,
                event_index,
            } => Err(self.nd_error(
                format!("continue_as_new mismatch: expected {expected}, got {actual}"),
                event_index,
                Some(expected),
                Some(actual),
            )),
            HistoryMatch::Failed { .. }
            | HistoryMatch::TimedOut { .. }
            | HistoryMatch::ActivityInProgress { .. }
            | HistoryMatch::AwaitingExternalCompletion { .. }
            | HistoryMatch::ChildInProgress { .. }
            | HistoryMatch::LocalActivityInProgress { .. }
            | HistoryMatch::ExternalSignalInProgress { .. }
            | HistoryMatch::ExternalSignalFailed { .. }
            | HistoryMatch::ExternalCancelInProgress { .. }
            | HistoryMatch::ExternalCancelFailed { .. }
            | HistoryMatch::DetachedChildSpawned { .. } => {
                let actual = format!("{history_match:?}");
                Err(self.nd_error(
                    "continue_as_new history contains unexpected terminal state".to_string(),
                    self.match_history(|m| i32::try_from(m.position()).ok()),
                    Some("ContinueAsNew".to_string()),
                    Some(actual),
                ))
            }
            HistoryMatch::NoMatch => {
                self.check_strict_replay_no_match("ContinueAsNew")?;
                let observed = serde_json::to_string(&input).map_or(0, |s| s.len() as u64);
                if self.payload_max_workflow_input > 0
                    && observed > self.payload_max_workflow_input
                    && !self.offload_will_apply(observed)
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
                .map_err(|e| HarvestError::QueryHandlerPanicked(crate::error::panic_message(e)))?
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

    /// Register a declarative query handler from a [`QueryHandlerInfo`] companion record.
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

    /// Register a declarative update handler from an [`UpdateHandlerInfo`] companion record.
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
        // WorkflowContext on each invocation. Inherit exec_id, start_time,
        // cancellation_reason, workflow_id, and workflow_name from the parent
        // so handlers see consistent values and all logger correlation keys.
        let handler_fn = info.handler;
        let exec_id = self.exec_id;
        let start_time = self.start_time;
        let cancellation_reason = self.cancellation_reason.clone();
        let state = std::sync::Arc::clone(&self.state);
        let name = info.name;
        let workflow_id = self.workflow_id.clone();
        let workflow_name = self.workflow_name.clone();
        let context_headers = std::sync::Arc::clone(&self.context_headers);
        // Carryover is frozen in WorkflowStarted, so a handler on a scheduled workflow
        // must observe the same last_completion_result/last_error as the workflow body
        // (issue #488).
        let last_completion_result = self.last_completion_result.clone();
        let last_error = self.last_error.clone();
        let metrics = std::sync::Arc::clone(&self.metrics);

        let boxed_handler: crate::update::BoxUpdateHandler = std::sync::Arc::new(move |input| {
            let mut ctx = Self::new_for_handler(
                exec_id,
                start_time,
                cancellation_reason.clone(),
                std::sync::Arc::clone(&state),
            );
            // Propagate correlation fields for logger. Arc was just created;
            // no other reference exists yet so get_mut always succeeds.
            {
                let inner = std::sync::Arc::get_mut(&mut ctx).unwrap();
                inner.workflow_id.clone_from(&workflow_id);
                inner.workflow_name.clone_from(&workflow_name);
                inner.context_headers = std::sync::Arc::clone(&context_headers);
                inner
                    .last_completion_result
                    .clone_from(&last_completion_result);
                inner.last_error.clone_from(&last_error);
                inner.metrics = std::sync::Arc::clone(&metrics);
            }
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
            let mut ctx = Self::new_for_handler(
                self.exec_id,
                self.start_time,
                self.cancellation_reason.clone(),
                std::sync::Arc::clone(&self.state),
            );
            {
                let inner = std::sync::Arc::get_mut(&mut ctx).unwrap();
                inner.workflow_id.clone_from(&self.workflow_id);
                inner.workflow_name.clone_from(&self.workflow_name);
                inner.context_headers = std::sync::Arc::clone(&self.context_headers);
                // Carryover is frozen in WorkflowStarted; handlers see the same values
                // as the workflow body (issue #488).
                inner
                    .last_completion_result
                    .clone_from(&self.last_completion_result);
                inner.last_error.clone_from(&self.last_error);
                inner.metrics = std::sync::Arc::clone(&self.metrics);
            }
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

    // ── Signal handlers (issue #546) ────────────────────────────────────

    /// Register a push-based signal handler with an untyped JSON payload.
    ///
    /// Unlike [`wait_for_signal`](Self::wait_for_signal), which blocks a single
    /// specific point in the workflow body waiting for the next matching
    /// signal, a signal handler is dispatched automatically for **every**
    /// matching `SignalReceived` event already recorded in history at the
    /// point of registration -- including ones delivered before the handler
    /// existed (e.g. a signal that arrived on an earlier workflow-task cycle,
    /// before this cycle's code reached the registration call). No signal is
    /// silently dropped.
    ///
    /// Handlers are **fire-and-forget**: they run synchronously and return
    /// nothing. There is no validator and no completion event -- that is the
    /// `update` primitive's job (issue #140). A handler is expected to mutate
    /// author-captured state (e.g. an `Arc<Mutex<T>>` declared at the top of
    /// the workflow body).
    ///
    /// **Dispatch timing.** Registration itself only stores the handler --
    /// it does not fire inline. Dispatch happens the next time the workflow
    /// body makes any other history-consulting call (an activity, timer,
    /// child workflow, another signal wait, a deterministic primitive like
    /// `system_now`/`new_uuid`/`side_effect`, etc.), and at the latest once
    /// more when the workflow-task cycle finishes, so a handler is always
    /// guaranteed to have run by the time this cycle's outcome is decided.
    /// This is deliberate, not a limitation of "immediate" dispatch: an
    /// eager per-registration pump cannot know about a *different* handler
    /// about to register on the next line, so it can't order the two
    /// correctly against interleaved history for different signal names --
    /// see [`register_and_dispatch_signal_handler`](Self::register_and_dispatch_signal_handler)'s
    /// doc comment for the full rationale. A workflow that reads
    /// handler-mutated state should do so after at least one such call (a
    /// loop with an activity/timer between registration and the read is the
    /// idiomatic shape); reading it on the literal next line with nothing
    /// else in between is not supported.
    ///
    /// Registration is **idempotent** -- calling this with the same `name`
    /// multiple times (e.g. on every replay cycle at the top of the workflow
    /// function, as is idiomatic) is a no-op after the first call within a
    /// cycle for storage purposes.
    ///
    /// **Dispatch is per-cycle, not once-ever.** Unlike the `update`
    /// primitive (whose handler is guarded by a persisted `UpdateCompleted`/
    /// `UpdateFailed` event and is therefore skipped on replay),  a signal
    /// handler has no persisted "already delivered" marker. Every recorded
    /// `SignalReceived` event for `name` is drained and dispatched exactly
    /// once **per [`HistoryMatcher`](crate::replay::HistoryMatcher) instance**
    /// -- i.e. once per registration call within a single workflow-task
    /// cycle -- but a *new* cycle (the next activity/timer/signal
    /// completion, a worker restart, a warm-cache eviction) rebuilds the
    /// matcher from the full recorded history and redelivers the *same*
    /// historical signals again. That is safe and correct for reconstructing
    /// in-memory state, because the captured `Arc<Mutex<T>>` is itself
    /// rebuilt from scratch at the top of the same cycle -- replaying the
    /// same signals into it always reconstructs the same final value,
    /// exactly like the rest of the workflow body's plain Rust logic. It is
    /// **not** safe for a non-idempotent side effect (an external call, a
    /// log line meant to fire once): that side effect repeats on every
    /// subsequent replay of the same history. Keep handlers limited to
    /// mutating captured state; route side effects through a regular
    /// activity instead.
    ///
    /// A signal name that is never registered with a handler, or is only ever
    /// consumed by [`wait_for_signal`](Self::wait_for_signal), is completely
    /// unaffected by this method -- the existing pull-based buffered behavior
    /// is preserved exactly. The two consumption styles for the *same* name
    /// coexist without double-delivering a single `SignalReceived` event:
    /// whichever style claims an event first (in code-execution order) is the
    /// one that receives it. This also holds against a `receive_signal_timeout`
    /// / `wait_for_signal_timeout` race (issue #476) for the same name: a
    /// signal that is the resolution of a still-open race is reserved for
    /// that race and is never claimed by a push handler, so mixing both
    /// styles for one signal name cannot silently flip a race outcome.
    ///
    /// Must be called from the main workflow body, not from inside an
    /// `#[update]`/`register_update_handler` closure or a query handler: those
    /// run against a separate, throwaway [`WorkflowContext`] created fresh per
    /// invocation (see [`new_for_handler`](Self::new_for_handler)), so a
    /// registration made there is discarded the instant the handler returns
    /// and will never fire.
    ///
    /// For a typed payload, use
    /// [`register_signal_handler`](Self::register_signal_handler) instead.
    ///
    /// # Panics
    ///
    /// Panics if the internal signal registry mutex is poisoned. If the
    /// handler closure itself panics, [`invoke_signal_handler`] catches it at
    /// the dispatch boundary and logs it rather than propagating -- but note
    /// that if the handler panics *while holding a lock* on its own captured
    /// `Mutex`, that mutex is poisoned by ordinary Rust semantics regardless
    /// of this catch, and a later `.lock().unwrap()` on the very same mutex
    /// elsewhere in the same cycle will still panic. Prefer
    /// `.lock().unwrap_or_else(std::sync::PoisonError::into_inner)` (or keep
    /// critical sections trivially short and infallible) if a handler shares
    /// a mutex with other code in the workflow body.
    pub fn register_signal_handler_raw<H>(&self, name: &str, handler: H)
    where
        H: Fn(Value) + Send + Sync + 'static,
    {
        let boxed: BoxSignalHandler = Arc::new(handler);
        self.register_and_dispatch_signal_handler(name, boxed);
    }

    /// Register a **typed** push-based signal handler.
    ///
    /// The engine deserializes each matching `SignalReceived` payload as `Req`
    /// before calling `handler`. A payload that fails to deserialize is
    /// logged and dropped (the handler is never invoked for it) rather than
    /// panicking the workflow task -- signals are fire-and-forget by
    /// contract, so there is no caller waiting on a rejection.
    ///
    /// See [`register_signal_handler_raw`](Self::register_signal_handler_raw)
    /// for the full dispatch contract (buffering, idempotency, coexistence
    /// with pull-based `wait_for_signal`).
    ///
    /// ```rust
    /// use std::sync::{Arc, Mutex};
    /// use autumn_harvest::context::WorkflowContext;
    ///
    /// #[derive(serde::Deserialize)]
    /// struct CancelRequest { reason: String }
    ///
    /// # fn example(ctx: &WorkflowContext) {
    /// let cancelled_reason: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    /// let state = cancelled_reason.clone();
    /// ctx.register_signal_handler("cancel", move |req: CancelRequest| {
    ///     *state.lock().unwrap() = Some(req.reason);
    /// });
    /// # }
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the internal signal registry mutex is poisoned.
    pub fn register_signal_handler<Req, H>(&self, name: &str, handler: H)
    where
        Req: serde::de::DeserializeOwned + 'static,
        H: Fn(Req) + Send + Sync + 'static,
    {
        let name_for_log = name.to_string();
        self.register_signal_handler_raw(
            name,
            move |payload: Value| match serde_json::from_value::<Req>(payload) {
                Ok(req) => handler(req),
                Err(e) => {
                    tracing::warn!(
                        signal = %name_for_log,
                        error = %e,
                        "signal handler payload deserialization failed; delivery dropped"
                    );
                }
            },
        );
    }

    /// Returns the sorted names of all registered push-based signal handlers.
    ///
    /// Parity with [`list_query_names`](Self::list_query_names).
    ///
    /// # Panics
    ///
    /// Panics if the internal signal registry mutex is poisoned.
    #[must_use]
    pub fn list_signal_handler_names(&self) -> Vec<String> {
        self.signal_registry
            .lock()
            .expect("signal_registry lock poisoned")
            .list_names()
    }

    /// Registers `handler` under `name` (idempotent, first wins). Storage
    /// only -- see the module-level dispatch contract below for when the
    /// handler actually fires.
    ///
    /// Deliberately does **not** dispatch inline (PR #890 review, "preserve
    /// signal history order across handlers"): an earlier version pumped
    /// eagerly at each registration call so a signal already recorded before
    /// the cursor's current position would be delivered on the same line.
    /// That worked for a single handler, but broke ordering across
    /// *different* handler names registered back-to-back with no
    /// intervening command -- at the moment the first name registers, the
    /// second literally hasn't executed its own `register_*` call yet, so
    /// the first pump can never know to wait for it. Dispatch is instead
    /// deferred to [`pump_signal_handlers`](Self::pump_signal_handlers),
    /// triggered by [`match_history`](Self::match_history)'s post-hook: the
    /// first real cursor-advancing call made *after* every handler this
    /// cycle has registered (an activity/timer/signal wait, or -- if the
    /// workflow body does nothing else -- the executor's end-of-cycle flush)
    /// considers every currently-registered name together and sorts by
    /// event index, so cross-name ordering always follows history rather
    /// than registration order.
    fn register_and_dispatch_signal_handler(&self, name: &str, handler: BoxSignalHandler) {
        self.signal_registry
            .lock()
            .expect("signal_registry lock poisoned")
            .register(name, handler);
    }

    // ── Command drain ─────────────────────────────────────────────────

    /// Set a durable, human-readable status breadcrumb for this execution (issue #473).
    ///
    /// Calls are **last-write-wins**: the worker takes the most recently emitted
    /// value and overwrites `harvest_workflow_executions.current_details`.
    /// Passing an empty string **clears** the breadcrumb (persists `NULL`
    /// rather than an empty string) — issue #593.
    ///
    /// The value is **suppressed during replay** (zero new `harvest_events` rows,
    /// zero replay-determinism impact), mirroring the zero-footprint contract of
    /// Query handlers (#234) and `upsert_search_attrs`.
    ///
    /// The value is capped at [`DEFAULT_CURRENT_DETAILS_CAP_BYTES`] (configurable
    /// via `HarvestBuilder::with_current_details_cap`). Values longer than the cap
    /// are truncated to the cap boundary on a UTF-8 character boundary. The
    /// clear decision is made from the **pre-truncation** input, so a non-empty
    /// status that happens to truncate down to an empty string under an
    /// extreme cap (e.g. `0`, or smaller than its first UTF-8 character) is
    /// dropped as a no-op rather than misread as an explicit clear -- it never
    /// erases a previously stored breadcrumb (post-review hardening, #593).
    ///
    /// Operators can read the latest value from `GET /workflows/{id}` and
    /// `GET /workflows` without fan-out queries or a live worker.
    pub fn set_current_details(&self, details: impl Into<String>) {
        if self.is_replaying() {
            return;
        }
        let raw = details.into();
        let explicit_clear = raw.is_empty();
        let capped = if raw.len() > self.current_details_cap {
            // floor_char_boundary is not yet stable (tracking #93743); scan back
            // manually to the nearest valid UTF-8 character boundary.
            let mut boundary = self.current_details_cap;
            while !raw.is_char_boundary(boundary) {
                boundary -= 1;
            }
            raw[..boundary].to_string()
        } else {
            raw
        };
        self.push_command(WorkflowCommand::SetCurrentDetails {
            value: capped,
            explicit_clear,
        });
    }

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

/// Live-mode future for the signal-vs-deadline race (issue #476).
///
/// Polls the signal channel before the timer channel so that an in-cycle
/// resolution of both always picks the signal deterministically. A dropped
/// channel is treated as "will never resolve" rather than an error, so a
/// harness that resolves only one side (dropping the other sender) still
/// completes the race; only when **both** senders are gone does the future
/// resolve to [`HarvestError::Cancelled`].
struct SignalOrTimerRaceFut {
    signal_name: String,
    signal_rx: oneshot::Receiver<Value>,
    timer_rx: oneshot::Receiver<()>,
    signal_gone: bool,
    timer_gone: bool,
}

impl std::future::Future for SignalOrTimerRaceFut {
    type Output = HarvestResult<Option<Value>>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();

        if !this.signal_gone {
            match std::pin::Pin::new(&mut this.signal_rx).poll(cx) {
                std::task::Poll::Ready(Ok(payload)) => {
                    return std::task::Poll::Ready(Ok(Some(payload)));
                }
                std::task::Poll::Ready(Err(_)) => this.signal_gone = true,
                std::task::Poll::Pending => {}
            }
        }

        if !this.timer_gone {
            match std::pin::Pin::new(&mut this.timer_rx).poll(cx) {
                std::task::Poll::Ready(Ok(())) => return std::task::Poll::Ready(Ok(None)),
                std::task::Poll::Ready(Err(_)) => this.timer_gone = true,
                std::task::Poll::Pending => {}
            }
        }

        if this.signal_gone && this.timer_gone {
            return std::task::Poll::Ready(Err(HarvestError::Cancelled(format!(
                "signal-or-timeout race '{}' cancelled: result channels dropped",
                this.signal_name
            ))));
        }

        std::task::Poll::Pending
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
    /// Error string from the most recent failed attempt, if this is a retry.
    ///
    /// `None` on the first attempt. Derived from the task row's last recorded
    /// failure at dispatch time; populated identically for regular activities,
    /// local activities, and Saga steps.
    previous_failure: Option<String>,
    /// Maximum number of attempts configured for this activity (retry policy
    /// `max_attempts`, or `1` if no policy). Use `attempt() == max_attempts()`
    /// to detect the final attempt and branch on last-attempt behaviour.
    max_attempts: Option<u32>,
    /// State needed by [`Self::run_transactional`].
    ///
    /// `None` for test contexts, local activity contexts, and any context that
    /// was not constructed by the worker's regular activity dispatch path.
    #[cfg(feature = "db")]
    transactional_state: Option<TransactionalState>,
    /// Ambient context headers propagated from the parent workflow (issue #481).
    /// Read via `header()` / `headers()`. Empty for activities dispatched before
    /// this feature was deployed.
    context_headers: std::sync::Arc<HashMap<String, String>>,
    /// Metrics recorder for user-emitted custom business metrics (issue #532).
    /// Activity metrics are never suppressed — each invocation (including retries)
    /// emits independently.
    metrics: std::sync::Arc<dyn crate::telemetry::MetricsRecorder>,
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
            previous_failure: None,
            max_attempts: None,
            #[cfg(feature = "db")]
            transactional_state: None,
            context_headers: std::sync::Arc::new(HashMap::new()),
            metrics: std::sync::Arc::new(crate::telemetry::NoOpMetrics),
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
            previous_failure: None,
            max_attempts: None,
            transactional_state: None,
            context_headers: std::sync::Arc::new(HashMap::new()),
            metrics: std::sync::Arc::new(crate::telemetry::NoOpMetrics),
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
            previous_failure: None,
            max_attempts: None,
            #[cfg(feature = "db")]
            transactional_state: None,
            context_headers: std::sync::Arc::new(HashMap::new()),
            metrics: std::sync::Arc::new(crate::telemetry::NoOpMetrics),
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

    // ── Context headers (issue #481) ──────────────────────────────────────────

    /// Attach the parent workflow's context headers to this activity context.
    ///
    /// Called automatically by the worker at dispatch time; you only need it
    /// when constructing an `ActivityContext` manually in tests.
    #[must_use]
    pub fn with_context_headers(
        mut self,
        headers: std::sync::Arc<HashMap<String, String>>,
    ) -> Self {
        self.context_headers = headers;
        self
    }

    // ── Custom metrics (issue #532) ───────────────────────────────────────────

    /// Attach a metrics recorder to this activity context (builder-style, called by the worker).
    #[must_use]
    pub fn with_metrics(
        mut self,
        metrics: std::sync::Arc<dyn crate::telemetry::MetricsRecorder>,
    ) -> Self {
        self.metrics = metrics;
        self
    }

    /// Obtain a custom-metrics handle for this activity invocation.
    ///
    /// Unlike workflow metrics, activity metrics are **never suppressed** —
    /// each actual invocation emits.  Retries count as separate executions,
    /// so a counter inside an activity body increments once per attempt.
    ///
    /// ```rust,ignore
    /// #[activity]
    /// async fn charge_card(ctx: &ActivityContext, amount: u64) -> Result<(), String> {
    ///     ctx.metrics().counter("payments_attempted", 1, &[("currency", "usd")]);
    ///     charge(amount)?;
    ///     ctx.metrics().counter("payments_succeeded", 1, &[("currency", "usd")]);
    ///     Ok(())
    /// }
    /// ```
    #[must_use]
    pub fn metrics(&self) -> crate::telemetry::UserMetrics<'_> {
        crate::telemetry::UserMetrics::new(&*self.metrics, false)
    }

    /// Return the value of the named context header, or `None` if not set.
    ///
    /// Returns `None` (never panics) when `key` was never attached.
    #[must_use]
    pub fn header(&self, key: &str) -> Option<&str> {
        self.context_headers.get(key).map(String::as_str)
    }

    /// Return the full context header map propagated from the parent workflow.
    ///
    /// The map is empty for activities dispatched before this feature shipped.
    #[must_use]
    pub fn headers(&self) -> &HashMap<String, String> {
        &self.context_headers
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

    /// Set the error message from the most recent failed attempt.
    ///
    /// `None` means this is the first attempt (no prior failure). The engine
    /// sets this automatically from the task queue's stored error; you only
    /// need it when constructing a context manually in tests.
    #[must_use]
    pub fn with_previous_failure(mut self, failure: Option<String>) -> Self {
        self.previous_failure = failure;
        self
    }

    /// Set the maximum number of attempts for this activity invocation.
    ///
    /// Mirrors `RetryPolicy::max_attempts`. The engine sets this automatically;
    /// you only need it when constructing a context manually in tests.
    #[must_use]
    pub const fn with_max_attempts(mut self, max: u32) -> Self {
        self.max_attempts = Some(max);
        self
    }

    /// Attach the transactional state needed by [`Self::run_transactional`].
    ///
    /// Called by the worker after the `ActivityStarted` event is committed so
    /// the `activity_id` is stable.  Not available on test or local-activity
    /// contexts.
    #[cfg(feature = "db")]
    #[must_use]
    pub(crate) fn with_transactional_state(mut self, state: TransactionalState) -> Self {
        self.transactional_state = Some(state);
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

    /// 1-indexed attempt number for this activity invocation.
    ///
    /// Returns `1` on the first dispatch and increments with each worker-level
    /// retry.  Use `attempt() == max_attempts()` to detect the final attempt:
    ///
    /// ```rust
    /// use autumn_harvest::context::ActivityContext;
    ///
    /// # fn example(ctx: &ActivityContext) {
    /// if ctx.attempt() == ctx.max_attempts() {
    ///     // Last attempt — write a fallback row, page an on-call, emit a metric.
    /// }
    /// if let Some(prev) = ctx.previous_failure() {
    ///     tracing::warn!(attempt = ctx.attempt(), error = prev, "retrying after previous failure");
    /// }
    /// # }
    /// ```
    ///
    /// The default idempotency key is **retry-stable** (same value for all
    /// attempts).  Call `ctx.idempotency_key()?.subkey(&format!("attempt-{}",
    /// ctx.attempt()))` to opt into an attempt-scoped subkey if your downstream
    /// API requires distinct keys per attempt.
    #[must_use]
    pub const fn attempt(&self) -> u32 {
        match self.attempt {
            Some(n) => n,
            None => 1,
        }
    }

    /// Error string from the most recent failed attempt.
    ///
    /// Returns `None` on the first attempt.  On a retry, contains the
    /// human-readable error message recorded when the previous attempt failed.
    /// Use with [`attempt`](Self::attempt) and [`max_attempts`](Self::max_attempts)
    /// to implement retry-aware logging or last-attempt escape hatches.
    #[must_use]
    pub fn previous_failure(&self) -> Option<&str> {
        self.previous_failure.as_deref()
    }

    /// Maximum number of attempts configured for this activity.
    ///
    /// Reflects the `RetryPolicy::max_attempts` value for the activity (or `1`
    /// if no retry policy was configured).  Combine with [`attempt`](Self::attempt)
    /// to detect the final attempt: `ctx.attempt() == ctx.max_attempts()`.
    #[must_use]
    pub const fn max_attempts(&self) -> u32 {
        match self.max_attempts {
            Some(n) => n,
            None => 1,
        }
    }

    /// Returns `true` when this is the last allowed attempt.
    ///
    /// Equivalent to `self.attempt() == self.max_attempts()`.
    #[must_use]
    pub const fn is_last_attempt(&self) -> bool {
        self.attempt() == self.max_attempts()
    }

    /// Returns `true` when this is a retry (attempt > 1).
    ///
    /// Equivalent to `self.attempt() > 1`.
    #[must_use]
    pub const fn is_retrying(&self) -> bool {
        self.attempt() > 1
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
    /// [`Self::heartbeat_details`]. Always check the return value — an
    /// `Err(ActivityCancelled)` means the owning workflow was cancelled and the
    /// activity should wind down promptly.
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
    ///     // Heartbeat with current progress; exit early on cancellation.
    ///     ctx.heartbeat(serde_json::json!({"progress": chunk})).await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// - [`HarvestError::ActivityCancelled`] if the cancellation token has been
    ///   triggered, the owning workflow execution has reached a cancelling or
    ///   terminal state, or the heartbeat channel is closed.
    /// - [`HarvestError::Serialization`] if `details` fails to serialize.
    /// - [`HarvestError::Config`] for local activities, which do not support
    ///   heartbeating.
    pub async fn heartbeat(&self, details: impl serde::Serialize) -> crate::HarvestResult<()> {
        // Check cancellation first -- fast path.
        if self.cancel.is_cancelled() {
            return Err(HarvestError::ActivityCancelled(
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
            HarvestError::ActivityCancelled("activity cancelled: heartbeat channel closed".into())
        })?;

        Ok(())
    }

    /// Check whether the owning workflow has been cancelled.
    ///
    /// This is a lightweight convenience that works for both regular and local
    /// activities.  It checks the worker's cancellation token first (fast
    /// path), then performs a throttled durable check against the task queue
    /// row (db feature only) to catch cancellations that arrived while the
    /// activity was not heartbeating.
    ///
    /// Regular activities that perform long-running loops should call this
    /// periodically to exit cleanly when the owning workflow is cancelled.
    /// The call is cheap when the durable-check interval has not elapsed.
    ///
    /// # Local activities
    ///
    /// This method is a **no-op** for local activities.  Local activities are
    /// created with a fresh, disconnected cancellation token and no durable
    /// queue-row check, so both paths always return `Ok(())` regardless of
    /// workflow state.  For local activities, cancellation is surfaced on the
    /// enclosing [`WorkflowContext`]: call [`WorkflowContext::check_cancellation`]
    /// after each local-activity step to detect it.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::ActivityCancelled`] when cancellation has been
    /// requested (regular activities only).  Returns [`Ok(())`] otherwise.
    #[cfg_attr(not(feature = "db"), allow(clippy::unused_async))]
    pub async fn check_cancellation(&self) -> crate::HarvestResult<()> {
        if self.cancel.is_cancelled() {
            return Err(HarvestError::ActivityCancelled(
                "activity cancelled via cancellation token".into(),
            ));
        }
        #[cfg(feature = "db")]
        self.check_durable_cancellation().await?;
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
            Some((_, Some(error))) if error.contains("workflow cancelled") => {
                Err(HarvestError::ActivityCancelled(error))
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

    /// Run user domain writes and commit the `ActivityCompleted` event in a
    /// single atomic Postgres transaction.
    ///
    /// This is the primary API for transactional activities (issue #352).  When
    /// your activity body and harvest share the same Postgres cluster, wrapping
    /// domain writes in `run_transactional` eliminates the dual-write problem:
    /// if the worker crashes between "did the work" and "recorded the work" no
    /// duplicate effects are produced on retry, even without idempotency keys.
    ///
    /// # How it works
    ///
    /// `f` receives a `&mut AsyncPgConnection` that is inside the same
    /// transaction harvest will use to append `ActivityCompleted` and mark the
    /// task complete.  If `f` returns `Ok(value)`:
    ///
    /// * harvest appends `ActivityCompleted { output: value }` within the same
    ///   transaction
    /// * the task row is set to `COMPLETED`
    /// * the workflow is woken
    /// * the transaction commits — user writes and the event are visible together
    ///
    /// If `f` returns `Err(e)` the transaction is rolled back: user writes are
    /// discarded and the activity's error propagates through the normal retry
    /// and `ActivityFailed` path.
    ///
    /// # Constraints
    ///
    /// * **Not supported for local activities.** Local activities run inline on
    ///   the workflow worker and do not have a dedicated DB connection.  Calling
    ///   `run_transactional` on a local activity context returns `Err`.
    /// * **DB must be the same cluster.** The connection comes from harvest's
    ///   own worker pool.  Cross-cluster atomicity is not possible — use the
    ///   traditional idempotency-key pattern for that case.
    /// * **Must be the final expression.** Once `run_transactional` returns
    ///   `Ok`, harvest has already committed `ActivityCompleted` and marked
    ///   the task `COMPLETED`.  Any fallible work done by the activity *after*
    ///   this call cannot roll back the committed event — if it fails, the
    ///   workflow still observes success.  Always return the result of
    ///   `run_transactional` directly; do not use `?` on it and then do more
    ///   work.
    /// * **Keep bodies short.** The Postgres row lock held during `f` blocks
    ///   concurrent event appends for the same workflow execution.  Long-running
    ///   bodies (> a few seconds) will delay other activities and should use
    ///   regular activities with idempotency keys instead.
    /// * **Heartbeating is unaffected** — `ctx.heartbeat()` still works
    ///   normally outside the `run_transactional` closure.
    ///
    /// # Errors
    ///
    /// Returns `Err(String)` when:
    /// * this context has no transactional pool attached (e.g. test contexts
    ///   or local activities),
    /// * a DB connection cannot be acquired from the pool,
    /// * `f` returns `Err(e)` (the user error is propagated as-is), or
    /// * the harvest finalization (event append / task complete / workflow wake)
    ///   fails.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use autumn_harvest::prelude::*;
    ///
    /// #[activity(start_to_close = "30s")]
    /// async fn create_order(ctx: &ActivityContext, order: Order) -> Result<OrderId, String> {
    ///     ctx.run_transactional(|conn| Box::pin(async move {
    ///         diesel::insert_into(orders::table)
    ///             .values(&NewOrder::from(&order))
    ///             .execute(conn)
    ///             .await
    ///             .map_err(|e| e.to_string())?;
    ///         Ok(order.id)
    ///     })).await
    /// }
    /// ```
    #[cfg(feature = "db")]
    pub async fn run_transactional<T, F>(&self, f: F) -> Result<T, String>
    where
        F: for<'conn> FnOnce(
                &'conn mut diesel_async::AsyncPgConnection,
            ) -> futures::future::BoxFuture<'conn, Result<T, String>>
            + Send,
        T: serde::Serialize + Send,
    {
        // Two-level error type so the user's original Err(String) propagates
        // unchanged.  Wrapping it in HarvestError::Config would stringify as
        // "invalid configuration: …" which breaks non_retryable_errors matching
        // and corrupts the ActivityFailed payload seen by the retry policy.
        // Defined before any statements to satisfy clippy::items_after_statements.
        enum TxError {
            /// User closure returned Err — propagated verbatim.
            User(String),
            /// Internal harvest error (lock, append, DB).
            Harvest(HarvestError),
            /// Structured `ActivityFailure` JSON payload (e.g. `PayloadTooLarge`)
            /// that must reach `handle_activity_result` as-is so the failure
            /// parser can mark it non-retryable.
            Payload(String),
        }
        impl From<HarvestError> for TxError {
            fn from(e: HarvestError) -> Self {
                Self::Harvest(e)
            }
        }
        impl From<diesel::result::Error> for TxError {
            fn from(e: diesel::result::Error) -> Self {
                Self::Harvest(crate::error::database_error(e))
            }
        }

        use diesel_async::AsyncConnection as _;
        use scoped_futures::ScopedFutureExt as _;

        let Some(txn) = &self.transactional_state else {
            return Err(
                "ctx.run_transactional() is not supported for this activity context: \
                 transactional activities require a regular (non-local) activity whose \
                 worker pool shares the harvest Postgres cluster; \
                 test contexts and local activities do not have a transactional pool attached"
                    .to_string(),
            );
        };

        let exec_id = txn.exec_id;
        let activity_id = txn.activity_id;
        let task_id = txn.task_id;
        let max_result_bytes = txn.max_result_bytes;

        let mut conn =
            txn.pool.get().await.map_err(|e| {
                format!("transactional activity failed to acquire DB connection: {e}")
            })?;

        conn.transaction::<T, TxError, _>(|conn| {
            async move {
                // Run user domain writes.
                let user_result = f(conn).await.map_err(TxError::User)?;

                // Serialize the result for the event log.
                let output =
                    serde_json::to_value(&user_result).map_err(HarvestError::Serialization)?;

                // Enforce the result-size cap before committing.  The worker's
                // post-handler cap check runs after the handler returns, which
                // is too late for transactional activities — the event would
                // already be committed.  Rolling back here ensures an oversized
                // result never lands in harvest_events.
                if max_result_bytes > 0 {
                    let observed = serde_json::to_string(&output).map_or(0, |s| s.len() as u64);
                    if observed > max_result_bytes {
                        use crate::failure::IntoActivityErrorString as _;
                        let payload = crate::failure::ActivityFailure::non_retryable(
                            "PayloadTooLarge",
                            format!(
                                "transactional activity result exceeds cap: \
                                 {observed} bytes (cap {max_result_bytes} bytes)"
                            ),
                        )
                        .into_error_payload();
                        return Err(TxError::Payload(payload));
                    }
                }

                // Lock the execution row first (consistent with the rest of the
                // codebase: harvest_workflow_executions → harvest_task_queue)
                // and load history so we can compute the next sequential
                // event_id before appending.
                let history = crate::store::lock_and_load_history(conn, exec_id).await?;

                // Idempotency guard: verify the task is still RUNNING before
                // we commit.  If it's already COMPLETED (e.g. this is a
                // crash-recovery attempt where the first transaction succeeded)
                // we roll back the user writes so the caller sees a clean
                // slate, matching the "exactly-once" contract.
                match crate::queue::task_state_for_update(conn, task_id).await? {
                    Some(ref s) if s == "RUNNING" => {}
                    Some(other) => {
                        return Err(TxError::Harvest(HarvestError::Config(format!(
                            "transactional activity task {task_id} is in state '{other}', \
                             not RUNNING; rolling back user writes (the ActivityCompleted \
                             event was already committed by a prior attempt)"
                        ))));
                    }
                    None => {
                        return Err(TxError::Harvest(HarvestError::Config(format!(
                            "transactional activity task {task_id} no longer exists; \
                             rolling back user writes"
                        ))));
                    }
                }

                // Append ActivityCompleted within the same transaction.
                let completion_event = crate::event::WorkflowEvent::ActivityCompleted {
                    activity_id,
                    output: output.clone(),
                };
                crate::store::append_events(
                    conn,
                    exec_id,
                    &[completion_event],
                    history.next_event_id,
                )
                .await?;

                // Mark the task COMPLETED.
                crate::queue::complete_task(conn, task_id, output).await?;

                // Wake the workflow so it can pick up the ActivityCompleted
                // result on its next execution cycle.
                crate::queue::wake_workflow_task(conn, exec_id).await?;

                Ok(user_result)
            }
            .scope_boxed()
        })
        .await
        .map_err(|e| match e {
            TxError::User(s) => s,
            TxError::Harvest(he) => he.to_string(),
            TxError::Payload(p) => p,
        })
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
        .with_max_attempts(1)
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

    // ── Signal handlers (issue #546) ────────────────────────────────────────

    fn started_event() -> WorkflowEvent {
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }
    }

    #[test]
    fn register_signal_handler_raw_is_idempotent() {
        let ctx = WorkflowContext::new_test();
        // Registering twice must not panic or error.
        ctx.register_signal_handler_raw("cancel", |_payload: Value| {});
        ctx.register_signal_handler_raw("cancel", |_payload: Value| {});
        assert!(
            ctx.list_signal_handler_names()
                .contains(&"cancel".to_string())
        );
    }

    #[test]
    fn register_signal_handler_raw_emits_no_commands() {
        let ctx = WorkflowContext::new_test();
        ctx.register_signal_handler_raw("cancel", |_payload: Value| {});
        let cmds = ctx.drain_commands();
        assert!(cmds.is_empty(), "registration must not emit any commands");
    }

    #[test]
    fn register_signal_handler_raw_dispatches_already_buffered_signal_once_flushed() {
        // Dispatch is deferred: registration only stores the handler.
        // `flush_pending_signal_handlers` mirrors the trigger every other
        // history-consulting call (activity/timer/deterministic primitive)
        // provides naturally, and the executor's own end-of-cycle flush.
        let events = vec![
            started_event(),
            WorkflowEvent::SignalReceived {
                signal_name: "cancel".into(),
                payload: serde_json::json!({"reason": "user_requested"}),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        let received: Arc<std::sync::Mutex<Vec<Value>>> = Arc::new(std::sync::Mutex::new(vec![]));
        let received_clone = received.clone();
        ctx.register_signal_handler_raw("cancel", move |payload: Value| {
            received_clone.lock().unwrap().push(payload);
        });
        ctx.flush_pending_signal_handlers();

        assert_eq!(
            *received.lock().unwrap(),
            vec![serde_json::json!({"reason": "user_requested"})],
            "signal delivered before the handler existed must still be dispatched \
             once the handler is registered and the cycle is flushed"
        );
    }

    #[test]
    fn register_signal_handler_typed_deserializes_payload() {
        #[derive(serde::Deserialize)]
        struct CancelRequest {
            reason: String,
        }

        let events = vec![
            started_event(),
            WorkflowEvent::SignalReceived {
                signal_name: "cancel".into(),
                payload: serde_json::json!({"reason": "budget_exceeded"}),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        let received: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(vec![]));
        let received_clone = received.clone();
        ctx.register_signal_handler("cancel", move |req: CancelRequest| {
            received_clone.lock().unwrap().push(req.reason);
        });
        ctx.flush_pending_signal_handlers();

        assert_eq!(
            *received.lock().unwrap(),
            vec!["budget_exceeded".to_string()]
        );
    }

    #[test]
    fn register_signal_handler_multiple_events_dispatched_in_order() {
        let events = vec![
            started_event(),
            WorkflowEvent::SignalReceived {
                signal_name: "cancel".into(),
                payload: serde_json::json!(1),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "cancel".into(),
                payload: serde_json::json!(2),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        let received: Arc<std::sync::Mutex<Vec<i64>>> = Arc::new(std::sync::Mutex::new(vec![]));
        let received_clone = received.clone();
        ctx.register_signal_handler_raw("cancel", move |payload: Value| {
            received_clone
                .lock()
                .unwrap()
                .push(payload.as_i64().unwrap());
        });
        ctx.flush_pending_signal_handlers();

        assert_eq!(*received.lock().unwrap(), vec![1, 2]);
    }

    #[test]
    fn register_signal_handler_does_not_dispatch_when_no_signal_recorded() {
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), vec![started_event()]);
        let received: Arc<std::sync::Mutex<Vec<Value>>> = Arc::new(std::sync::Mutex::new(vec![]));
        let received_clone = received.clone();
        ctx.register_signal_handler_raw("cancel", move |payload: Value| {
            received_clone.lock().unwrap().push(payload);
        });
        assert!(received.lock().unwrap().is_empty());
    }

    #[test]
    fn register_signal_handler_does_not_fire_for_other_names() {
        let events = vec![
            started_event(),
            WorkflowEvent::SignalReceived {
                signal_name: "approved".into(),
                payload: serde_json::json!({"ok": true}),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let received: Arc<std::sync::Mutex<Vec<Value>>> = Arc::new(std::sync::Mutex::new(vec![]));
        let received_clone = received.clone();
        ctx.register_signal_handler_raw("cancel", move |payload: Value| {
            received_clone.lock().unwrap().push(payload);
        });
        assert!(received.lock().unwrap().is_empty());
    }

    #[test]
    fn signal_handler_and_wait_for_signal_do_not_double_deliver() {
        // Two "cancel" signals recorded. The push handler (registered first,
        // as is idiomatic at the top of a workflow body) claims both; a later
        // pull-based wait_for_signal for the same name must not also see them.
        let events = vec![
            started_event(),
            WorkflowEvent::SignalReceived {
                signal_name: "cancel".into(),
                payload: serde_json::json!(1),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        let received: Arc<std::sync::Mutex<Vec<Value>>> = Arc::new(std::sync::Mutex::new(vec![]));
        let received_clone = received.clone();
        ctx.register_signal_handler_raw("cancel", move |payload: Value| {
            received_clone.lock().unwrap().push(payload);
        });
        ctx.flush_pending_signal_handlers();
        assert_eq!(*received.lock().unwrap(), vec![serde_json::json!(1)]);

        // A pull-based wait for the same name, later in the same task, must
        // not see the already-dispatched event -- it should behave as NoMatch
        // (i.e. suspend for a future occurrence), not replay/return it again.
        let history_match = ctx.match_history(|m| m.match_signal("cancel"));
        assert_eq!(history_match, HistoryMatch::NoMatch);
    }

    #[tokio::test]
    async fn register_signal_handler_does_not_fire_before_an_unconsumed_activity_is_matched() {
        // Regression (PR #890 review, "preserve history order when
        // dispatching handlers", context.rs:5090): with WorkflowStarted,
        // ActivityScheduled, ActivityCompleted, SignalReceived("cancel"), a
        // handler registered at the top of the workflow body (the idiomatic
        // placement) must NOT fire before the workflow has actually replayed
        // the recorded activity. The original implementation eagerly drained
        // every recorded "cancel" event at registration time regardless of
        // the activity sitting in between, so a workflow gating the activity
        // call on the handler-mutated state could skip the recorded
        // `execute_activity` call entirely, diverging from history.
        let activity_id = ActivityExecId::new();
        let events = vec![
            started_event(),
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "send_email".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!("sent"),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "cancel".into(),
                payload: serde_json::json!({"reason": "manual"}),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        let received: Arc<std::sync::Mutex<Vec<Value>>> = Arc::new(std::sync::Mutex::new(vec![]));
        let received_clone = received.clone();
        ctx.register_signal_handler_raw("cancel", move |payload: Value| {
            received_clone.lock().unwrap().push(payload);
        });
        assert!(
            received.lock().unwrap().is_empty(),
            "handler must not fire before the workflow has matched the preceding activity"
        );

        // The workflow body now actually replays the activity.
        let result = ctx
            .execute_activity_raw("send_email", Value::Null, "default")
            .await;
        assert_eq!(result.unwrap(), serde_json::json!("sent"));

        // Only now -- after the cursor has passed the activity -- is the
        // trailing signal visible to the handler.
        assert_eq!(
            *received.lock().unwrap(),
            vec![serde_json::json!({"reason": "manual"})]
        );
    }

    #[tokio::test]
    async fn signal_handlers_dispatch_in_history_order_across_names_not_registration_order() {
        // Regression (PR #890 review, "preserve signal history order across
        // handlers", replay.rs:2745): history records "pause" before
        // "cancel", but the workflow body registers the "cancel" handler
        // first. A real command (here, an activity) separates registration
        // from both signals; the pump triggered by matching it considers
        // every handler registered so far together and sorts by event
        // index -- dispatch follows SignalReceived history order (pause,
        // then cancel), not the order the two `register_*` calls happened to
        // run in. See `signal_handlers_dispatch_in_history_order_with_zero_intervening_commands`
        // for the same guarantee with nothing at all between the two
        // registrations.
        let activity_id = ActivityExecId::new();
        let events = vec![
            started_event(),
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "send_email".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!("sent"),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "pause".into(),
                payload: serde_json::json!("pause-payload"),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "cancel".into(),
                payload: serde_json::json!("cancel-payload"),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        let order: Arc<std::sync::Mutex<Vec<&'static str>>> =
            Arc::new(std::sync::Mutex::new(vec![]));

        let order_for_cancel = order.clone();
        ctx.register_signal_handler_raw("cancel", move |_payload: Value| {
            order_for_cancel.lock().unwrap().push("cancel");
        });
        let order_for_pause = order.clone();
        ctx.register_signal_handler_raw("pause", move |_payload: Value| {
            order_for_pause.lock().unwrap().push("pause");
        });

        // Both registrations' own eager pumps find nothing yet -- the
        // ActivityScheduled event blocks the cursor-bound sweep.
        assert!(
            order.lock().unwrap().is_empty(),
            "neither handler may fire before the preceding activity is matched"
        );

        // The workflow body now actually replays the activity, advancing the
        // cursor past both trailing signals in one go.
        let result = ctx
            .execute_activity_raw("send_email", Value::Null, "default")
            .await;
        assert_eq!(result.unwrap(), serde_json::json!("sent"));

        assert_eq!(
            *order.lock().unwrap(),
            vec!["pause", "cancel"],
            "dispatch must follow SignalReceived history order, not registration order"
        );
    }

    #[test]
    fn signal_handlers_dispatch_in_history_order_with_zero_intervening_commands() {
        // The case an earlier eager-per-registration design could not fix
        // (PR #890 review follow-up, context.rs:5169): history records
        // "pause" before "cancel", the workflow body registers "cancel"
        // then "pause" back-to-back with *nothing* in between, and BOTH
        // signals are already recorded before either `register_*` call
        // runs. Because dispatch is deferred (neither registration pumps
        // inline), a single flush considers both handlers together and
        // sorts by event index -- "pause" still fires first, matching
        // history order despite the reversed registration order and the
        // total absence of any intervening activity/timer/etc.
        let events = vec![
            started_event(),
            WorkflowEvent::SignalReceived {
                signal_name: "pause".into(),
                payload: serde_json::json!("pause-payload"),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "cancel".into(),
                payload: serde_json::json!("cancel-payload"),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        let order: Arc<std::sync::Mutex<Vec<&'static str>>> =
            Arc::new(std::sync::Mutex::new(vec![]));

        let order_for_cancel = order.clone();
        ctx.register_signal_handler_raw("cancel", move |_payload: Value| {
            order_for_cancel.lock().unwrap().push("cancel");
        });
        let order_for_pause = order.clone();
        ctx.register_signal_handler_raw("pause", move |_payload: Value| {
            order_for_pause.lock().unwrap().push("pause");
        });

        // Neither registration dispatches inline -- nothing has fired yet.
        assert!(
            order.lock().unwrap().is_empty(),
            "dispatch must be deferred, not fired inline per registration call"
        );

        // Mirrors the executor's end-of-cycle flush.
        ctx.flush_pending_signal_handlers();

        assert_eq!(
            *order.lock().unwrap(),
            vec!["pause", "cancel"],
            "dispatch must follow SignalReceived history order even with zero \
             intervening commands between the two registrations"
        );
    }

    #[test]
    fn list_signal_handler_names_returns_sorted_names() {
        let ctx = WorkflowContext::new_test();
        ctx.register_signal_handler_raw("zeta", |_: Value| {});
        ctx.register_signal_handler_raw("alpha", |_: Value| {});
        ctx.register_signal_handler_raw("mid", |_: Value| {});
        assert_eq!(
            ctx.list_signal_handler_names(),
            vec!["alpha".to_string(), "mid".to_string(), "zeta".to_string()]
        );
    }

    #[test]
    fn register_signal_handler_typed_deserialization_failure_does_not_panic() {
        #[derive(serde::Deserialize)]
        #[allow(dead_code)]
        struct CancelRequest {
            reason: String,
        }

        let events = vec![
            started_event(),
            WorkflowEvent::SignalReceived {
                signal_name: "cancel".into(),
                // Missing the required "reason" field.
                payload: serde_json::json!({}),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        // Must not panic even though the payload cannot deserialize into CancelRequest.
        ctx.register_signal_handler("cancel", |_req: CancelRequest| {
            panic!("handler must never run on a deserialization failure");
        });
    }

    #[test]
    fn workflow_context_history_event_count_reports_loaded_history() {
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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

        assert!(matches!(result, Err(HarvestError::NonDeterministic { .. })));
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::WorkflowContinuedAsNew {
                new_exec_id: ExecutionId::new(),
                input: serde_json::json!({"cycle": 3}),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let result = ctx.continue_as_new(serde_json::json!({"cycle": 2})).await;

        assert!(matches!(result, Err(HarvestError::NonDeterministic { .. })));
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

        // Heartbeat should return ActivityCancelled error.
        let result = ctx.heartbeat(serde_json::json!({})).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, HarvestError::ActivityCancelled(_)));
    }

    #[tokio::test]
    async fn activity_check_cancellation_returns_ok_when_not_cancelled() {
        let (tx, _rx) = tokio::sync::mpsc::channel(16);
        let cancel = tokio_util::sync::CancellationToken::new();
        let ctx = ActivityContext::new(Arc::new(HashMap::new()), Some(tx), cancel);
        assert!(ctx.check_cancellation().await.is_ok());
    }

    #[tokio::test]
    async fn activity_check_cancellation_returns_activity_cancelled_when_token_set() {
        let (tx, _rx) = tokio::sync::mpsc::channel(16);
        let cancel = tokio_util::sync::CancellationToken::new();
        let ctx = ActivityContext::new(Arc::new(HashMap::new()), Some(tx), cancel.clone());
        cancel.cancel();
        let result = ctx.check_cancellation().await;
        assert!(
            matches!(result, Err(HarvestError::ActivityCancelled(_))),
            "check_cancellation should return ActivityCancelled when token is set"
        );
    }

    #[tokio::test]
    async fn activity_check_cancellation_works_without_heartbeat_channel() {
        // Local activities have no heartbeat channel; check_cancellation must
        // still detect token cancellation correctly.
        let cancel = tokio_util::sync::CancellationToken::new();
        let ctx = ActivityContext::new(Arc::new(HashMap::new()), None, cancel.clone());
        assert!(ctx.check_cancellation().await.is_ok());
        cancel.cancel();
        let result = ctx.check_cancellation().await;
        assert!(
            matches!(result, Err(HarvestError::ActivityCancelled(_))),
            "check_cancellation on token-less context should still fire on cancel"
        );
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
        assert!(matches!(
            result.unwrap_err(),
            HarvestError::ActivityCancelled(_)
        ));
    }

    #[test]
    fn context_now_returns_deterministic_time() {
        let fixed_time = DateTime::parse_from_rfc3339("2026-01-15T10:30:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);

        let events = vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: fixed_time,
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
    async fn context_replays_circuit_open_failure_with_typed_metadata() {
        // Consumability (issue #369): a replayed CircuitOpen ActivityFailed must
        // surface its typed error_type and details to workflow code, not just a
        // human message — so workflows can branch on `is_circuit_open()` and read
        // `retry_after_secs` deterministically on replay.
        let activity_id = ActivityExecId::new();
        let failure = crate::failure::ActivityFailure::circuit_open(
            "charge_card",
            None,
            Some(std::time::Duration::from_secs(45)),
        );
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "charge_card".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityFailed {
                activity_id,
                error: failure.message.clone(),
                attempt: 1,
                error_type: failure.error_type.clone(),
                non_retryable: true,
                details: failure.details.clone(),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let err = ctx
            .execute_activity_raw("charge_card", Value::Null, "default")
            .await
            .expect_err("replay must reproduce the CircuitOpen failure");

        assert_eq!(err.activity_error_type(), Some("CircuitOpen"));
        assert!(err.is_circuit_open());
        let details = err
            .activity_details()
            .expect("CircuitOpen failure carries structured details on replay");
        assert!((details["retry_after_secs"].as_f64().unwrap() - 45.0).abs() < 0.001);
    }

    #[tokio::test]
    async fn context_replays_timed_out_activity() {
        let activity_id = ActivityExecId::new();

        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
        if let Err(HarvestError::NonDeterministic { reason: msg, .. }) = result {
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

    // ── Patched / deprecate_patch (issue #687) ─────────────────────────────

    #[test]
    fn context_patched_records_marker_and_returns_true_on_live_execution() {
        let ctx = WorkflowContext::new_test();

        assert!(ctx.patched("billing_v2"));

        let cmds = ctx.drain_commands();
        assert_eq!(cmds.len(), 1);
        assert!(
            matches!(&cmds[0], WorkflowCommand::RecordMarker { name, .. } if name == "patch:billing_v2")
        );
    }

    #[test]
    fn context_patched_returns_true_on_replay_with_marker() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::MarkerRecorded {
                name: "patch:billing_v2".into(),
                details: serde_json::json!(1),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        assert!(ctx.patched("billing_v2"));

        // No commands during replay.
        assert!(ctx.drain_commands().is_empty());
    }

    #[tokio::test]
    async fn context_patched_returns_false_on_replay_without_marker() {
        let activity_id = crate::types::ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "some_activity".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!("ok"),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        // Pre-patch history: no marker at this position → old branch.
        assert!(!ctx.patched("billing_v2"));
        assert!(ctx.drain_commands().is_empty());

        // The cursor was not advanced — the recorded activity still matches.
        let output = ctx
            .execute_activity_raw("some_activity", Value::Null, "default")
            .await
            .expect("activity at cursor must still match after patched() miss");
        assert_eq!(output, serde_json::json!("ok"));
    }

    #[test]
    fn context_patched_observes_version_marker_as_patched() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::MarkerRecorded {
                name: "version:billing_v2".into(),
                details: serde_json::json!(2),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        // Interop: a run recorded under the old ctx.version() API is patched.
        assert!(ctx.patched("billing_v2"));
        assert!(ctx.drain_commands().is_empty());
    }

    #[test]
    fn context_deprecate_patch_emits_no_commands_on_live_execution() {
        let ctx = WorkflowContext::new_test();

        ctx.deprecate_patch("x");
        assert!(ctx.drain_commands().is_empty());

        // The documented footgun, pinned deliberately: after deprecate_patch,
        // a residual patched() call treats a NEW execution as unpatched and
        // records nothing. The fix is to delete the residual call.
        assert!(!ctx.patched("x"));
        assert!(ctx.drain_commands().is_empty());
    }

    #[tokio::test]
    async fn context_deprecate_patch_consumes_marker_anywhere_and_residual_patched_returns_true() {
        let activity_id = crate::types::ActivityExecId::new();
        // Phase-1 history: the marker sits at the old patched() call position,
        // BEFORE the activity — but phase-2 code deprecates first.
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::MarkerRecorded {
                name: "patch:x".into(),
                details: serde_json::json!(1),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "some_activity".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!("ok"),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        ctx.deprecate_patch("x");

        // The marker is transparent wherever it sits — the activity matches.
        let output = ctx
            .execute_activity_raw("some_activity", Value::Null, "default")
            .await
            .expect("activity must match cleanly past the deprecated marker");
        assert_eq!(output, serde_json::json!("ok"));

        // Residual patched() call stays deterministic: marker was present.
        assert!(ctx.patched("x"));
        assert!(ctx.drain_commands().is_empty());
    }

    #[tokio::test]
    async fn context_deprecate_patch_tolerates_pre_patch_history() {
        let activity_id = crate::types::ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "some_activity".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!("ok"),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        // Phase-0 history has no marker: deprecation no-ops.
        ctx.deprecate_patch("x");

        let output = ctx
            .execute_activity_raw("some_activity", Value::Null, "default")
            .await
            .expect("activity must match after a no-op deprecation");
        assert_eq!(output, serde_json::json!("ok"));

        assert!(!ctx.patched("x"));
        assert!(ctx.drain_commands().is_empty());
    }

    #[tokio::test]
    async fn context_deprecate_patch_consumes_version_marker_interop() {
        let activity_id = crate::types::ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::MarkerRecorded {
                name: "version:x".into(),
                details: serde_json::json!(2),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "some_activity".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!("ok"),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        ctx.deprecate_patch("x");

        let output = ctx
            .execute_activity_raw("some_activity", Value::Null, "default")
            .await
            .expect("activity must match cleanly past the deprecated version marker");
        assert_eq!(output, serde_json::json!("ok"));

        assert!(ctx.patched("x"));
        assert!(ctx.drain_commands().is_empty());
    }

    /// An empty patch id is a programmer error — must panic immediately with
    /// a clear message (mirrors `version_panics_when_min_exceeds_max`).
    #[test]
    #[should_panic(expected = "patch id must not be empty")]
    fn patched_panics_on_empty_patch_id() {
        let ctx = WorkflowContext::new_test();
        ctx.patched("");
    }

    #[test]
    #[should_panic(expected = "patch id must not be empty")]
    fn deprecate_patch_panics_on_empty_patch_id() {
        let ctx = WorkflowContext::new_test();
        ctx.deprecate_patch("");
    }

    #[test]
    fn context_patched_then_deprecate_then_patched_is_consistent_live() {
        // The sandwich flip (review finding F1): on the live cycle the first
        // patched() call's marker exists only as a pending command, so a
        // naive deprecate_patch history scan would latch `false` and the
        // residual patched() would return false live / true on replay — a
        // permanent nd-block. The this-cycle latch makes the memo agree
        // with every replay cycle.
        let ctx = WorkflowContext::new_test();

        assert!(ctx.patched("x"));
        ctx.deprecate_patch("x");
        assert!(
            ctx.patched("x"),
            "residual patched() must agree with the marker recorded this cycle"
        );

        // Exactly ONE marker command: the residual call hits the memo and
        // must not record a second marker.
        let cmds = ctx.drain_commands();
        assert_eq!(cmds.len(), 1);
        assert!(
            matches!(&cmds[0], WorkflowCommand::RecordMarker { name, .. } if name == "patch:x")
        );
    }

    #[test]
    fn context_patched_is_deterministic_across_repeated_calls() {
        // Live: two calls → two markers, both true.
        let ctx = WorkflowContext::new_test();
        assert!(ctx.patched("x"));
        assert!(ctx.patched("x"));
        let cmds = ctx.drain_commands();
        assert_eq!(cmds.len(), 2);
        assert!(
            cmds.iter().all(
                |c| matches!(c, WorkflowCommand::RecordMarker { name, .. } if name == "patch:x")
            )
        );

        // Replay of a history with two markers → both calls true, no commands.
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::MarkerRecorded {
                name: "patch:x".into(),
                details: serde_json::json!(1),
            },
            WorkflowEvent::MarkerRecorded {
                name: "patch:x".into(),
                details: serde_json::json!(1),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        assert!(ctx.patched("x"));
        assert!(ctx.patched("x"));
        assert!(ctx.drain_commands().is_empty());
    }

    #[test]
    fn context_side_effect_returns_recorded_value() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
            WorkflowCommand::RecordSideEffect { kind, name, value } => {
                assert_eq!(*kind, crate::event::SideEffectKind::Custom);
                assert_eq!(name.as_deref(), Some("random_num"));
                assert_eq!(value, &serde_json::json!(42));
            }
            _ => panic!("Expected RecordSideEffect command"),
        }
    }

    #[test]
    fn context_random_uuid_returns_recorded_value() {
        let expected_uuid = uuid::Uuid::new_v4();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
            WorkflowCommand::RecordSideEffect { kind, name, value } => {
                assert_eq!(*kind, crate::event::SideEffectKind::Custom);
                assert_eq!(name.as_deref(), Some("txn_id"));
                assert_eq!(value, &serde_json::json!(result));
            }
            _ => panic!("Expected RecordSideEffect command"),
        }
    }

    // ── Deterministic built-in primitives (issue #384) ────────────────────────

    use crate::event::SideEffectKind;

    #[test]
    fn system_now_emits_side_effect_event_during_live_execution() {
        let ctx = WorkflowContext::new_test();
        let t = ctx.system_now();
        // The captured instant is a real wall-clock value (>= the year 2020).
        assert!(t.timestamp() > 1_577_836_800);

        let cmds = ctx.drain_commands();
        assert_eq!(cmds.len(), 1);
        match &cmds[0] {
            WorkflowCommand::RecordSideEffect { kind, name, .. } => {
                assert_eq!(*kind, SideEffectKind::Now);
                assert_eq!(*name, None, "built-in primitives have no name");
            }
            other => panic!("expected RecordSideEffect, got {other:?}"),
        }
    }

    #[test]
    fn system_now_replays_recorded_value_verbatim() {
        let frozen_millis = 1_700_000_123_456_i64;
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::SideEffectRecorded {
                kind: SideEffectKind::Now,
                name: None,
                value: serde_json::json!(frozen_millis),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let t = ctx.system_now();
        assert_eq!(t.timestamp_millis(), frozen_millis);
        // No command emitted on replay.
        assert!(ctx.drain_commands().is_empty());
        assert!(ctx.take_deferred_nd_error().is_none());
    }

    #[test]
    fn new_uuid_is_v7_and_captured() {
        let ctx = WorkflowContext::new_test();
        let id = ctx.new_uuid();
        assert_eq!(id.get_version_num(), 7, "new_uuid must mint a UUIDv7");
        let cmds = ctx.drain_commands();
        assert!(matches!(
            &cmds[0],
            WorkflowCommand::RecordSideEffect {
                kind: SideEffectKind::Uuid,
                name: None,
                ..
            }
        ));
    }

    #[test]
    fn new_uuid_replays_recorded_value() {
        let expected = uuid::Uuid::now_v7();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::SideEffectRecorded {
                kind: SideEffectKind::Uuid,
                name: None,
                value: serde_json::json!(expected),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        assert_eq!(ctx.new_uuid(), expected);
    }

    #[test]
    fn random_helpers_replay_recorded_values() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::SideEffectRecorded {
                kind: SideEffectKind::Random,
                name: None,
                value: serde_json::json!(42_u64),
            },
            WorkflowEvent::SideEffectRecorded {
                kind: SideEffectKind::Random,
                name: None,
                value: serde_json::json!(7_i32),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        assert_eq!(ctx.random_u64(), 42);
        assert_eq!(ctx.random_range(0..100_i32), 7);
        assert!(ctx.take_deferred_nd_error().is_none());
    }

    #[test]
    fn random_f64_rejects_out_of_domain_replayed_value() {
        // A call site changed from random_u64() to random_f64(): the recorded
        // draw (42) deserializes fine as 42.0 but violates the [0, 1) contract.
        // It must be reported as drift, not silently returned.
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::SideEffectRecorded {
                kind: SideEffectKind::Random,
                name: None,
                value: serde_json::json!(42_u64),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let v = ctx.random_f64();
        assert!(
            (0.0..1.0).contains(&v),
            "fallback draw must still honour the contract"
        );
        let nd = ctx
            .take_deferred_nd_error()
            .expect("out-of-domain replay must record drift");
        assert!(nd.contains("side-effect drift mismatch"), "{nd}");
        assert!(nd.contains("out-of-domain"), "{nd}");
    }

    #[test]
    fn random_f64_replays_valid_recorded_value() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::SideEffectRecorded {
                kind: SideEffectKind::Random,
                name: None,
                value: serde_json::json!(0.25_f64),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        assert!((ctx.random_f64() - 0.25).abs() < f64::EPSILON);
        assert!(ctx.take_deferred_nd_error().is_none());
    }

    #[test]
    fn random_range_rejects_out_of_range_replayed_value() {
        // History captured 7 from random_range(0..100); the code now narrows to
        // 0..5. The replayed 7 is outside the current range and must drift.
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::SideEffectRecorded {
                kind: SideEffectKind::Random,
                name: None,
                value: serde_json::json!(7_i32),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let v: i32 = ctx.random_range(0..5_i32);
        assert!((0..5).contains(&v), "fallback draw must honour the range");
        let nd = ctx
            .take_deferred_nd_error()
            .expect("out-of-range replay must record drift");
        assert!(nd.contains("side-effect drift mismatch"), "{nd}");
        assert!(nd.contains("out-of-range"), "{nd}");
    }

    /// Replay safety (AC): a workflow that calls `system_now()` 1,000 times
    /// produces a byte-identical sequence of recorded values on every replay.
    #[test]
    fn thousand_now_calls_replay_byte_identical() {
        // Build a history with 1,000 distinct recorded Now values.
        let mut events = vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }];
        for i in 0..1000_i64 {
            events.push(WorkflowEvent::SideEffectRecorded {
                kind: SideEffectKind::Now,
                name: None,
                value: serde_json::json!(1_700_000_000_000_i64 + i),
            });
        }

        // Replay twice; both passes must yield the identical ordered sequence.
        let run = || {
            let ctx = WorkflowContext::for_replay(ExecutionId::new(), events.clone());
            let seq: Vec<i64> = (0..1000)
                .map(|_| ctx.system_now().timestamp_millis())
                .collect();
            assert!(ctx.take_deferred_nd_error().is_none());
            seq
        };
        let first = run();
        let second = run();
        assert_eq!(first, second);
        assert_eq!(first[0], 1_700_000_000_000);
        assert_eq!(first[999], 1_700_000_000_999);
    }

    /// Drift detection (AC): an infallible built-in that diverges from history
    /// records a deferred non-determinism error the executor can surface.
    #[test]
    fn builtin_primitive_records_deferred_nd_on_divergence() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            // History expects an activity here, but the code calls system_now().
            WorkflowEvent::ActivityScheduled {
                activity_id: crate::types::ActivityExecId::new(),
                name: "some_activity".into(),
                input: Value::Null,
                queue: "default".into(),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let _ = ctx.system_now(); // returns a fallback value, records the drift
        let nd = ctx
            .take_deferred_nd_error()
            .expect("drift must be recorded");
        assert!(
            nd.contains("side-effect drift mismatch"),
            "message must classify as side-effect drift: {nd}"
        );
        assert!(nd.contains("ActivityScheduled"), "actual event named: {nd}");
    }

    #[test]
    fn builtin_kind_mismatch_records_drift() {
        // History recorded a Uuid capture but the code now calls system_now().
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::SideEffectRecorded {
                kind: SideEffectKind::Uuid,
                name: None,
                value: serde_json::json!(uuid::Uuid::now_v7()),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let _ = ctx.system_now();
        let nd = ctx
            .take_deferred_nd_error()
            .expect("kind drift must be recorded");
        assert!(nd.contains("side-effect drift mismatch"), "{nd}");
        assert!(
            nd.contains("SideEffectRecorded(uuid)"),
            "actual kind named: {nd}"
        );
    }

    #[test]
    fn side_effect_reads_legacy_marker_for_in_flight_compat() {
        // In-flight executions recorded side effects as MarkerRecorded under the
        // pre-#384 engine. The migrated matcher must still replay them.
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::MarkerRecorded {
                name: "side_effect:legacy".into(),
                details: serde_json::json!("hello"),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let v: String = ctx.side_effect("legacy", || "fresh".to_string()).unwrap();
        assert_eq!(v, "hello");
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
                    last_completion_result: None,
                    last_error: None,
                    scheduled_time: None,
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
        assert!(matches!(err, HarvestError::NonDeterministic { .. }));
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
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
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
            HarvestError::NonDeterministic { .. }
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
            HarvestError::NonDeterministic { .. }
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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

        assert!(matches!(result, Err(HarvestError::NonDeterministic { .. })));
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
            HarvestError::NonDeterministic { .. }
        ));
    }

    #[tokio::test]
    async fn wait_for_signal_replays_recorded_signal() {
        let exec_id = ExecutionId::new();
        let history = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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

    // ── wait_for_signal_timeout / receive_signal_timeout (issue #476) ──────

    #[tokio::test]
    async fn wait_for_signal_timeout_returns_payload_when_signal_recorded_first() {
        let history = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new("__signal_timeout:1:approval"),
                duration_secs: 300,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".to_string(),
                payload: serde_json::json!({"approved": true}),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), history);

        let result = ctx
            .wait_for_signal_timeout("approval", std::time::Duration::from_secs(300))
            .await
            .expect("race should replay");
        assert_eq!(result, Some(serde_json::json!({"approved": true})));
        assert!(ctx.drain_commands().is_empty());
    }

    #[tokio::test]
    async fn wait_for_signal_timeout_returns_none_when_timer_recorded_first() {
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let history = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired { timer_id },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), history);

        let result = ctx
            .wait_for_signal_timeout("approval", std::time::Duration::from_secs(300))
            .await
            .expect("race should replay");
        assert_eq!(result, None);
        assert!(ctx.drain_commands().is_empty());
    }

    #[tokio::test]
    async fn wait_for_signal_timeout_timer_win_keeps_late_signal_observable() {
        // The signal arrived after the deadline: the race returns None and the
        // late signal is still consumable by a subsequent receive_signal call.
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let history = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired { timer_id },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".to_string(),
                payload: serde_json::json!({"approved": true}),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), history);

        let result = ctx
            .wait_for_signal_timeout("approval", std::time::Duration::from_secs(300))
            .await
            .expect("race should replay");
        assert_eq!(result, None, "timer won — no signal payload consumed");

        let late = ctx
            .wait_for_signal("approval")
            .await
            .expect("late signal must still be deliverable");
        assert_eq!(late, serde_json::json!({"approved": true}));
    }

    #[tokio::test]
    async fn wait_for_signal_timeout_replays_signal_branch_when_both_events_exist() {
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let history = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 300,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".to_string(),
                payload: serde_json::json!({"approved": true}),
            },
            WorkflowEvent::TimerFired { timer_id },
        ];

        // Whichever event is first in recorded history wins on every replay.
        for _ in 0..3 {
            let ctx = WorkflowContext::for_replay(ExecutionId::new(), history.clone());
            let result = ctx
                .wait_for_signal_timeout("approval", std::time::Duration::from_secs(300))
                .await
                .expect("race should replay");
            assert_eq!(result, Some(serde_json::json!({"approved": true})));
        }
    }

    #[tokio::test]
    async fn receive_signal_timeout_deserializes_typed_payload() {
        #[derive(serde::Deserialize, Debug, PartialEq, Eq)]
        struct Approval {
            approved: bool,
        }

        let history = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new("__signal_timeout:1:approval"),
                duration_secs: 60,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".to_string(),
                payload: serde_json::json!({"approved": true}),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), history);

        let result: Option<Approval> = ctx
            .receive_signal_timeout("approval", std::time::Duration::from_secs(60))
            .await
            .expect("race should replay");
        assert_eq!(result, Some(Approval { approved: true }));
    }

    #[tokio::test]
    async fn receive_signal_timeout_returns_none_on_timeout_branch() {
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let history = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 60,
            },
            WorkflowEvent::TimerFired { timer_id },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), history);

        let result: Option<Value> = ctx
            .receive_signal_timeout("approval", std::time::Duration::from_secs(60))
            .await
            .expect("race should replay");
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn wait_for_signal_timeout_signal_win_without_recorded_timer() {
        // Signal arrived before the race even started on the live run — the
        // timer was never started, so no TimerStarted event exists.
        let history = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".to_string(),
                payload: serde_json::json!({"approved": false}),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), history);

        let result = ctx
            .wait_for_signal_timeout("approval", std::time::Duration::from_secs(300))
            .await
            .expect("race should replay");
        assert_eq!(result, Some(serde_json::json!({"approved": false})));
        assert!(
            ctx.drain_commands().is_empty(),
            "no timer must be started when the signal already won"
        );
    }

    #[tokio::test]
    async fn wait_for_signal_timeout_live_emits_timer_and_signal_wait_commands() {
        let ctx = WorkflowContext::new_test();

        let fut = ctx.wait_for_signal_timeout("approval", std::time::Duration::from_millis(1500));
        let mut fut = Box::pin(fut);
        let waker = std::task::Waker::noop();
        let mut poll_cx = std::task::Context::from_waker(waker);
        assert!(
            std::future::Future::poll(fut.as_mut(), &mut poll_cx).is_pending(),
            "live race must suspend"
        );

        let cmds = ctx.drain_commands();
        assert_eq!(cmds.len(), 2, "expected StartTimer + WaitForSignal");
        let WorkflowCommand::StartTimer {
            timer_id,
            duration_secs,
            ..
        } = &cmds[0]
        else {
            panic!("first command must be StartTimer, got {cmds:?}");
        };
        assert_eq!(timer_id.as_str(), "__signal_timeout:1:approval");
        assert_eq!(
            *duration_secs, 2,
            "sub-second timeouts round up to whole seconds"
        );
        assert!(
            matches!(
                &cmds[1],
                WorkflowCommand::WaitForSignal { signal_name, .. } if signal_name == "approval"
            ),
            "second command must be WaitForSignal, got {cmds:?}"
        );
    }

    #[tokio::test]
    async fn wait_for_signal_timeout_diverges_on_unrelated_history() {
        let history = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: ActivityExecId::new(),
                name: "send_email".into(),
                input: Value::Null,
                queue: "default".into(),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), history);

        let result = ctx
            .wait_for_signal_timeout("approval", std::time::Duration::from_secs(300))
            .await;
        assert!(matches!(
            result.unwrap_err(),
            HarvestError::NonDeterministic { .. }
        ));
    }

    #[test]
    fn context_is_not_cancelled_without_terminal_event() {
        let events = vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
        assert!(matches!(result, Err(HarvestError::NonDeterministic { .. })));
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
    fn upsert_search_attrs_rejects_nd_diagnostic_keys() {
        // Issue #603 fix: a workflow author must never be able to write one of
        // the six replay-non-determinism diagnostic key names — doing so would
        // let `nd_search_attrs_clear_patch`'s recovery clear silently delete
        // the author's own business data on an unrelated ND-block recovery.
        let ctx = WorkflowContext::new_test();
        for key in &[
            "failure_cause",
            "event_index",
            "expected",
            "actual",
            "workflow_type",
            "build_id",
        ] {
            let err = ctx
                .upsert_search_attrs([(key.to_string(), Some(Value::String("x".to_string())))])
                .expect_err(&format!("ND diagnostic key '{key}' must be rejected"));
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
    async fn signal_external_workflow_with_idempotency_threads_key_into_command() {
        // The opt-in key must be carried on the emitted command so the worker
        // persists it into ExternalSignalRequested and dedupes the target's
        // signal insert.
        let target = ExecutionId::new();
        let cmds = {
            let ctx = WorkflowContext::new_test();
            let ctx_ref = &ctx;
            let cmd_fut = ctx_ref.signal_external_workflow_with_idempotency(
                target,
                "tenant_cancel",
                serde_json::json!({"reason": "billing_lapse"}),
                "evt_abc".to_string(),
            );
            let _ = tokio::time::timeout(std::time::Duration::from_millis(1), cmd_fut).await;
            ctx.drain_commands()
        };

        assert_eq!(cmds.len(), 1, "one SignalExternalWorkflow command expected");
        match &cmds[0] {
            WorkflowCommand::SignalExternalWorkflow {
                idempotency_key, ..
            } => {
                assert_eq!(
                    idempotency_key.as_deref(),
                    Some("evt_abc"),
                    "the opt-in key must be threaded onto the command"
                );
            }
            other => panic!("expected SignalExternalWorkflow, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn signal_external_workflow_without_key_has_none_on_command() {
        // The plain (legacy) method must emit a command with no key.
        let target = ExecutionId::new();
        let cmds = {
            let ctx = WorkflowContext::new_test();
            let ctx_ref = &ctx;
            let cmd_fut = ctx_ref.signal_external_workflow(
                target,
                "tenant_cancel",
                serde_json::json!({"reason": "billing_lapse"}),
            );
            let _ = tokio::time::timeout(std::time::Duration::from_millis(1), cmd_fut).await;
            ctx.drain_commands()
        };
        match &cmds[0] {
            WorkflowCommand::SignalExternalWorkflow {
                idempotency_key, ..
            } => assert_eq!(idempotency_key.as_deref(), None),
            other => panic!("expected SignalExternalWorkflow, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn signal_external_workflow_crash_recovery_reuses_recorded_key() {
        // Replay-safety: ExternalSignalRequested is durable but no terminal
        // event follows (worker crashed mid-delivery). The re-dispatch
        // must reuse the *recorded* key, even if the current code passes a
        // different one — otherwise a code change could diverge an in-flight
        // delivery that the outbox later resolves.
        let signal_id = crate::types::ExternalSignalId::new();
        let target = ExecutionId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ExternalSignalRequested {
                signal_id,
                target,
                signal_name: "tenant_cancel".into(),
                payload: serde_json::json!({"reason": "billing_lapse"}),
                idempotency_key: Some("recorded_key".into()),
            },
            // no terminal event → crash-recovery path
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let ctx_ref = &ctx;
        // Current code passes a *different* key; the recorded one must win.
        let cmd_fut = ctx_ref.signal_external_workflow_with_idempotency(
            target,
            "tenant_cancel",
            serde_json::json!({"reason": "billing_lapse"}),
            "different_key".to_string(),
        );
        let _ = tokio::time::timeout(std::time::Duration::from_millis(1), cmd_fut).await;
        let cmds = ctx.drain_commands();

        assert_eq!(cmds.len(), 1, "crash recovery re-emits the signal command");
        match &cmds[0] {
            WorkflowCommand::SignalExternalWorkflow {
                already_requested,
                idempotency_key,
                signal_id: cmd_signal_id,
                ..
            } => {
                assert!(already_requested, "request event already durable");
                assert_eq!(
                    idempotency_key.as_deref(),
                    Some("recorded_key"),
                    "recovery must reuse the recorded key, not the current argument"
                );
                assert_eq!(
                    *cmd_signal_id, signal_id,
                    "recovery reuses recorded signal_id"
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ExternalSignalRequested {
                signal_id,
                target,
                signal_name: "tenant_cancel".into(),
                payload: serde_json::json!({"reason": "billing_lapse"}),
                idempotency_key: None,
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ExternalSignalRequested {
                signal_id,
                target,
                signal_name: "cancel".into(),
                payload: Value::Null,
                idempotency_key: None,
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ExternalSignalRequested {
                signal_id,
                target,
                signal_name: "notify".into(),
                payload: Value::Null,
                idempotency_key: None,
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ExternalSignalRequested {
                signal_id,
                target,
                signal_name: "cancel".into(),
                payload: Value::Null,
                idempotency_key: None,
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
            HarvestError::NonDeterministic { reason: msg, .. } => {
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ExternalSignalRequested {
                signal_id,
                target,
                signal_name: "cancel".into(),
                payload: Value::Null,
                idempotency_key: None,
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
            HarvestError::NonDeterministic { reason: msg, .. } => {
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
                idempotency_key: None,
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

    // ── request_cancel_external_workflow tests (issue #492) ──────────────────

    #[tokio::test]
    async fn cancel_external_workflow_live_mode_emits_command() {
        let target = ExecutionId::new();
        let ctx = WorkflowContext::new_test();
        let ctx_ref = &ctx;
        let target_clone = target;
        let cmd_fut = ctx_ref.request_cancel_external_workflow(target_clone);
        let _ = tokio::time::timeout(std::time::Duration::from_millis(1), cmd_fut).await;
        let cmds = ctx.drain_commands();

        assert_eq!(
            cmds.len(),
            1,
            "one RequestCancelExternalWorkflow command expected"
        );
        match &cmds[0] {
            WorkflowCommand::RequestCancelExternalWorkflow {
                target: t,
                already_requested,
                ..
            } => {
                assert_eq!(*t, target_clone);
                assert!(
                    !already_requested,
                    "first call should not be already_requested"
                );
            }
            other => panic!("expected RequestCancelExternalWorkflow, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn cancel_external_workflow_replays_delivered_outcome() {
        let cancel_id = crate::types::ExternalCancelId::new();
        let target = ExecutionId::new();

        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ExternalCancelRequested { cancel_id, target },
            WorkflowEvent::ExternalCancelDelivered { cancel_id },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let result = ctx.request_cancel_external_workflow(target).await;

        assert!(result.is_ok(), "delivered history should return Ok(())");
        assert!(ctx.drain_commands().is_empty(), "replay emits no commands");
    }

    #[tokio::test]
    async fn cancel_external_workflow_replays_failed_outcome_target_unknown() {
        let cancel_id = crate::types::ExternalCancelId::new();
        let target = ExecutionId::new();

        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ExternalCancelRequested { cancel_id, target },
            WorkflowEvent::ExternalCancelFailed {
                cancel_id,
                reason_code: "target_unknown".into(),
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let result = ctx.request_cancel_external_workflow(target).await;

        assert!(result.is_err(), "failed history should return Err");
        match result.unwrap_err() {
            HarvestError::ExternalCancelFailed { reason_code, .. } => {
                assert_eq!(reason_code, "target_unknown");
            }
            other => panic!("expected ExternalCancelFailed, got {other:?}"),
        }
        assert!(ctx.drain_commands().is_empty(), "replay emits no commands");
    }

    #[tokio::test]
    async fn cancel_external_workflow_nondeterminism_wrong_target() {
        let cancel_id = crate::types::ExternalCancelId::new();
        let target = ExecutionId::new();
        let other_target = ExecutionId::new();

        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ExternalCancelRequested { cancel_id, target },
            WorkflowEvent::ExternalCancelDelivered { cancel_id },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let result = ctx.request_cancel_external_workflow(other_target).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            HarvestError::NonDeterministic { reason: msg, .. } => {
                assert!(msg.contains("external cancel mismatch"), "msg: {msg}");
            }
            other => panic!("expected NonDeterministic, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn cancel_external_workflow_self_cancel_rejected() {
        let own_id = ExecutionId::new();
        let ctx = WorkflowContext::for_replay(
            own_id,
            vec![WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            }],
        );
        let result = ctx.request_cancel_external_workflow(own_id).await;
        assert!(result.is_err(), "self-cancel should be rejected");
        match result.unwrap_err() {
            HarvestError::ExternalCancelFailed {
                reason_code,
                target,
                ..
            } => {
                assert_eq!(reason_code, "self_cancel");
                assert_eq!(target, own_id);
            }
            other => panic!("expected ExternalCancelFailed(self_cancel), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn cancel_external_workflow_after_activity_replays_correctly() {
        let cancel_id = crate::types::ExternalCancelId::new();
        let activity_id = ActivityExecId::new();
        let target = ExecutionId::new();

        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
            WorkflowEvent::ExternalCancelRequested { cancel_id, target },
            WorkflowEvent::ExternalCancelDelivered { cancel_id },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let r = ctx
            .execute_activity_raw("step_one", Value::Null, "default")
            .await
            .expect("activity replay ok");
        assert_eq!(r, "done");

        let cancel_result = ctx.request_cancel_external_workflow(target).await;
        assert!(
            cancel_result.is_ok(),
            "cancel after activity should replay Ok"
        );
        assert!(
            ctx.drain_commands().is_empty(),
            "no live commands after full replay"
        );
    }

    #[tokio::test]
    async fn cancel_external_workflow_stashes_interleaved_signal() {
        // Regression: a SignalReceived recorded between ExternalCancelRequested and
        // ExternalCancelDelivered must still be observable by a later receive_signal.
        // Previously match_external_cancel skipped the signal transparently, jumping
        // the cursor past it on settle and losing it.
        let cancel_id = crate::types::ExternalCancelId::new();
        let target = ExecutionId::new();

        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ExternalCancelRequested { cancel_id, target },
            WorkflowEvent::SignalReceived {
                signal_name: "approved".into(),
                payload: serde_json::json!({"ok": true}),
            },
            WorkflowEvent::ExternalCancelDelivered { cancel_id },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        let cancel_result = ctx.request_cancel_external_workflow(target).await;
        assert!(cancel_result.is_ok(), "cancel should replay Ok");

        // The interleaved signal must still be observable, not lost to the cursor jump.
        let sig = ctx
            .wait_for_signal("approved")
            .await
            .expect("interleaved signal must be observable after cancel replay");
        assert_eq!(sig, serde_json::json!({"ok": true}));

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
            default_schedule_to_close: None,
            default_queue: None,
            max_concurrent: None,
            concurrency_key: None,
            is_local: local,
            max_input_bytes: None,
            max_result_bytes: None,
            rate_limit_rps: None,
            rate_limit_burst: None,
            rate_limit_key: None,
            circuit_breaker: None,
            requires: None,
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        }
    }

    fn make_workflow_info(name: &'static str) -> crate::info::WorkflowInfo {
        crate::info::WorkflowInfo {
            mcp: false,
            name,
            module: "test",
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
            execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            max_input_bytes: None,
            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }
    }

    #[tokio::test]
    async fn execute_activity_typed_replays_completed_output() {
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
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

    // ── set_current_details tests (issue #473) ────────────────────────────

    /// Helper: extract the last `SetCurrentDetails` value from a command list.
    fn last_set_current_details(cmds: &[WorkflowCommand]) -> Option<&str> {
        cmds.iter().rev().find_map(|cmd| {
            if let WorkflowCommand::SetCurrentDetails { value, .. } = cmd {
                Some(value.as_str())
            } else {
                None
            }
        })
    }

    /// Helper: extract the last `SetCurrentDetails` command's `explicit_clear` flag.
    fn last_set_current_details_explicit_clear(cmds: &[WorkflowCommand]) -> Option<bool> {
        cmds.iter().rev().find_map(|cmd| {
            if let WorkflowCommand::SetCurrentDetails { explicit_clear, .. } = cmd {
                Some(*explicit_clear)
            } else {
                None
            }
        })
    }

    #[test]
    fn set_current_details_pushes_set_current_details_command() {
        let ctx = WorkflowContext::new_test();
        ctx.set_current_details("Step 3/5: awaiting vendor approval");
        let cmds = ctx.drain_commands();
        assert_eq!(cmds.len(), 1, "exactly one command should be pushed");
        assert!(
            matches!(&cmds[0], WorkflowCommand::SetCurrentDetails { value, explicit_clear } if value == "Step 3/5: awaiting vendor approval" && !explicit_clear),
            "command must be SetCurrentDetails with the correct value and explicit_clear=false"
        );
    }

    #[test]
    fn set_current_details_last_write_wins() {
        let ctx = WorkflowContext::new_test();
        ctx.set_current_details("first status");
        ctx.set_current_details("second status");
        ctx.set_current_details("third status");
        let cmds = ctx.drain_commands();
        // The worker takes the LAST SetCurrentDetails command.
        assert_eq!(
            last_set_current_details(&cmds),
            Some("third status"),
            "last-write-wins: the last command value wins"
        );
    }

    #[test]
    fn set_current_details_no_command_when_never_set() {
        let ctx = WorkflowContext::new_test();
        let cmds = ctx.drain_commands();
        assert_eq!(
            last_set_current_details(&cmds),
            None,
            "no SetCurrentDetails command when never set"
        );
    }

    #[test]
    fn set_current_details_suppressed_during_replay() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::MarkerRecorded {
                name: "some-marker".into(),
                details: Value::Null,
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        assert!(ctx.is_replaying(), "context must be in replay mode");
        ctx.set_current_details("status during replay");
        // During replay, set_current_details is a no-op: zero commands pushed.
        let cmds = ctx.drain_commands();
        assert!(
            cmds.is_empty(),
            "set_current_details must be suppressed during replay: no commands emitted"
        );
    }

    #[test]
    fn set_current_details_cap_truncates_over_limit() {
        let ctx = WorkflowContext::new_test();
        // Build a string just over the 1 KiB default cap.
        let over_cap = "x".repeat(DEFAULT_CURRENT_DETAILS_CAP_BYTES + 10);
        ctx.set_current_details(over_cap);
        let cmds = ctx.drain_commands();
        let stored =
            last_set_current_details(&cmds).expect("SetCurrentDetails command must be present");
        assert!(
            stored.len() <= DEFAULT_CURRENT_DETAILS_CAP_BYTES,
            "stored value length {} exceeds cap {}",
            stored.len(),
            DEFAULT_CURRENT_DETAILS_CAP_BYTES
        );
    }

    #[test]
    fn set_current_details_within_cap_stored_unchanged() {
        let ctx = WorkflowContext::new_test();
        let msg = "Step 2/4: processing payment";
        ctx.set_current_details(msg);
        let cmds = ctx.drain_commands();
        assert_eq!(last_set_current_details(&cmds), Some(msg));
    }

    #[test]
    fn set_current_details_cap_truncates_on_utf8_boundary() {
        // "ä" is U+00E4, encoded as 2 bytes (0xC3 0xA4) in UTF-8.
        // "äää" = 6 bytes. A cap of 5 must not split the 3rd char,
        // so floor_char_boundary(5) = 4 → "ää" is the result.
        let ctx = WorkflowContext::new_test().with_current_details_cap(5);
        ctx.set_current_details("äää");
        let cmds = ctx.drain_commands();
        let stored =
            last_set_current_details(&cmds).expect("SetCurrentDetails command must be present");
        assert_eq!(
            stored, "ää",
            "truncation must land on a valid char boundary"
        );
        assert!(
            stored.len() <= 5,
            "stored length {} exceeds cap 5",
            stored.len()
        );
    }

    #[test]
    fn set_current_details_empty_string_pushes_empty_command() {
        // The context's job is only to forward the raw value (capped, replay-
        // gated); interpreting an empty string as "clear to NULL" is the
        // worker's responsibility (`worker::latest_current_details_update`,
        // issue #593). This test locks in that the empty string reaches the
        // drained command list unfiltered, with `explicit_clear = true`, so
        // the worker-side clear signal is never silently dropped at the
        // context layer.
        let ctx = WorkflowContext::new_test();
        ctx.set_current_details("in progress");
        ctx.set_current_details("");
        let cmds = ctx.drain_commands();
        assert_eq!(
            last_set_current_details(&cmds),
            Some(""),
            "an empty-string call must still push a SetCurrentDetails command \
             carrying an empty value, so the worker can resolve it to a clear"
        );
        assert_eq!(
            last_set_current_details_explicit_clear(&cmds),
            Some(true),
            "an author-supplied empty string must be marked explicit_clear"
        );
    }

    #[test]
    fn set_current_details_truncated_to_empty_is_not_marked_explicit_clear() {
        // Post-review hardening (issue #593, PR #894): a non-empty status can
        // truncate down to an empty string when current_details_cap is 0 (or
        // smaller than the input's first UTF-8 character). That must NOT be
        // confused with an author-issued clear -- explicit_clear is decided
        // from the pre-truncation input, which was non-empty here.
        let ctx = WorkflowContext::new_test().with_current_details_cap(0);
        ctx.set_current_details("😀 running a very long status");
        let cmds = ctx.drain_commands();
        assert_eq!(
            last_set_current_details(&cmds),
            Some(""),
            "a cap of 0 truncates any non-empty input down to an empty string"
        );
        assert_eq!(
            last_set_current_details_explicit_clear(&cmds),
            Some(false),
            "a non-empty input that truncates to empty must NOT be marked \
             explicit_clear -- only a literal empty-string call should clear"
        );
    }

    #[test]
    fn set_current_details_explicit_empty_under_tiny_cap_is_still_marked_explicit_clear() {
        // An author-issued empty string is still explicit_clear even when the
        // cap is degenerate -- the flag is orthogonal to the cap because it is
        // decided before truncation runs at all.
        let ctx = WorkflowContext::new_test().with_current_details_cap(0);
        ctx.set_current_details("");
        let cmds = ctx.drain_commands();
        assert_eq!(last_set_current_details(&cmds), Some(""));
        assert_eq!(last_set_current_details_explicit_clear(&cmds), Some(true));
    }

    // ── Context headers — WorkflowContext ────────────────────────────────────

    #[test]
    fn workflow_context_header_returns_set_value() {
        let mut headers = std::collections::HashMap::new();
        headers.insert("tenant_id".to_string(), "acme".to_string());
        let ctx = WorkflowContext::new_test().with_context_headers(headers);
        assert_eq!(ctx.header("tenant_id"), Some("acme"));
    }

    #[test]
    fn workflow_context_header_missing_key_returns_none() {
        let ctx = WorkflowContext::new_test();
        assert!(ctx.header("tenant_id").is_none());
    }

    #[test]
    fn workflow_context_headers_returns_all() {
        let mut headers = std::collections::HashMap::new();
        headers.insert("a".to_string(), "1".to_string());
        headers.insert("b".to_string(), "2".to_string());
        let ctx = WorkflowContext::new_test().with_context_headers(headers);
        let map = ctx.headers();
        assert_eq!(map.get("a").map(String::as_str), Some("1"));
        assert_eq!(map.get("b").map(String::as_str), Some("2"));
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn workflow_context_headers_empty_on_default() {
        let ctx = WorkflowContext::new_test();
        assert!(ctx.headers().is_empty());
    }

    // ── Context headers — ActivityContext ────────────────────────────────────

    #[test]
    fn activity_context_header_returns_set_value() {
        let mut headers = std::collections::HashMap::new();
        headers.insert("tenant_id".to_string(), "acme".to_string());
        let ctx = ActivityContext::new_test().with_context_headers(std::sync::Arc::new(headers));
        assert_eq!(ctx.header("tenant_id"), Some("acme"));
    }

    #[test]
    fn activity_context_header_missing_key_returns_none() {
        let ctx = ActivityContext::new_test();
        assert!(ctx.header("tenant_id").is_none());
    }

    #[test]
    fn activity_context_headers_returns_all() {
        let mut headers = std::collections::HashMap::new();
        headers.insert("x".to_string(), "foo".to_string());
        headers.insert("y".to_string(), "bar".to_string());
        let ctx = ActivityContext::new_test().with_context_headers(std::sync::Arc::new(headers));
        let map = ctx.headers();
        assert_eq!(map.get("x").map(String::as_str), Some("foo"));
        assert_eq!(map.get("y").map(String::as_str), Some("bar"));
    }

    #[test]
    fn activity_context_headers_empty_on_default() {
        let ctx = ActivityContext::new_test();
        assert!(ctx.headers().is_empty());
    }

    // ── Orphaned update handler check tests (issue #536) ──────────────────

    #[test]
    fn test_unfinished_update_handler_accessors() {
        let update_id1 = UpdateId::new();
        let update_id2 = UpdateId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::UpdateAdmitted {
                update_id: update_id1,
                name: "update_1".into(),
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::UpdateAdmitted {
                update_id: update_id2,
                name: "update_2".into(),
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::UpdateCompleted {
                update_id: update_id1,
                output: Value::Null,
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        // We expect update_id2 to be unfinished, while update_id1 is finished.
        assert_eq!(
            ctx.matcher
                .lock()
                .unwrap()
                .unfinished_update_handler_count_at_end(),
            1
        );
        assert!(!ctx.matcher.lock().unwrap().all_handlers_finished_at_end());
    }

    #[tokio::test]
    async fn test_await_all_handlers_finished() {
        let update_id = UpdateId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::UpdateAdmitted {
                update_id,
                name: "update_1".into(),
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::UpdateCompleted {
                update_id,
                output: Value::Null,
            },
        ];

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        assert_eq!(ctx.unfinished_update_handler_count(), 0);
        assert!(ctx.all_handlers_finished());
        // Since all handlers are finished, this should resolve immediately
        ctx.await_condition(|| ctx.all_handlers_finished())
            .await
            .unwrap();
    }

    // ── ctx.race() tests (issue #600) ───────────────────────────────────────

    fn race_started_event() -> WorkflowEvent {
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }
    }

    #[tokio::test]
    async fn race_rejects_zero_branches() {
        let ctx = WorkflowContext::new_test();
        let err = ctx.race().run().await.unwrap_err();
        assert!(matches!(err, HarvestError::Config(_)));
    }

    #[tokio::test]
    async fn race_rejects_mixed_activity_and_timer_shape() {
        let ctx = WorkflowContext::new_test();
        let err = ctx
            .race()
            .activity_raw("fetch_a", Value::Null, "default")
            .timer(std::time::Duration::from_secs(60))
            .run()
            .await
            .unwrap_err();
        assert!(matches!(err, HarvestError::Config(_)));
    }

    /// Regression test (Codex review, PR #902): a payload-cap failure on a
    /// fresh branch must leave zero durable trace of the race -- specifically,
    /// the `race:{seq}` open marker must not have been queued, since nothing
    /// else in this cycle (no branch's ScheduleActivity/StartChildWorkflow)
    /// would be recorded to match it on a later replay.
    #[tokio::test]
    async fn race_rejects_oversized_second_branch_without_queuing_open_marker() {
        let ctx = WorkflowContext::new_test().with_payload_caps(
            1024 * 1024,
            2 * 1024 * 1024,
            256 * 1024,
            2 * 1024 * 1024,
        );
        let oversized = serde_json::json!({ "data": "x".repeat(2 * 1024 * 1024) });

        let err = ctx
            .race()
            .activity_raw("fetch_a", Value::Null, "default")
            .activity_raw("fetch_b", oversized, "default")
            .run()
            .await
            .unwrap_err();
        assert!(
            matches!(err, HarvestError::PayloadTooLarge { .. }),
            "expected PayloadTooLarge, got {err:?}"
        );

        let commands = ctx.drain_commands();
        assert!(
            commands.is_empty(),
            "an aborted race must not have queued the open marker (or any \
             branch dispatch command) -- otherwise replay would find the \
             marker with no corresponding branch event and diverge: \
             {commands:?}"
        );
    }

    struct FailsToSerialize;

    impl serde::Serialize for FailsToSerialize {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom(
                "intentional serialization failure",
            ))
        }
    }

    #[tokio::test]
    async fn race_activity_branch_propagates_serialization_error() {
        let ctx = WorkflowContext::new_test();
        let info = make_activity_info("fetch_a", false);
        let err = ctx
            .race()
            .activity(&info, FailsToSerialize)
            .activity_raw("fetch_b", Value::Null, "default")
            .run()
            .await
            .unwrap_err();
        assert!(
            matches!(err, HarvestError::Serialization(_)),
            "expected Serialization error, got {err:?}"
        );
    }

    #[tokio::test]
    async fn race_child_workflow_branch_propagates_serialization_error() {
        let ctx = WorkflowContext::new_test();
        let info = make_workflow_info("child_wf");
        let err = ctx
            .race()
            .child_workflow(&info, FailsToSerialize)
            .run()
            .await
            .unwrap_err();
        assert!(
            matches!(err, HarvestError::Serialization(_)),
            "expected Serialization error, got {err:?}"
        );
    }

    #[tokio::test]
    async fn race_live_dispatch_emits_open_marker_and_schedules_all_branches() {
        let ctx = std::sync::Arc::new(WorkflowContext::new_test());
        let race_ctx = ctx.clone();
        let handle = tokio::spawn(async move {
            race_ctx
                .race()
                .activity_raw("fetch_a", Value::Null, "default")
                .activity_raw("fetch_b", Value::Null, "default")
                .run()
                .await
        });

        // The race never resolves live in this test (no worker completes the
        // activities), so it suspends -- give it a moment to push commands.
        let timeout_result =
            tokio::time::timeout(std::time::Duration::from_millis(50), handle).await;
        assert!(
            timeout_result.is_err(),
            "race with no resolution should suspend"
        );

        let commands = ctx.drain_commands();
        assert!(matches!(
            commands.first(),
            Some(WorkflowCommand::RecordMarker { name, .. }) if name == "race:1"
        ));
        let scheduled = commands
            .iter()
            .filter(|c| matches!(c, WorkflowCommand::ScheduleActivity { .. }))
            .count();
        assert_eq!(
            scheduled, 2,
            "both branches must be dispatched: {commands:?}"
        );
    }

    #[tokio::test]
    async fn race_activity_replays_winner_and_records_marker_plus_cancel_losers()
    -> Result<(), HarvestError> {
        let loser_id = ActivityExecId::new();
        let winner_id = ActivityExecId::new();
        let winner_output = serde_json::json!({"provider": "b"});

        let events = vec![
            race_started_event(),
            WorkflowEvent::MarkerRecorded {
                name: "race:1".to_string(),
                details: Value::from(2u64),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: loser_id,
                name: "fetch_a".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: winner_id,
                name: "fetch_b".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: winner_id,
                output: winner_output.clone(),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        let winner = ctx
            .race()
            .activity_raw("fetch_a", Value::Null, "default")
            .activity_raw("fetch_b", Value::Null, "default")
            .run()
            .await?;

        assert_eq!(
            winner.index, 1,
            "branch b resolved first in history and must win"
        );
        assert_eq!(winner.value, winner_output);

        let commands = ctx.drain_commands();
        assert!(
            commands.iter().any(|c| matches!(
                c,
                WorkflowCommand::RecordMarker { name, details }
                    if name == "race_winner:1" && details.as_u64() == Some(1)
            )),
            "winner marker must be recorded: {commands:?}"
        );
        assert!(
            commands.iter().any(|c| matches!(
                c,
                WorkflowCommand::CancelRaceLosers { activities, .. } if activities == &vec![loser_id]
            )),
            "the still-open loser must be queued for cancellation: {commands:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn race_activity_verifies_previously_recorded_winner_without_new_commands()
    -> Result<(), HarvestError> {
        let loser_id = ActivityExecId::new();
        let winner_id = ActivityExecId::new();
        let winner_output = serde_json::json!({"provider": "b"});

        let events = vec![
            race_started_event(),
            WorkflowEvent::MarkerRecorded {
                name: "race:1".to_string(),
                details: Value::from(2u64),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: loser_id,
                name: "fetch_a".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: winner_id,
                name: "fetch_b".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: winner_id,
                output: winner_output.clone(),
            },
            // Cancellation of the loser already committed on an earlier cycle,
            // synthesizing its terminal exactly as `apply_race_loser_cancellations` does.
            WorkflowEvent::ActivityFailed {
                activity_id: loser_id,
                error: "lost race to a sibling branch".to_string(),
                attempt: 1,
                error_type: "Error".to_string(),
                non_retryable: true,
                details: None,
            },
            WorkflowEvent::MarkerRecorded {
                name: "race_winner:1".to_string(),
                details: Value::from(1u64),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        let winner = ctx
            .race()
            .activity_raw("fetch_a", Value::Null, "default")
            .activity_raw("fetch_b", Value::Null, "default")
            .run()
            .await?;

        assert_eq!(winner.index, 1);
        assert_eq!(winner.value, winner_output);
        assert!(
            ctx.drain_commands().is_empty(),
            "a fully-resolved race must not re-emit any command on replay"
        );
        Ok(())
    }

    /// Regression test (Codex review, PR #902): two `ctx.race()` calls driven
    /// concurrently (e.g. via `futures::join!`) interleave their histories --
    /// a sibling race's own open marker and branch schedules land, in
    /// recorded event order, between this race's own branch schedules and
    /// its already-recorded winner marker (since the sibling was dispatched
    /// on an earlier cycle and hasn't resolved yet). `peek_u64_marker` must
    /// scan past that interleaved sibling data to find `race_winner:1`
    /// rather than treating the sibling's `race:2` marker sitting at the
    /// (rewound) cursor as a miss and re-emitting a duplicate winner marker.
    #[tokio::test]
    async fn race_verifies_previously_recorded_winner_across_an_interleaved_sibling_race()
    -> Result<(), HarvestError> {
        let a1 = ActivityExecId::new();
        let a2 = ActivityExecId::new();
        let b1 = ActivityExecId::new();
        let b2 = ActivityExecId::new();
        let winner_output = serde_json::json!({"provider": "a1"});

        let events = vec![
            race_started_event(),
            // Race #1's own open marker + branch schedules.
            WorkflowEvent::MarkerRecorded {
                name: "race:1".to_string(),
                details: Value::from(2u64),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: a1,
                name: "a1".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: a2,
                name: "a2".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            // Race #2 (a sibling driven concurrently via futures::join!) is
            // dispatched but never resolves in this history snapshot.
            WorkflowEvent::MarkerRecorded {
                name: "race:2".to_string(),
                details: Value::from(2u64),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: b1,
                name: "b1".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: b2,
                name: "b2".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            // Race #1 resolves and is fully recorded, all positioned *after*
            // race #2's still-open branch schedules above.
            WorkflowEvent::ActivityCompleted {
                activity_id: a1,
                output: winner_output.clone(),
            },
            WorkflowEvent::MarkerRecorded {
                name: "race_winner:1".to_string(),
                details: Value::from(0u64),
            },
            WorkflowEvent::ActivityFailed {
                activity_id: a2,
                error: "lost race to a sibling branch".to_string(),
                attempt: 1,
                error_type: "Error".to_string(),
                non_retryable: true,
                details: None,
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        let winner = ctx
            .race()
            .activity_raw("a1", Value::Null, "default")
            .activity_raw("a2", Value::Null, "default")
            .run()
            .await?;

        assert_eq!(winner.index, 0, "a1 completed and must win");
        assert_eq!(winner.value, winner_output);
        let commands = ctx.drain_commands();
        assert!(
            commands.is_empty(),
            "the already-recorded race_winner:1 marker must be found past the \
             interleaved sibling race's events, not re-emitted as a duplicate: \
             {commands:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn race_activity_diverges_when_recorded_winner_disagrees_with_replay() {
        let branch_a = ActivityExecId::new();
        let branch_b = ActivityExecId::new();

        let events = vec![
            race_started_event(),
            WorkflowEvent::MarkerRecorded {
                name: "race:1".to_string(),
                details: Value::from(2u64),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: branch_a,
                name: "fetch_a".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            // branch_a never resolves in this history (still in progress).
            WorkflowEvent::ActivityScheduled {
                activity_id: branch_b,
                name: "fetch_b".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: branch_b,
                output: Value::Null,
            },
            // Recorded winner (branch 0) disagrees with what this replay can
            // actually observe as resolved (only branch 1 has a terminal).
            WorkflowEvent::MarkerRecorded {
                name: "race_winner:1".to_string(),
                details: Value::from(0u64),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        let err = ctx
            .race()
            .activity_raw("fetch_a", Value::Null, "default")
            .activity_raw("fetch_b", Value::Null, "default")
            .run()
            .await
            .unwrap_err();
        assert!(matches!(err, HarvestError::NonDeterministic { .. }));
    }

    #[tokio::test]
    async fn race_activity_fingerprint_mismatch_diverges() {
        let events = vec![
            race_started_event(),
            WorkflowEvent::MarkerRecorded {
                name: "race:1".to_string(),
                details: Value::from(3u64), // recorded 3 branches
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        // Current code only supplies 2 branches -- a resize since the recorded run.
        let err = ctx
            .race()
            .activity_raw("fetch_a", Value::Null, "default")
            .activity_raw("fetch_b", Value::Null, "default")
            .run()
            .await
            .unwrap_err();
        assert!(matches!(err, HarvestError::NonDeterministic { .. }));
    }

    #[tokio::test]
    async fn race_activity_failed_winner_propagates_error_after_recording_marker()
    -> Result<(), HarvestError> {
        let winner_id = ActivityExecId::new();
        let loser_id = ActivityExecId::new();

        let events = vec![
            race_started_event(),
            WorkflowEvent::MarkerRecorded {
                name: "race:1".to_string(),
                details: Value::from(2u64),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: winner_id,
                name: "fetch_a".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityFailed {
                activity_id: winner_id,
                error: "upstream 500".to_string(),
                attempt: 1,
                error_type: "Error".to_string(),
                non_retryable: false,
                details: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: loser_id,
                name: "fetch_b".into(),
                input: Value::Null,
                queue: "default".into(),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        let err = ctx
            .race()
            .activity_raw("fetch_a", Value::Null, "default")
            .activity_raw("fetch_b", Value::Null, "default")
            .run()
            .await
            .unwrap_err();
        assert!(matches!(err, HarvestError::ActivityFailed { .. }));
        assert!(err.to_string().contains("upstream 500"));

        // The failed branch still legitimately "won" the race (first terminal
        // in history) -- the still-open sibling must be queued for cancellation.
        let commands = ctx.drain_commands();
        assert!(commands.iter().any(|c| matches!(
            c,
            WorkflowCommand::CancelRaceLosers { activities, .. } if activities == &vec![loser_id]
        )));
        Ok(())
    }

    #[tokio::test]
    async fn race_child_workflow_replays_winner() -> Result<(), HarvestError> {
        let loser_id = ExecutionId::new();
        let winner_id = ExecutionId::new();
        let winner_output = serde_json::json!({"report": "done"});

        let events = vec![
            race_started_event(),
            WorkflowEvent::MarkerRecorded {
                name: "race:1".to_string(),
                details: Value::from(2u64),
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id: loser_id,
                workflow_name: "provider_a".into(),
                input: Value::Null,
            },
            WorkflowEvent::ChildWorkflowStarted {
                child_id: winner_id,
                workflow_name: "provider_b".into(),
                input: Value::Null,
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id: winner_id,
                output: winner_output.clone(),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        let winner = ctx
            .race()
            .child_workflow_raw("provider_a", Value::Null)
            .child_workflow_raw("provider_b", Value::Null)
            .run()
            .await?;

        assert_eq!(winner.index, 1);
        assert_eq!(winner.value, winner_output);
        let commands = ctx.drain_commands();
        assert!(commands.iter().any(|c| matches!(
            c,
            WorkflowCommand::CancelRaceLosers { children, .. } if children == &vec![loser_id]
        )));
        Ok(())
    }

    #[tokio::test]
    async fn race_timer_signal_pair_signal_branch_wins() -> Result<(), HarvestError> {
        let timer_id = "__signal_timeout:1:approval".to_string();
        let events = vec![
            race_started_event(),
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new(&timer_id),
                duration_secs: 300,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"approved": true}),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        let winner = ctx
            .race()
            .signal("approval")
            .timer(std::time::Duration::from_secs(300))
            .run()
            .await?;

        assert_eq!(
            winner.index, 1,
            "signal branch has the fixed role-based index 1"
        );
        assert_eq!(winner.value, serde_json::json!({"approved": true}));

        let commands = ctx.drain_commands();
        assert!(
            commands.iter().any(|c| matches!(
                c,
                WorkflowCommand::CancelRaceLosers { activities, children, timers }
                    if activities.is_empty()
                        && children.is_empty()
                        && timers == &vec![TimerId::new(&timer_id)]
            )),
            "the now-stale still-armed timer must be queued for durable \
             deletion so it doesn't block retention or fire later: {commands:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn race_timer_signal_pair_timer_branch_wins() -> Result<(), HarvestError> {
        let timer_id = "__signal_timeout:1:approval".to_string();
        let events = vec![
            race_started_event(),
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new(&timer_id),
                duration_secs: 300,
            },
            WorkflowEvent::TimerFired {
                timer_id: TimerId::new(&timer_id),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        let winner = ctx
            .race()
            .signal("approval")
            .timer(std::time::Duration::from_secs(300))
            .run()
            .await?;

        assert_eq!(
            winner.index, 0,
            "timer branch has the fixed role-based index 0"
        );
        assert_eq!(winner.value, Value::Null);

        let commands = ctx.drain_commands();
        assert!(
            !commands
                .iter()
                .any(|c| matches!(c, WorkflowCommand::CancelRaceLosers { .. })),
            "the timer already fired to win -- there is nothing to durably \
             cancel, and the signal branch simply stays observable: {commands:?}"
        );
        Ok(())
    }

    /// Regression test (Codex review, PR #902): `RaceWinner.index` for the
    /// timer+signal shape must be a fixed role-based value, independent of
    /// whether `.timer()` or `.signal()` was called first in the builder
    /// chain -- otherwise a pure reorder between deploys (same `signal_name` /
    /// `duration_secs`) could silently flip the index an in-flight execution
    /// observes on replay, with no `NonDeterministic` error.
    #[tokio::test]
    async fn race_timer_signal_pair_index_is_independent_of_builder_call_order()
    -> Result<(), HarvestError> {
        let timer_id = "__signal_timeout:1:approval".to_string();
        let events = vec![
            race_started_event(),
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new(&timer_id),
                duration_secs: 300,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: serde_json::json!({"approved": true}),
            },
        ];

        let ctx_signal_first = WorkflowContext::for_replay(ExecutionId::new(), events.clone());
        let winner_signal_first = ctx_signal_first
            .race()
            .signal("approval")
            .timer(std::time::Duration::from_secs(300))
            .run()
            .await?;

        let ctx_timer_first = WorkflowContext::for_replay(ExecutionId::new(), events);
        let winner_timer_first = ctx_timer_first
            .race()
            .timer(std::time::Duration::from_secs(300))
            .signal("approval")
            .run()
            .await?;

        assert_eq!(winner_signal_first.index, winner_timer_first.index);
        assert_eq!(winner_signal_first.value, winner_timer_first.value);
        Ok(())
    }

    /// Property test (issue #600 success metric): across M synthetic histories
    /// with the winning branch at a different position each time, replaying
    /// the *same* history repeatedly always resolves to the *same* winner and
    /// the *same* commands (zero divergences), and `CancelRaceLosers` is
    /// always keyed to exactly the non-winning branches -- never the winner.
    #[tokio::test]
    async fn race_activity_winner_is_stable_across_randomized_completion_positions()
    -> Result<(), HarvestError> {
        const BRANCH_COUNT: usize = 5;
        const ITERATIONS: usize = 20;

        for iteration in 0..ITERATIONS {
            let winner_position = iteration % BRANCH_COUNT;
            let ids: Vec<ActivityExecId> =
                (0..BRANCH_COUNT).map(|_| ActivityExecId::new()).collect();

            let mut events = vec![
                race_started_event(),
                WorkflowEvent::MarkerRecorded {
                    name: "race:1".to_string(),
                    details: Value::from(BRANCH_COUNT as u64),
                },
            ];
            for (i, id) in ids.iter().enumerate() {
                events.push(WorkflowEvent::ActivityScheduled {
                    activity_id: *id,
                    name: format!("provider_{i}"),
                    input: Value::Null,
                    queue: "default".into(),
                });
            }
            // Only the designated winner has a terminal in this history; every
            // other branch is still legitimately in progress.
            events.push(WorkflowEvent::ActivityCompleted {
                activity_id: ids[winner_position],
                output: serde_json::json!({"winner_position": winner_position}),
            });

            let expected_losers: Vec<ActivityExecId> = ids
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != winner_position)
                .map(|(_, id)| *id)
                .collect();

            // Replay the identical history three times -- every run must agree.
            for _ in 0..3 {
                let ctx = WorkflowContext::for_replay(ExecutionId::new(), events.clone());
                let mut builder = ctx.race();
                for i in 0..BRANCH_COUNT {
                    builder =
                        builder.activity_raw(&format!("provider_{i}"), Value::Null, "default");
                }
                let winner = builder.run().await?;

                assert_eq!(
                    winner.index, winner_position,
                    "iteration {iteration}: winner must match the branch with the recorded terminal"
                );

                let commands = ctx.drain_commands();
                let cancelled: Vec<ActivityExecId> = commands
                    .iter()
                    .find_map(|c| match c {
                        WorkflowCommand::CancelRaceLosers { activities, .. } => {
                            Some(activities.clone())
                        }
                        _ => None,
                    })
                    .unwrap_or_default();
                let mut cancelled_sorted = cancelled;
                cancelled_sorted.sort_by_key(ActivityExecId::as_uuid);
                let mut expected_sorted = expected_losers.clone();
                expected_sorted.sort_by_key(ActivityExecId::as_uuid);
                assert_eq!(
                    cancelled_sorted, expected_sorted,
                    "iteration {iteration}: exactly the non-winning branches must be queued for cancellation"
                );
                assert!(
                    !cancelled_sorted.contains(&ids[winner_position]),
                    "iteration {iteration}: the winner must never be cancelled"
                );
            }
        }
        Ok(())
    }

    // ── Worker session marker resolution tests (issue #606) ──────────────────

    #[test]
    fn next_session_seq_increments_monotonically() {
        let ctx = WorkflowContext::new_test();
        assert_eq!(ctx.next_session_seq(), 1);
        assert_eq!(ctx.next_session_seq(), 2);
        assert_eq!(ctx.next_session_seq(), 3);
    }

    #[test]
    fn resolve_session_id_on_live_execution_generates_and_records_marker() {
        let ctx = WorkflowContext::new_test();
        let session_id = ctx.resolve_session_id(1).expect("live resolve succeeds");

        let commands = ctx.drain_commands();
        let recorded = commands.iter().find_map(|c| match c {
            WorkflowCommand::RecordMarker { name, details } if name == "session:1" => {
                Some(details.clone())
            }
            _ => None,
        });
        let recorded = recorded.expect("a session:1 marker must be recorded on live dispatch");
        assert_eq!(recorded, Value::from(session_id.to_string()));
    }

    #[test]
    fn resolve_session_id_generates_distinct_ids_per_seq() {
        let ctx = WorkflowContext::new_test();
        let a = ctx.resolve_session_id(1).unwrap();
        let b = ctx.resolve_session_id(2).unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn resolve_session_id_on_replay_recovers_recorded_id() {
        let session_uuid = uuid::Uuid::new_v4();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::MarkerRecorded {
                name: "session:1".into(),
                details: Value::from(session_uuid.to_string()),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let resolved = ctx.resolve_session_id(1).expect("replay resolve succeeds");
        assert_eq!(resolved, SessionId::from_uuid(session_uuid));

        // Replay must not re-push a RecordMarker command — the marker was
        // already recorded.
        let commands = ctx.drain_commands();
        assert!(
            !commands.iter().any(
                |c| matches!(c, WorkflowCommand::RecordMarker { name, .. } if name == "session:1")
            ),
            "replay must not re-record an already-recorded session marker"
        );
    }

    #[test]
    fn resolve_session_id_diverges_when_history_disagrees() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::TimerFired {
                timer_id: TimerId::new("unrelated"),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let result = ctx.resolve_session_id(1);
        assert!(matches!(result, Err(HarvestError::NonDeterministic { .. })));
    }

    #[test]
    fn resolve_session_id_two_sessions_get_distinct_markers_on_replay() {
        let uuid1 = uuid::Uuid::new_v4();
        let uuid2 = uuid::Uuid::new_v4();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::MarkerRecorded {
                name: "session:1".into(),
                details: Value::from(uuid1.to_string()),
            },
            WorkflowEvent::MarkerRecorded {
                name: "session:2".into(),
                details: Value::from(uuid2.to_string()),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        assert_eq!(
            ctx.resolve_session_id(1).unwrap(),
            SessionId::from_uuid(uuid1)
        );
        assert_eq!(
            ctx.resolve_session_id(2).unwrap(),
            SessionId::from_uuid(uuid2)
        );
    }

    // ── ctx.create_session() / Session tests (issue #606) ────────────────────

    #[test]
    fn session_options_default_queue_and_timeout() {
        let opts = SessionOptions::new("gpu-workers");
        assert_eq!(opts.queue, "gpu-workers");
        assert_eq!(
            opts.acquisition_timeout,
            DEFAULT_SESSION_ACQUISITION_TIMEOUT
        );
    }

    #[test]
    fn session_options_with_acquisition_timeout_overrides_default() {
        let opts = SessionOptions::new("default")
            .with_acquisition_timeout(std::time::Duration::from_secs(5));
        assert_eq!(opts.acquisition_timeout, std::time::Duration::from_secs(5));
    }

    #[test]
    fn session_options_impl_default_uses_default_queue() {
        let opts = SessionOptions::default();
        assert_eq!(opts.queue, "default");
    }

    #[tokio::test]
    async fn create_session_live_emits_marker_then_acquire_activity_and_suspends() {
        let ctx = WorkflowContext::new_test();
        let opts = SessionOptions::new("gpu-workers")
            .with_acquisition_timeout(std::time::Duration::from_secs(45));

        let fut = ctx.create_session(opts);
        tokio::pin!(fut);
        tokio::select! {
            _ = &mut fut => panic!("create_session should suspend, not complete, with no acquire result in history"),
            () = tokio::time::sleep(std::time::Duration::from_millis(20)) => {}
        }

        let commands = ctx.drain_commands();
        assert_eq!(
            commands.len(),
            2,
            "expected exactly [RecordMarker, ScheduleActivity]"
        );
        match &commands[0] {
            WorkflowCommand::RecordMarker { name, .. } => assert_eq!(name, "session:1"),
            other => panic!("expected RecordMarker first, got {other:?}"),
        }
        match &commands[1] {
            WorkflowCommand::ScheduleActivity {
                name,
                queue,
                session_id,
                session_worker_id,
                schedule_to_start_override,
                ..
            } => {
                assert_eq!(name, SESSION_ACQUIRE_ACTIVITY_NAME);
                assert_eq!(queue, "gpu-workers");
                // The acquire task itself is never hard-pinned -- it has no
                // resolved host yet; that's the whole point of dispatching it.
                assert_eq!(*session_id, None);
                assert_eq!(*session_worker_id, None);
                assert_eq!(
                    *schedule_to_start_override,
                    Some(std::time::Duration::from_secs(45))
                );
            }
            other => panic!("expected ScheduleActivity second, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn create_session_replay_recovers_host_worker_id_from_acquire_output() {
        let session_uuid = uuid::Uuid::new_v4();
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::MarkerRecorded {
                name: "session:1".into(),
                details: Value::from(session_uuid.to_string()),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: SESSION_ACQUIRE_ACTIVITY_NAME.to_string(),
                input: Value::Null,
                queue: "gpu-workers".to_string(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!("worker-42"),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let session = ctx
            .create_session(SessionOptions::new("gpu-workers"))
            .await
            .expect("replay resolves the session from recorded history");

        assert_eq!(session.id(), SessionId::from_uuid(session_uuid));
        assert_eq!(session.host_worker_id(), "worker-42");
    }

    #[tokio::test]
    async fn create_session_maps_schedule_to_start_timeout_to_session_acquire_timeout() {
        let session_uuid = uuid::Uuid::new_v4();
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::MarkerRecorded {
                name: "session:1".into(),
                details: Value::from(session_uuid.to_string()),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: SESSION_ACQUIRE_ACTIVITY_NAME.to_string(),
                input: Value::Null,
                queue: "gpu-workers".to_string(),
            },
            WorkflowEvent::ActivityTimedOut {
                activity_id,
                timeout_type: crate::error::TimeoutType::ScheduleToStart,
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let Err(err) = ctx.create_session(SessionOptions::new("gpu-workers")).await else {
            panic!("expected create_session to fail with SessionAcquireTimeout");
        };
        match err {
            HarvestError::SessionAcquireTimeout {
                session_id, queue, ..
            } => {
                assert_eq!(session_id, SessionId::from_uuid(session_uuid));
                assert_eq!(queue, "gpu-workers");
            }
            other => panic!("expected SessionAcquireTimeout, got {other}"),
        }
    }

    /// Test double counting `record_session_acquisition` calls by outcome.
    #[derive(Default)]
    struct SessionAcquisitionCounter {
        acquired: std::sync::atomic::AtomicUsize,
        timed_out: std::sync::atomic::AtomicUsize,
        broken: std::sync::atomic::AtomicUsize,
    }

    impl crate::telemetry::MetricsRecorder for SessionAcquisitionCounter {
        fn record_session_acquisition(
            &self,
            _queue: &str,
            outcome: crate::telemetry::SessionAcquisitionOutcome,
        ) {
            use crate::telemetry::SessionAcquisitionOutcome as O;
            let counter = match outcome {
                O::Acquired => &self.acquired,
                O::TimedOut => &self.timed_out,
                O::Broken => &self.broken,
            };
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// The `TimedOut` metric fires when the `ActivityTimedOut` event is the
    /// tail of recorded history (the cycle that first discovers it, where
    /// `is_replaying()` becomes `false` right after the match) -- mirroring
    /// the exactly-once-on-the-live-frontier contract `ctx.metrics()`
    /// (issue #532) already documents.
    #[tokio::test]
    async fn create_session_emits_timed_out_metric_on_first_discovery() {
        let session_uuid = uuid::Uuid::new_v4();
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::MarkerRecorded {
                name: "session:1".into(),
                details: Value::from(session_uuid.to_string()),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: SESSION_ACQUIRE_ACTIVITY_NAME.to_string(),
                input: Value::Null,
                queue: "gpu-workers".to_string(),
            },
            WorkflowEvent::ActivityTimedOut {
                activity_id,
                timeout_type: crate::error::TimeoutType::ScheduleToStart,
            },
        ];
        let recorder = std::sync::Arc::new(SessionAcquisitionCounter::default());
        let ctx =
            WorkflowContext::for_replay(ExecutionId::new(), events).with_metrics(recorder.clone());
        let result = ctx.create_session(SessionOptions::new("gpu-workers")).await;
        assert!(matches!(
            result,
            Err(HarvestError::SessionAcquireTimeout { .. })
        ));
        assert_eq!(
            recorder.timed_out.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(
            recorder.acquired.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert_eq!(recorder.broken.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    /// A later replay of a history that already carries a further event past
    /// the timeout (i.e. the cursor still has more to consume at the match
    /// point) must not re-increment the metric -- `is_replaying()` is `true`
    /// at that point, so the guard suppresses it.
    #[tokio::test]
    async fn create_session_does_not_reemit_timed_out_metric_on_later_replay() {
        let session_uuid = uuid::Uuid::new_v4();
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::MarkerRecorded {
                name: "session:1".into(),
                details: Value::from(session_uuid.to_string()),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: SESSION_ACQUIRE_ACTIVITY_NAME.to_string(),
                input: Value::Null,
                queue: "gpu-workers".to_string(),
            },
            WorkflowEvent::ActivityTimedOut {
                activity_id,
                timeout_type: crate::error::TimeoutType::ScheduleToStart,
            },
            // A further recorded event past the timeout -- proves this
            // history has already progressed beyond the point where the
            // timeout was first discovered.
            WorkflowEvent::WorkflowFailed {
                error: "session acquisition timed out".to_string(),
            },
        ];
        let recorder = std::sync::Arc::new(SessionAcquisitionCounter::default());
        let ctx =
            WorkflowContext::for_replay(ExecutionId::new(), events).with_metrics(recorder.clone());
        let _ = ctx.create_session(SessionOptions::new("gpu-workers")).await;
        assert_eq!(
            recorder.timed_out.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "a replay that still has further recorded history ahead must not \
             re-increment the metric"
        );
    }

    #[tokio::test]
    async fn session_execute_activity_raw_hard_pins_to_host_worker() {
        let host_activity_id = ActivityExecId::new();
        let session_uuid = uuid::Uuid::new_v4();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::MarkerRecorded {
                name: "session:1".into(),
                details: Value::from(session_uuid.to_string()),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: host_activity_id,
                name: SESSION_ACQUIRE_ACTIVITY_NAME.to_string(),
                input: Value::Null,
                queue: "gpu-workers".to_string(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: host_activity_id,
                output: serde_json::json!("worker-42"),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let session = ctx
            .create_session(SessionOptions::new("gpu-workers"))
            .await
            .unwrap();

        let fut = session.execute_activity_raw("transcode_chunk", Value::Null, "gpu-workers");
        tokio::pin!(fut);
        tokio::select! {
            _ = &mut fut => panic!("member activity should suspend, not complete"),
            () = tokio::time::sleep(std::time::Duration::from_millis(20)) => {}
        }

        let commands = ctx.drain_commands();
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            WorkflowCommand::ScheduleActivity {
                name,
                session_id,
                session_worker_id,
                ..
            } => {
                assert_eq!(name, "transcode_chunk");
                assert_eq!(*session_id, Some(SessionId::from_uuid(session_uuid)));
                assert_eq!(session_worker_id.as_deref(), Some("worker-42"));
            }
            other => panic!("expected ScheduleActivity, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn session_complete_dispatches_release_activity_hard_pinned_to_host() {
        let host_activity_id = ActivityExecId::new();
        let session_uuid = uuid::Uuid::new_v4();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::MarkerRecorded {
                name: "session:1".into(),
                details: Value::from(session_uuid.to_string()),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: host_activity_id,
                name: SESSION_ACQUIRE_ACTIVITY_NAME.to_string(),
                input: Value::Null,
                queue: "gpu-workers".to_string(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: host_activity_id,
                output: serde_json::json!("worker-42"),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let session = ctx
            .create_session(SessionOptions::new("gpu-workers"))
            .await
            .unwrap();

        let fut = session.complete();
        tokio::pin!(fut);
        tokio::select! {
            _ = &mut fut => panic!("complete() should suspend, not complete"),
            () = tokio::time::sleep(std::time::Duration::from_millis(20)) => {}
        }

        let commands = ctx.drain_commands();
        assert_eq!(commands.len(), 1);
        match &commands[0] {
            WorkflowCommand::ScheduleActivity {
                name,
                session_id,
                session_worker_id,
                ..
            } => {
                assert_eq!(name, SESSION_RELEASE_ACTIVITY_NAME);
                assert_eq!(*session_id, Some(SessionId::from_uuid(session_uuid)));
                assert_eq!(session_worker_id.as_deref(), Some("worker-42"));
            }
            other => panic!("expected ScheduleActivity, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn create_session_checks_cancellation_before_dispatch() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::WorkflowCancelled {
                reason: "operator cancel".into(),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
        let result = ctx.create_session(SessionOptions::new("default")).await;
        assert!(matches!(result, Err(HarvestError::Cancelled(_))));
        // Cancellation must be checked before any command is pushed.
        assert!(ctx.drain_commands().is_empty());
    }
}
