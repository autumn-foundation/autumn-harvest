//! Replay test harness for verifying workflow code changes against recorded histories.
//!
//! # Overview
//!
//! `WorkflowReplayer` lets you assert that a `#[workflow]` function is
//! replay-safe before deploying a code change.  Build one, register your
//! workflow handlers, then call any of the three replay methods:
//!
//! - [`WorkflowReplayer::replay_from_events`] — hand-authored or property-test fixtures
//! - [`WorkflowReplayer::replay_from_json`] — JSON snapshots exportable from any env
//! - [`WorkflowReplayer::replay_from_db`] — live pull from `harvest_events` (requires
//!   the `db` feature)
//!
//! Each call returns a [`ReplayReport`] with a structured [`ReplayStatus`] that
//! implements both `Debug` and `Display` so `panic!("{report}")` gives a useful
//! CI message.
//!
//! # CI pattern
//!
//! ```rust,no_run
//! # use autumn_harvest::testing::{WorkflowReplayer, ReplayStatus};
//! # use autumn_harvest::event::WorkflowEvent;
//! # use autumn_harvest::context::WorkflowContext;
//! # use serde_json::Value;
//! # use std::pin::Pin;
//! # fn my_workflow<'a>(ctx: &'a WorkflowContext, input: Value)
//! #   -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>
//! # { Box::pin(async move { Ok(input) }) }
//! # async fn example() {
//! # let history: Vec<WorkflowEvent> = vec![];
//! let report = WorkflowReplayer::new()
//!     .register_fn("my_workflow", my_workflow)
//!     .replay_from_events(history)
//!     .await;
//!
//! assert!(
//!     matches!(report.status, ReplayStatus::ReplaySucceeded),
//!     "replay regression detected:\n{report}"
//! );
//! # }
//! ```

use std::any::TypeId;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::context::{SharedState, WorkflowCommand, empty_shared_state};
use crate::event::WorkflowEvent;
use crate::executor::{
    WorkflowExecuteSpanMeta, WorkflowOutcome, run_workflow_canary, run_workflow_strict,
    run_workflow_strict_advancing_clock, run_workflow_with_state_advancing_clock,
};
use crate::info::{WorkflowHandlerFn, WorkflowInfo};
use crate::types::{ActivityExecId, ExecutionId, ParentClosePolicy, WorkerId};

// ---------------------------------------------------------------------------
// NonDeterminismKind
// ---------------------------------------------------------------------------

/// The category of non-determinism detected during replay.
///
/// Each variant maps to a distinct command/event kind so callers can
/// distinguish (and report on) activity vs timer vs signal divergences.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum NonDeterminismKind {
    /// An activity was scheduled with a different name than what history recorded.
    ActivityScheduleMismatch,
    /// A local activity was scheduled with a different name than history recorded.
    LocalActivityScheduleMismatch,
    /// A timer was started but history recorded a different event at that position.
    TimerMismatch,
    /// A signal wait was issued but history recorded a non-signal at that position.
    SignalMismatch,
    /// A child workflow was started but name or input differed from history.
    ChildWorkflowMismatch,
    /// A side-effect ID did not match the recorded marker.
    SideEffectMismatch,
    /// A deterministic built-in primitive (`ctx.system_now()`, `ctx.new_uuid()`,
    /// `ctx.random_*()`) drifted from the recorded `SideEffectRecorded` history —
    /// a captured value was reordered, renamed, inserted, or removed across a
    /// code change (issue #384).
    SideEffectDrift,
    /// An external activity name did not match the recorded event.
    ExternalActivityMismatch,
    /// A `signal_external_workflow` call did not match the recorded event.
    ExternalSignalMismatch,
    /// An `await_external_workflow` call did not match the recorded event
    /// (issue #757) — the awaited target differs from what history recorded.
    ExternalAwaitMismatch,
    /// A continue-as-new input differed from history.
    ContinueAsNewMismatch,
    /// The workflow returned before consuming all recorded history events.
    EarlyCompletion,
    /// A version gate's `change_id` was renamed without migrating the history
    /// so the old `version:…` marker was left unconsumed and was encountered
    /// by the next command at that cursor position.
    VersionMarkerMismatch,
    /// A `patch:{id}` marker was left unconsumed — e.g. the `patched()` call
    /// was removed or renamed before all marker-bearing executions drained
    /// (issue #687's deploy-3 step taken too early) — and was encountered by
    /// the next command at that cursor position.
    PatchMarkerMismatch,
    /// A `TimerCancelled` event was left unconsumed — e.g. a
    /// [`crate::context::WorkflowContext::cancel_timer`] / `TimerHandle::cancel`
    /// / `TimerHandle::reset` call was removed before all cancel-bearing
    /// executions drained (issue #768) — and was encountered by the next
    /// command at that cursor position.
    TimerCancelMismatch,
    /// A durable-mutex acquire (issue #691) diverged from history — the workflow
    /// issued a different `ctx.mutex(key).acquire()` (a different key, or an
    /// acquire where history recorded another event) than the recorded
    /// `MutexGranted` at that cursor position.
    MutexGrantMismatch,
    /// The divergence could not be classified into a known category.
    Unknown,
}

impl std::fmt::Display for NonDeterminismKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ActivityScheduleMismatch => write!(f, "ActivityScheduleMismatch"),
            Self::LocalActivityScheduleMismatch => write!(f, "LocalActivityScheduleMismatch"),
            Self::TimerMismatch => write!(f, "TimerMismatch"),
            Self::SignalMismatch => write!(f, "SignalMismatch"),
            Self::ChildWorkflowMismatch => write!(f, "ChildWorkflowMismatch"),
            Self::SideEffectMismatch => write!(f, "SideEffectMismatch"),
            Self::SideEffectDrift => write!(f, "SideEffectDrift"),
            Self::ExternalActivityMismatch => write!(f, "ExternalActivityMismatch"),
            Self::ExternalSignalMismatch => write!(f, "ExternalSignalMismatch"),
            Self::ExternalAwaitMismatch => write!(f, "ExternalAwaitMismatch"),
            Self::ContinueAsNewMismatch => write!(f, "ContinueAsNewMismatch"),
            Self::EarlyCompletion => write!(f, "EarlyCompletion"),
            Self::VersionMarkerMismatch => write!(f, "VersionMarkerMismatch"),
            Self::PatchMarkerMismatch => write!(f, "PatchMarkerMismatch"),
            Self::TimerCancelMismatch => write!(f, "TimerCancelMismatch"),
            Self::MutexGrantMismatch => write!(f, "MutexGrantMismatch"),
            Self::Unknown => write!(f, "Unknown"),
        }
    }
}

// ---------------------------------------------------------------------------
// ReplayStatus
// ---------------------------------------------------------------------------

/// The result classification of a single replay run.
#[derive(Debug, Clone, serde::Serialize)]
pub enum ReplayStatus {
    /// The workflow replayed the entire recorded history without divergence.
    ReplaySucceeded,
    /// The workflow issued a command that diverged from the recorded history.
    NonDeterminismDetected {
        /// The category of mismatch detected.
        kind: NonDeterminismKind,
        /// What the history expected at this position.
        expected: String,
        /// What the workflow code actually requested.
        actual: String,
        /// Approximate index into the event list where the divergence occurred.
        event_index: usize,
    },
    /// The workflow returned an error (not caused by non-determinism).
    WorkflowFailed {
        /// The error string returned by the workflow function.
        error: String,
        /// Index of the last event processed before the failure.
        event_index: usize,
    },
}

// ---------------------------------------------------------------------------
// ReplayReport
// ---------------------------------------------------------------------------

/// Structured output from a single replay run.
///
/// Implements `Display` so `panic!("{report}")` produces a useful CI failure
/// message without any additional formatting work.
#[derive(Debug, Clone)]
pub struct ReplayReport {
    /// The execution ID used for this replay.
    pub execution_id: ExecutionId,
    /// How many events from the input history were processed.
    pub events_replayed: usize,
    /// Whether replay succeeded, detected non-determinism, or failed.
    pub status: ReplayStatus,
    /// Human-readable one-line summary of the mismatch (set for non-determinism).
    pub mismatched_command_summary: Option<String>,
}

impl std::fmt::Display for ReplayReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ReplayReport(exec={}, events_replayed={}, status=",
            self.execution_id, self.events_replayed
        )?;
        match &self.status {
            ReplayStatus::ReplaySucceeded => write!(f, "ReplaySucceeded)")?,
            ReplayStatus::NonDeterminismDetected {
                kind,
                expected,
                actual,
                event_index,
            } => {
                write!(
                    f,
                    "NonDeterminismDetected(kind={kind}, event_index={event_index}, \
                     expected=\"{expected}\", actual=\"{actual}\"))"
                )?;
            }
            ReplayStatus::WorkflowFailed { error, event_index } => {
                write!(
                    f,
                    "WorkflowFailed(event_index={event_index}, error=\"{error}\"))"
                )?;
            }
        }
        if let Some(summary) = &self.mismatched_command_summary {
            write!(f, " [{summary}]")?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// HistorySnapshot  (JSON wire format)
// ---------------------------------------------------------------------------

/// A portable snapshot of a workflow's event history for use with
/// [`WorkflowReplayer::replay_from_json`].
///
/// Serialise a captured history to JSON and check it into your repo as a
/// fixture — then replay it in CI against every code change.
///
/// ```rust
/// # use autumn_harvest::testing::HistorySnapshot;
/// # use autumn_harvest::types::ExecutionId;
/// # use autumn_harvest::event::WorkflowEvent;
/// # use chrono::Utc;
/// # use serde_json::Value;
/// let snapshot = HistorySnapshot {
///     workflow_name: "onboarding".to_string(),
///     execution_id: ExecutionId::new(),
///     events: vec![
///         WorkflowEvent::WorkflowStarted {
///             input: Value::Null,
///             timestamp: Utc::now(),
///             last_completion_result: None,
///             last_error: None,
///             scheduled_time: None,
///         },
///     ],
///     context_headers: None,
///     execution_timeout: None,
///     deadline_at: None,
///     parent_execution_id: None,
///     workflow_id: None,
///     queue_name: None,
/// };
/// let json = serde_json::to_string(&snapshot).unwrap();
/// // Store `json` as a fixture file.
/// ```
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HistorySnapshot {
    /// The registered workflow name that should handle this history.
    pub workflow_name: String,
    /// The execution ID of the captured run.
    pub execution_id: ExecutionId,
    /// The full ordered event log, as returned by `load_history`.
    pub events: Vec<WorkflowEvent>,
    /// Per-execution context headers attached at workflow start.
    ///
    /// `None` means the field was absent in the JSON (legacy snapshot or not
    /// set by the caller) — `replay_from_snapshot` falls back to any headers
    /// configured on the [`WorkflowReplayer`] itself.  `Some(map)` (including
    /// `Some(HashMap::new())`) is used verbatim, so an explicitly-empty header
    /// map is not overridden by the replayer's ambient headers.
    #[serde(default)]
    pub context_headers: Option<HashMap<String, String>>,
    /// The execution's `execution_timeout` budget (issue #772). When `Some`,
    /// [`replay_from_snapshot`](WorkflowReplayer::replay_from_snapshot) threads
    /// it into the replayed `WorkflowContext` (preferring it over the replayer's
    /// global [`with_execution_timeout`](WorkflowReplayer::with_execution_timeout)),
    /// so a deadline-aware history that recorded a `SideEffectRecorded{Now}`
    /// deadline probe replays cleanly instead of false-reporting non-determinism.
    /// Serialised as integer milliseconds; `None` (the field absent — a legacy
    /// snapshot) falls back to the replayer's global timeout. A full history
    /// export (`history_export::HistoryExportDocument`) serialises this field at
    /// the same top-level name, so an exported history round-trips into this
    /// snapshot verbatim.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::history_export::opt_duration_millis"
    )]
    pub execution_timeout: Option<chrono::Duration>,
    /// The execution's live (pause/resume/redrive-shifted) absolute `deadline_at`
    /// (issue #772). When `Some`, the internal continue-as-new budget check reads
    /// this instead of the nominal `start + execution_timeout`. `None` (absent /
    /// legacy snapshot) falls back to no live deadline (the nominal is used).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_at: Option<chrono::DateTime<chrono::Utc>>,
    /// The spawning parent's execution id (issue #698). `parent_execution_id` is
    /// sourced from the `harvest_workflow_executions.parent_id` column and lives
    /// in no `WorkflowEvent`, so a captured fixture cannot recover it from the
    /// event log alone. When `Some`, both replay paths thread it into the
    /// `WorkflowContext` (preferring it over the replayer's global
    /// [`with_parent_execution_id`](WorkflowReplayer::with_parent_execution_id)),
    /// so a child that branches command-affecting control flow on
    /// `ctx.info().parent_execution_id` replays deterministically. `None` (the
    /// field absent — a legacy snapshot, or a top-level run) falls back to the
    /// replayer's global parent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_execution_id: Option<ExecutionId>,
    /// The business-level workflow identifier (issue #698), sourced from the
    /// `harvest_workflow_executions.workflow_id` column. Like
    /// [`parent_execution_id`](Self::parent_execution_id), it lives in no
    /// `WorkflowEvent`, so a captured fixture cannot recover it from the event log
    /// alone. When `Some`, both replay paths thread it into the `WorkflowContext`
    /// (preferring it over the replayer's global
    /// [`with_workflow_id`](WorkflowReplayer::with_workflow_id)) so a workflow that
    /// branches command-affecting control flow on `ctx.info().workflow_id` — or
    /// embeds it in an activity input — replays deterministically. Serialised at
    /// the same top-level name as
    /// [`history_export::HistoryExportDocument::workflow_id`], so an exported
    /// history round-trips into this snapshot verbatim. `None` (the field absent —
    /// a legacy snapshot, or a run without an explicit id) falls back to the
    /// replayer's global. The workflow **type** name rides
    /// [`workflow_name`](Self::workflow_name) (the handler-lookup key), which both
    /// replay paths already apply to the context, so `ctx.info().workflow_type`
    /// needs no separate snapshot field.
    ///
    /// [`history_export::HistoryExportDocument::workflow_id`]: crate::history_export::HistoryExportDocument::workflow_id
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
    /// The execution's task queue (issue #798), sourced from the
    /// `harvest_workflow_executions.queue_name` column.
    ///
    /// The live worker sets `ctx.queue_name()` from the claimed task row, but the
    /// value lives in no `WorkflowEvent`, so a captured fixture cannot recover it
    /// from the event log alone — the same family as
    /// [`context_headers`](Self::context_headers) and
    /// [`workflow_id`](Self::workflow_id). When `Some`, both replay paths thread
    /// it into the `WorkflowContext` (preferring it over the replayer's global
    /// [`with_queue_name`](WorkflowReplayer::with_queue_name)) so a workflow that
    /// branches command-affecting control flow on `ctx.queue_name()` — or embeds
    /// it in an activity input — replays deterministically instead of running
    /// under `""` and false-reporting drift. Serialised at the same top-level name
    /// as [`history_export::HistoryExportDocument::queue_name`], so an exported
    /// history round-trips into this snapshot verbatim. `None` (the field absent —
    /// a legacy snapshot) falls back to the replayer's global.
    ///
    /// [`history_export::HistoryExportDocument::queue_name`]: crate::history_export::HistoryExportDocument::queue_name
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_name: Option<String>,
}

// ---------------------------------------------------------------------------
// WorkflowReplayer
// ---------------------------------------------------------------------------

/// Read-only replay harness for verifying workflow determinism.
///
/// Register one or more workflow handlers, then call a replay method to
/// run each handler against a recorded event history and classify the
/// outcome.
///
/// The replayer **never** executes activities, writes to the database, or
/// sends signals. It runs the workflow function in pure replay mode — all
/// side-effect commands are suppressed — and only compares the commands the
/// code issues against what the recorded history expects.
pub struct WorkflowReplayer {
    handlers: HashMap<String, WorkflowHandlerFn>,
    state: SharedState,
    context_headers: HashMap<String, String>,
    /// Optional offloader for inflating offloaded payloads in the fixture
    /// before replay (issue #524).
    payload_offloader: Option<std::sync::Arc<crate::payload_store::PayloadOffloader>>,
    /// When `true`, replay uses the advancing virtual clock (issue #526) so
    /// that `ctx.now()` tracks elapsed timer duration.  Required for
    /// `TestRunOutcome::replay_check` on time-branching workflows.
    use_advancing_clock: bool,
    /// Metrics recorder injected into the replayed `WorkflowContext`.
    /// Defaults to `NoOpMetrics`; inject a counting recorder for replay-safety tests.
    metrics: std::sync::Arc<dyn crate::telemetry::MetricsRecorder>,
    /// Effective `execution_timeout` budget threaded into the replayed
    /// `WorkflowContext` (issue #772) so replays can exercise deadline-aware
    /// `continue_as_new`. `None` (the default) matches a workflow with no
    /// execution timeout.
    execution_timeout: Option<chrono::Duration>,
    /// Effective spawning-parent execution id threaded into the replayed
    /// `WorkflowContext` (issue #698). `parent_execution_id` lives in no
    /// `WorkflowEvent`, so a pure-history replay cannot recover it from the fixture
    /// alone; set it with
    /// [`with_parent_execution_id`](Self::with_parent_execution_id) so a child
    /// workflow that branches command-affecting control flow on
    /// `ctx.info().parent_execution_id` replays deterministically in a CI test.
    /// A [`HistorySnapshot`] that carries its own `parent_execution_id` overrides
    /// this global. `None` (the default) models a top-level run.
    parent_execution_id: Option<ExecutionId>,
    /// Effective business-level `workflow_id` threaded into the replayed
    /// `WorkflowContext` (issue #698). `workflow_id` lives in no `WorkflowEvent`, so
    /// a raw-events fixture cannot recover it; set it with
    /// [`with_workflow_id`](Self::with_workflow_id) so a workflow that branches
    /// command-affecting control flow on `ctx.info().workflow_id` — or embeds it in
    /// an activity input — replays deterministically. A [`HistorySnapshot`] that
    /// carries its own `workflow_id` overrides this global. `None` (the default)
    /// models a run without an explicit id.
    workflow_id: Option<String>,
    /// Effective task queue threaded into the replayed `WorkflowContext`
    /// (issue #798). `queue_name` is supplied by the live worker from the claimed
    /// task row and lives in no `WorkflowEvent`, so a fixture that omits it cannot
    /// recover it; set it with [`with_queue_name`](Self::with_queue_name) so a
    /// workflow that branches command-affecting control flow on `ctx.queue_name()`
    /// — or embeds it in an activity input — replays deterministically. A
    /// [`HistorySnapshot`] that carries its own `queue_name` overrides this
    /// global. `None` (the default) preserves the empty-string default.
    queue_name: Option<String>,
    /// The **candidate** build id threaded into the replayed `WorkflowContext`
    /// (issue #798), reported by [`WorkflowContext::build_id`].
    ///
    /// Unlike every sibling field here, this is deliberately **not** sourced
    /// from the fixture. The live worker supplies its *own configured*
    /// [`WorkerConfig::build_id`](crate::builder::WorkerConfig::build_id)
    /// through span metadata — never the execution's recorded
    /// `assigned_build_id` — so the value a replay gate needs is the build that
    /// is *about to be promoted*, applied uniformly to every fixture. Recording
    /// the exporting worker's build into the snapshot and replaying under it
    /// would take the **historical** branch, so candidate-only code such as
    /// `if ctx.build_id() == Some("v2")` would replay clean and the gate would
    /// report false GREEN on exactly the divergence it exists to catch.
    ///
    /// Consequently there is no `HistorySnapshot::build_id` to override this:
    /// set it with [`with_build_id`](Self::with_build_id). `None` (the default)
    /// reports no build id, matching a worker with none configured.
    build_id: Option<String>,
    /// Effective `execution_id` threaded into the replayed `WorkflowContext` on
    /// the raw [`replay_from_events`](Self::replay_from_events) path (issue #698).
    /// `execution_id` is documented as replay-safe (`ctx.info().execution_id`),
    /// but a raw-events fixture carries no [`HistorySnapshot`] to source it from,
    /// so `replay_from_events` mints a fresh id unless one is set here with
    /// [`with_execution_id`](Self::with_execution_id). Without it a workflow that
    /// recorded `ctx.info().execution_id` in a command-affecting value (e.g. an
    /// activity input) false-reports non-determinism (the random replay id vs the
    /// recorded original run id). The snapshot/DB/canary paths already carry the
    /// real id and ignore this global. `None` (the default) mints a fresh id, so
    /// existing raw-events fixtures are unaffected.
    execution_id: Option<ExecutionId>,
    /// History policy threaded into the replayed `WorkflowContext` (issue #614).
    /// Defaults to [`WorkflowHistoryPolicy::default`]; the replay-diagnosis
    /// endpoint sets it to the runtime registry's own policy
    /// (`registry.history_policy()`) so a workflow that branches command-affecting
    /// control flow on [`WorkflowContext::should_continue_as_new`] — which reads
    /// the policy's `continue_as_new_threshold` /
    /// `continue_as_new_deadline_fraction` — replays byte-faithfully to the live
    /// worker instead of surfacing false non-determinism.
    history_policy: crate::context::WorkflowHistoryPolicy,
    /// The **candidate** worker's payload limits (issue #798, Codex round 20).
    ///
    /// Same category as `build_id` and `history_policy` above, and deliberately
    /// **not** sourced from the fixture: payload caps and the offload threshold
    /// live in no `WorkflowEvent`, and the live worker supplies its own from
    /// `BuiltHarvest`. Left at the library defaults, a replay gate answers the
    /// wrong question — a candidate that *lowers* a cap replays clean here and
    /// then rejects the sampled in-flight runs with `PayloadTooLarge` once
    /// promoted (false GREEN), while a candidate that configures an offload
    /// threshold reports drift that will never happen (false RED).
    ///
    /// See [`ReplayPayloadLimits`](crate::executor::ReplayPayloadLimits) for why
    /// only a frontier dispatch consults these. Defaults to the library values,
    /// so a caller that does not set them is unaffected.
    payload_limits: crate::executor::ReplayPayloadLimits,
    /// The candidate's declarative `#[query]` / `#[update]` handlers (issue #798).
    ///
    /// Registered onto the replay context before the workflow body runs, exactly
    /// as the live worker does. Owned rather than borrowed so a builder can be
    /// constructed and moved freely; the executor borrows from these at the call
    /// site. Empty by default, which is byte-for-byte the pre-fix behavior.
    ///
    /// See [`ReplayDeclarativeHandlers`](crate::executor::ReplayDeclarativeHandlers)
    /// for why a replay that omits them answers the wrong question.
    declarative_queries: Vec<crate::info::QueryHandlerInfo>,
    /// The candidate's declarative update handlers. See `declarative_queries`.
    declarative_updates: Vec<crate::info::UpdateHandlerInfo>,
    /// Per-workflow `#[workflow(max_input_bytes = …)]` overrides (issue #798,
    /// Codex round 22), keyed by workflow type name.
    ///
    /// The same class as `payload_limits` above, one level finer. The live worker
    /// does not apply a single fleet-wide workflow-input cap: it resolves the
    /// effective cap **per workflow type**, raising the registry default by that
    /// workflow's own declared override —
    /// `workflow.max_input_bytes.map_or(global, |per| per.max(global))`
    /// (`worker.rs`). A gate that applies only the global cap therefore replays a
    /// cap-raising workflow under a cap the promoted worker will not enforce: a
    /// frontier dispatch that the worker accepts is rejected here with
    /// `PayloadTooLarge`, which is the false-RED direction — drift reported on a
    /// workflow nobody changed, blocking a good release.
    ///
    /// Populated from [`register`](Self::register), which already receives the
    /// full [`WorkflowInfo`] and previously kept only `name → handler`. Empty for
    /// [`register_fn`](Self::register_fn) (a bare fn pointer carries no override),
    /// which resolves to the global cap — byte-for-byte the pre-fix behavior.
    workflow_input_caps: HashMap<String, u64>,
}

/// Owned borrows of a replayer's declarative handlers.
///
/// The executor takes `&[&QueryHandlerInfo]` (mirroring the live worker's own
/// signature), but the replayer stores owned `Vec`s so a builder can be moved
/// freely. This is the bridge: it holds the reference vectors alive across the
/// executor call so the borrowed slices stay valid.
struct DeclarativeHandlerRefs<'a> {
    queries: Vec<&'a crate::info::QueryHandlerInfo>,
    updates: Vec<&'a crate::info::UpdateHandlerInfo>,
}

impl<'a> DeclarativeHandlerRefs<'a> {
    /// Borrow as the executor's parameter shape.
    fn as_params(&'a self) -> crate::executor::ReplayDeclarativeHandlers<'a> {
        crate::executor::ReplayDeclarativeHandlers {
            queries: &self.queries,
            updates: &self.updates,
        }
    }
}

impl Default for WorkflowReplayer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "db")]
struct SampledExecution {
    shard_id: crate::types::ShardId,
    execution_id: crate::types::ExecutionId,
    workflow_name: String,
    created_at: chrono::DateTime<chrono::Utc>,
    context_headers: Option<serde_json::Value>,
    // Issue #772: per-execution deadline-aware CAN inputs, threaded into the
    // canary replay so the deadline branch is enabled and computed against the
    // row's own (pause/resume/redrive-shifted) deadline.
    execution_timeout: Option<chrono::Duration>,
    deadline_at: Option<chrono::DateTime<chrono::Utc>>,
    // Issue #698: the sampled row's spawning-parent id (`parent_id` column),
    // threaded into the canary replay so a parent-aware child does not
    // false-report non-determinism.
    parent_execution_id: Option<ExecutionId>,
    // Issue #698: the sampled row's business `workflow_id` column, threaded into
    // the canary replay so a workflow that branches on `ctx.info().workflow_id`
    // does not false-report non-determinism.
    workflow_id: String,
    // Issue #798: the sampled row's `queue_name` column, threaded into the canary
    // replay so a workflow that branches on `ctx.queue_name()` does not
    // false-report non-determinism.
    queue_name: String,
}

impl WorkflowReplayer {
    /// Create an empty replayer with no registered handlers.
    #[must_use]
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
            state: empty_shared_state(),
            context_headers: HashMap::new(),
            payload_offloader: None,
            use_advancing_clock: false,
            metrics: std::sync::Arc::new(crate::telemetry::NoOpMetrics),
            execution_timeout: None,
            parent_execution_id: None,
            workflow_id: None,
            queue_name: None,
            build_id: None,
            execution_id: None,
            history_policy: crate::context::WorkflowHistoryPolicy::default(),
            payload_limits: crate::executor::ReplayPayloadLimits::default(),
            declarative_queries: Vec::new(),
            declarative_updates: Vec::new(),
            workflow_input_caps: HashMap::new(),
        }
    }

    /// Set the history policy threaded into the replayed `WorkflowContext`
    /// (issue #614).
    ///
    /// Defaults to [`WorkflowHistoryPolicy::default`]. The replay-diagnosis
    /// endpoint threads the runtime registry's own policy
    /// (`registry.history_policy()`) so a workflow that branches command-affecting
    /// control flow on [`WorkflowContext::should_continue_as_new`] — which reads
    /// the policy's `continue_as_new_threshold` /
    /// `continue_as_new_deadline_fraction` — replays byte-faithfully to the live
    /// worker rather than under the default policy, which would surface a false
    /// `diverged` / `workflow_failed` verdict for deployments that configure a
    /// non-default policy.
    #[must_use]
    pub const fn with_history_policy(
        mut self,
        history_policy: crate::context::WorkflowHistoryPolicy,
    ) -> Self {
        self.history_policy = history_policy;
        self
    }

    /// Set the effective `execution_timeout` budget threaded into the replayed
    /// `WorkflowContext` (issue #772).
    ///
    /// Required to exercise deadline-aware `continue_as_new`: without it,
    /// `ctx.deadline()` is `None` and `ctx.should_continue_as_new()` never fires
    /// on the deadline branch, so a history that continued-as-new because of the
    /// deadline would replay as a divergence.
    #[must_use]
    pub const fn with_execution_timeout(mut self, execution_timeout: chrono::Duration) -> Self {
        self.execution_timeout = Some(execution_timeout);
        self
    }

    /// Set the spawning-parent execution id threaded into the replayed
    /// `WorkflowContext` (issue #698).
    ///
    /// Required to replay a **child** workflow whose command-affecting control
    /// flow branches on `ctx.info().parent_execution_id` /
    /// `ctx.parent_execution_id()`: that id is sourced from the
    /// `harvest_workflow_executions.parent_id` column and lives in no
    /// `WorkflowEvent`, so a pure-history fixture cannot carry it. Without it a
    /// parent-aware child replays with `parent = None` and false-reports
    /// non-determinism against a history recorded under `parent = Some(P)`.
    /// A [`HistorySnapshot`](HistorySnapshot::parent_execution_id) that carries
    /// its own value overrides this global. `None` (the default) models a
    /// top-level run.
    #[must_use]
    pub const fn with_parent_execution_id(mut self, parent: Option<ExecutionId>) -> Self {
        self.parent_execution_id = parent;
        self
    }

    /// Set the business-level `workflow_id` threaded into the replayed
    /// `WorkflowContext` (issue #698).
    ///
    /// Required to replay a workflow whose command-affecting control flow branches
    /// on `ctx.info().workflow_id` (or embeds it in an activity input): that id is
    /// sourced from the `harvest_workflow_executions.workflow_id` column and lives
    /// in no `WorkflowEvent`, so a raw-events fixture cannot carry it. Without it
    /// the replayed context reports `workflow_id == ""` and false-reports
    /// non-determinism against a history recorded under a real id. A
    /// [`HistorySnapshot`](HistorySnapshot::workflow_id) that carries its own value
    /// overrides this global. `None` (the default) models a run without an
    /// explicit id.
    #[must_use]
    pub fn with_workflow_id(mut self, workflow_id: impl Into<String>) -> Self {
        self.workflow_id = Some(workflow_id.into());
        self
    }

    /// Set the task queue threaded into the replayed `WorkflowContext`
    /// (issue #798).
    ///
    /// Required to replay a workflow whose command-affecting control flow branches
    /// on `ctx.queue_name()` (or embeds it in an activity input): the live worker
    /// supplies that value from the claimed task row, so it lives in no
    /// `WorkflowEvent` and a fixture that omits it cannot carry it. Without it the
    /// replayed context reports `queue_name == ""` and false-reports
    /// non-determinism against a history recorded on a named queue. A
    /// [`HistorySnapshot`](HistorySnapshot::queue_name) that carries its own value
    /// overrides this global. `None` (the default) preserves the empty-string
    /// default.
    #[must_use]
    pub fn with_queue_name(mut self, queue_name: impl Into<String>) -> Self {
        self.queue_name = Some(queue_name.into());
        self
    }

    /// Set the **candidate** build id threaded into the replayed
    /// `WorkflowContext` (issue #798), reported by
    /// [`WorkflowContext::build_id`](crate::context::WorkflowContext::build_id).
    ///
    /// Pass the build id of the worker you are **about to deploy**, not the one
    /// that recorded the fixtures. The live worker reports its own configured
    /// [`WorkerConfig::build_id`](crate::builder::WorkerConfig::build_id) — never
    /// the execution's recorded `assigned_build_id` — so a replay gate answers
    /// "what will the candidate do with these in-flight histories?" only when the
    /// candidate's own id is threaded through every fixture.
    ///
    /// Leaving it unset makes `ctx.build_id()` report `None`, so a candidate-only
    /// branch is unreachable during the gate: the historical path replays clean
    /// and the gate reports success on code that diverges the moment it is
    /// promoted. This is why the value is **not** carried on
    /// [`HistorySnapshot`] — a fixture-sourced build id would reintroduce exactly
    /// that blind spot.
    ///
    /// ```no_run
    /// # use autumn_harvest::testing::WorkflowReplayer;
    /// # async fn demo() {
    /// let report = WorkflowReplayer::new()
    ///     .with_build_id("v2") // the build about to be promoted
    ///     .replay_bundle(std::path::Path::new("./bundle"))
    ///     .await;
    /// assert!(report.is_clean());
    /// # }
    /// ```
    #[must_use]
    pub fn with_build_id(mut self, build_id: impl Into<String>) -> Self {
        self.build_id = Some(build_id.into());
        self
    }

    /// Apply the **candidate** worker's payload caps to the replay context
    /// (issue #798).
    ///
    /// Bytes, in the order the executor threads them:
    /// `(max_activity_input, max_signal_payload, max_workflow_input)`. `0` means
    /// "no cap", matching
    /// [`WorkflowContext::with_payload_caps`](crate::context::WorkflowContext::with_payload_caps).
    /// Pass the values the candidate build configures on its `HarvestBuilder`, so
    /// the gate replays under the limits the promoted worker will actually
    /// enforce rather than the library defaults.
    ///
    /// `max_activity_result` is deliberately absent: it is enforced by the worker
    /// after an activity returns, never by `WorkflowContext`, so it cannot affect
    /// a replay and accepting it here would imply a guarantee that does not exist.
    #[must_use]
    pub const fn with_payload_caps(
        mut self,
        max_activity_input: u64,
        max_signal_payload: u64,
        max_workflow_input: u64,
    ) -> Self {
        self.payload_limits.max_activity_input = max_activity_input;
        self.payload_limits.max_signal_payload = max_signal_payload;
        self.payload_limits.max_workflow_input = max_workflow_input;
        self
    }

    /// Apply the **candidate** worker's large-payload offload threshold (#524)
    /// to the replay context (issue #798).
    ///
    /// A payload above the threshold is offloaded rather than capped, so a gate
    /// that knows the cap but not the threshold reports drift the promoted worker
    /// would never hit. `None` (the default) models a worker with no
    /// `PayloadStore` registered.
    #[must_use]
    pub const fn with_payload_offload_threshold(mut self, threshold: Option<u64>) -> Self {
        self.payload_limits.offload_threshold = threshold;
        self
    }

    /// Register the **candidate's** declarative `#[query]` handlers on the replay
    /// context (issue #798).
    ///
    /// The live worker registers these before any workflow code runs, and
    /// `ctx.list_query_names()` surfaces them, so a workflow that branches on
    /// which handlers exist replays down the wrong branch without them. Pass the
    /// same `queries![...]` collection the candidate build registers.
    #[must_use]
    pub fn queries(mut self, queries: Vec<crate::info::QueryHandlerInfo>) -> Self {
        self.declarative_queries = queries;
        self
    }

    /// Register the **candidate's** declarative `#[update]` handlers on the
    /// replay context (issue #798). See [`queries`](Self::queries).
    #[must_use]
    pub fn updates(mut self, updates: Vec<crate::info::UpdateHandlerInfo>) -> Self {
        self.declarative_updates = updates;
        self
    }

    /// Borrow the configured declarative handlers for an executor call.
    fn declarative_handlers(&self) -> DeclarativeHandlerRefs<'_> {
        DeclarativeHandlerRefs {
            queries: self.declarative_queries.iter().collect(),
            updates: self.declarative_updates.iter().collect(),
        }
    }

    /// Resolve the payload limits for one workflow type (issue #798, round 22).
    ///
    /// Mirrors the live worker's own arithmetic verbatim:
    ///
    /// ```text
    /// workflow.max_input_bytes.map_or(registry.max_workflow_input_bytes,
    ///                                 |per| per.max(registry.max_workflow_input_bytes))
    /// ```
    ///
    /// so the declared override **raises** the global cap and never lowers it.
    /// Reproducing the worker's expression — rather than an arguably-nicer rule —
    /// is the whole point: a gate is only trustworthy if its cap is the cap the
    /// promoted worker enforces, including where that expression is surprising.
    /// (`0` means "no cap" by the crate-wide convention, and `per.max(0) == per`
    /// narrows it; that quirk is the worker's, and a gate that "fixed" it here
    /// would answer a question about a worker that does not exist.)
    ///
    /// A workflow with no declared override, or one registered via
    /// [`register_fn`](Self::register_fn), resolves to the global limits
    /// unchanged.
    fn payload_limits_for(&self, workflow_name: &str) -> crate::executor::ReplayPayloadLimits {
        let mut limits = self.payload_limits;
        if let Some(&per) = self.workflow_input_caps.get(workflow_name) {
            limits.max_workflow_input = per.max(limits.max_workflow_input);
        }
        limits
    }

    /// Set the `execution_id` threaded into the replayed `WorkflowContext` on the
    /// raw [`replay_from_events`](Self::replay_from_events) path (issue #698).
    ///
    /// `ctx.info().execution_id` is replay-safe — the production/DB/snapshot/canary
    /// replay paths all recover it from the recorded run — but a raw-events fixture
    /// carries no [`HistorySnapshot`], so `replay_from_events` mints a fresh id
    /// unless you supply the captured original run id here. Without it a workflow
    /// that recorded `ctx.info().execution_id` in a command-affecting value (e.g.
    /// an activity input) false-reports non-determinism against a history recorded
    /// under the real id. The snapshot / DB / canary paths already carry the real
    /// id and ignore this global. `None` (the default) mints a fresh id, matching
    /// pre-#698 behaviour.
    #[must_use]
    pub const fn with_execution_id(mut self, execution_id: ExecutionId) -> Self {
        self.execution_id = Some(execution_id);
        self
    }

    /// Inject a [`MetricsRecorder`](crate::telemetry::MetricsRecorder) into replayed
    /// `WorkflowContext` instances.
    ///
    /// Use this in replay-safety tests to verify that user metric calls are
    /// **suppressed** during deterministic replay (counter stays at 0 after N
    /// replays) and emitted exactly once on the live frontier execution.
    #[must_use]
    pub fn with_metrics(
        mut self,
        metrics: std::sync::Arc<dyn crate::telemetry::MetricsRecorder>,
    ) -> Self {
        self.metrics = metrics;
        self
    }

    /// Enable the advancing virtual clock for this replayer (issue #526).
    ///
    /// When set, `ctx.now()` inside the replayed workflow reflects cumulative
    /// durable-timer duration rather than the fixed `WorkflowStarted` timestamp.
    /// Required when replaying histories from [`WorkflowTestEnv`] runs that
    /// branch on `ctx.now()`, otherwise the replay produces a false
    /// `ReplayStatus::Failed`.
    ///
    /// [`WorkflowTestEnv`]: crate::testing::WorkflowTestEnv
    #[must_use]
    pub const fn with_advancing_timer_clock(mut self) -> Self {
        self.use_advancing_clock = true;
        self
    }

    /// Set context headers to propagate into the replayed `WorkflowContext`.
    #[must_use]
    pub fn with_context_headers(mut self, headers: HashMap<String, String>) -> Self {
        self.context_headers = headers;
        self
    }

    /// Inject a typed shared-state value available to workflow handlers via
    /// `ctx.state::<T>()` during replay.
    ///
    /// Call this for every state type the workflow accesses, otherwise
    /// `ctx.state::<T>()` returns `None` and the workflow may return
    /// `WorkflowFailed` even when the history is fully deterministic.
    ///
    /// ```rust,no_run
    /// # use autumn_harvest::testing::WorkflowReplayer;
    /// # use autumn_harvest::event::WorkflowEvent;
    /// struct MyConfig { value: u32 }
    /// # async fn example() {
    /// # let history: Vec<WorkflowEvent> = vec![];
    /// let report = WorkflowReplayer::new()
    ///     .with_state(MyConfig { value: 42 })
    ///     // .register_fn(...)
    ///     .replay_from_events(history)
    ///     .await;
    /// # }
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the internal `Arc` has been cloned before `with_state` is
    /// called — this is unreachable in normal builder usage where `with_state`
    /// is always called on a freshly constructed `WorkflowReplayer`.
    #[must_use]
    pub fn with_state<T: Send + Sync + 'static>(mut self, value: T) -> Self {
        std::sync::Arc::get_mut(&mut self.state)
            .expect("state Arc has no other references during WorkflowReplayer construction")
            .insert(TypeId::of::<T>(), Box::new(value));
        self
    }

    /// Replace the shared state with a pre-built [`SharedState`] arc.
    ///
    /// Prefer [`with_state`](Self::with_state) for constructing state from typed
    /// values. This lower-level variant forwards an *already-built* container so
    /// the replay sees exactly the same shared state as another execution path.
    ///
    /// Used internally by [`TestRunOutcome::replay_check`] to forward the test
    /// environment's state to the replayer, and by the replay-diagnosis endpoint
    /// to thread the **live worker's** registry shared state
    /// (`registry.shared_state()`) into the diagnosis replay so a workflow that
    /// reads typed state via `ctx.state::<T>()` during replay sees the same state
    /// the worker's replay path sees — otherwise the per-execution verdict would
    /// spuriously report `workflow_failed`/`diverged` for state-registering
    /// deployments even when the execution replays cleanly on the worker.
    #[must_use]
    pub fn with_shared_state(mut self, state: SharedState) -> Self {
        self.state = state;
        self
    }

    /// Replay an exported in-flight history bundle and return an aggregate
    /// [`ReplayDriftReport`] (issue #798).
    ///
    /// Convenience entry point for callers already holding a
    /// [`WorkflowReplayer`]; it delegates to [`ReplayVerifier::replay_bundle`],
    /// which owns the bundle walk, concurrency budget, and per-fixture timeout.
    /// Use [`ReplayVerifier`] directly to configure those, plus
    /// `allow_unregistered`, `allow_empty_bundle`, or
    /// `require_complete_coverage`.
    pub async fn replay_bundle(&self, dir: impl AsRef<std::path::Path>) -> ReplayDriftReport {
        // Same module, so the verifier's private fields are reachable — no need
        // for a public setter that would only ever serve this delegation.
        let mut verifier = ReplayVerifier::new();
        verifier.handlers.clone_from(&self.handlers);
        verifier.state = self.state.clone();
        // Carry every replay-context value this replayer was configured with
        // across the delegation. Dropping any of them silently ignores the
        // corresponding `WorkflowReplayer::with_*` builder call on the bundle
        // path and replays under a different context — reporting drift for a
        // workflow that never drifted. Issue #614 established this for
        // `history_policy`; Codex round-10 P2 extended it to the four
        // `HistorySnapshot`-metadata fallbacks, which matter for any fixture
        // that omits the metadata (hand-built, or exported before #698).
        verifier.replay_defaults = FixtureReplayDefaults {
            context_headers: self.context_headers.clone(),
            execution_timeout: self.execution_timeout,
            parent_execution_id: self.parent_execution_id,
            workflow_id: self.workflow_id.clone(),
            queue_name: self.queue_name.clone(),
            build_id: self.build_id.clone(),
            history_policy: self.history_policy,
            // Codex round 20 extended the same rule to the candidate worker's
            // payload limits: dropping them here would replay the bundle under
            // the library defaults and certify a cap-lowering build.
            payload_limits: self.payload_limits,
            // Codex round 21 extended it again to the candidate's declarative
            // handlers: dropping them replays with an empty registry, so a
            // workflow branching on `ctx.list_query_names()` takes the other
            // branch and is reported as drift it never had.
            declarative_queries: self.declarative_queries.clone(),
            declarative_updates: self.declarative_updates.clone(),
            // Codex round 22 extended it once more to the per-workflow input-cap
            // overrides: dropping them replays a cap-raising workflow under the
            // global cap the promoted worker will not enforce for it.
            workflow_input_caps: self.workflow_input_caps.clone(),
        };
        verifier.replay_bundle(dir).await
    }

    /// Register a batch of workflows from a `workflows![]` macro call.
    ///
    /// ```rust,no_run
    /// # use autumn_harvest::testing::WorkflowReplayer;
    /// # use autumn_harvest::info::WorkflowInfo;
    /// # fn make_infos() -> Vec<WorkflowInfo> { vec![] }
    /// let replayer = WorkflowReplayer::new().register(make_infos());
    /// ```
    #[must_use]
    pub fn register(mut self, workflows: Vec<WorkflowInfo>) -> Self {
        for wf in workflows {
            // Issue #798 (Codex round 22): retain the per-workflow input-cap
            // override. The live worker raises its global cap by this value for
            // this workflow type; dropping it replayed a cap-raising workflow
            // under a cap the promoted worker never enforces.
            if let Some(per) = wf.max_input_bytes {
                self.workflow_input_caps.insert(wf.name.to_string(), per);
            }
            self.handlers.insert(wf.name.to_string(), wf.handler);
        }
        self
    }

    /// Register a single handler by name — useful in tests where workflow
    /// functions are defined as bare `fn` pointers without the `#[workflow]`
    /// macro.
    #[must_use]
    pub fn register_fn(mut self, name: impl Into<String>, handler: WorkflowHandlerFn) -> Self {
        self.handlers.insert(name.into(), handler);
        self
    }

    /// Attach a [`PayloadOffloader`](crate::payload_store::PayloadOffloader) so
    /// a recorded history whose payloads were offloaded (issue #524) is inflated
    /// from the backing store before replay, reconstructing byte-identical
    /// inputs/outputs.
    #[must_use]
    pub fn with_payload_offloader(
        mut self,
        offloader: std::sync::Arc<crate::payload_store::PayloadOffloader>,
    ) -> Self {
        self.payload_offloader = Some(offloader);
        self
    }

    /// Inflate offloaded payloads in `events` if an offloader is configured.
    async fn maybe_inflate(
        &self,
        events: Vec<WorkflowEvent>,
    ) -> Result<Vec<WorkflowEvent>, String> {
        let Some(off) = &self.payload_offloader else {
            return Ok(events);
        };
        let mut out = Vec::with_capacity(events.len());
        for ev in events {
            let mut v = serde_json::to_value(&ev).map_err(|e| e.to_string())?;
            off.inflate_event_value(&mut v)
                .await
                .map_err(|e| e.to_string())?;
            out.push(serde_json::from_value(v).map_err(|e| e.to_string())?);
        }
        Ok(out)
    }

    /// Replay a recorded [`HistorySnapshot`] against the handler registered
    /// for `snapshot.workflow_name`.
    ///
    /// This is the primary routing method used internally by
    /// [`replay_from_json`](Self::replay_from_json) and
    /// [`replay_from_db`](Self::replay_from_db).  Prefer those for most use
    /// cases; call `replay_from_snapshot` directly when you need to override
    /// the workflow name after constructing the snapshot (e.g. the
    /// `--workflow` flag in `harvest-replay`).
    ///
    /// Returns a [`ReplayReport`] regardless of outcome.  If
    /// `snapshot.workflow_name` is not registered, the report contains
    /// `ReplayStatus::WorkflowFailed` with a descriptive error.
    ///
    /// The deadline-aware continue-as-new budget (issue #772) prefers the
    /// snapshot's own [`execution_timeout`](HistorySnapshot::execution_timeout) /
    /// [`deadline_at`](HistorySnapshot::deadline_at) when the JSON carries them
    /// (a full history export does), falling back to this replayer's global
    /// [`with_execution_timeout`](Self::with_execution_timeout) for a legacy
    /// snapshot without them. This makes a deadline-aware exported history
    /// replay cleanly through the JSON / `harvest-replay` path instead of
    /// false-reporting non-determinism. [`replay_from_db`](Self::replay_from_db)
    /// threads the execution row's own values.
    pub async fn replay_from_snapshot(&self, snapshot: HistorySnapshot) -> ReplayReport {
        // Prefer the per-snapshot metadata (issue #772); fall back to the
        // replayer's global timeout for a legacy snapshot that lacks it.
        let execution_timeout = snapshot.execution_timeout.or(self.execution_timeout);
        let deadline_at = snapshot.deadline_at;
        // Issue #698: prefer the snapshot's own parent id; fall back to the
        // replayer's global (set via `with_parent_execution_id`).
        let parent_execution_id = snapshot.parent_execution_id.or(self.parent_execution_id);
        // Issue #698: prefer the snapshot's own `workflow_id`; fall back to the
        // replayer's global (set via `with_workflow_id`).
        let workflow_id = snapshot
            .workflow_id
            .clone()
            .or_else(|| self.workflow_id.clone());
        // Issue #798: prefer the snapshot's own `queue_name`; fall back to the
        // replayer's global (set via `with_queue_name`).
        let queue_name = snapshot
            .queue_name
            .clone()
            .or_else(|| self.queue_name.clone());
        self.replay_from_snapshot_effective(
            snapshot,
            execution_timeout,
            deadline_at,
            parent_execution_id,
            workflow_id,
            queue_name,
        )
        .await
    }

    /// Snapshot replay with an explicit per-execution `execution_timeout` /
    /// live `deadline_at` (issue #772) / spawning-parent id (issue #698). The
    /// public [`replay_from_snapshot`](Self::replay_from_snapshot) delegates here
    /// with the global timeout, no live deadline, and the resolved parent;
    /// [`replay_from_db`](Self::replay_from_db) passes the loaded execution row's
    /// own values.
    async fn replay_from_snapshot_effective(
        &self,
        snapshot: HistorySnapshot,
        execution_timeout: Option<chrono::Duration>,
        deadline_at: Option<chrono::DateTime<chrono::Utc>>,
        parent_execution_id: Option<ExecutionId>,
        workflow_id: Option<String>,
        // Issue #798: the execution's task queue, so a workflow branching on
        // `ctx.queue_name()` does not replay under `""`.
        queue_name: Option<String>,
    ) -> ReplayReport {
        let Some(&handler) = self.handlers.get(&snapshot.workflow_name) else {
            return ReplayReport {
                execution_id: snapshot.execution_id,
                events_replayed: 0,
                status: ReplayStatus::WorkflowFailed {
                    error: format!(
                        "workflow '{}' not registered in this replayer",
                        snapshot.workflow_name
                    ),
                    event_index: 0,
                },
                mismatched_command_summary: None,
            };
        };

        // Issue #798 (Codex round 21): borrow the candidate's declarative
        // handlers for the duration of the executor call.
        let declarative = self.declarative_handlers();
        let exec_id = snapshot.execution_id;
        // Issue #698: the workflow type name (mechanism 1 — the handler-lookup key)
        // applied to the replayed context via `.with_workflow_name(...)`.
        let workflow_name = snapshot.workflow_name.clone();
        // Issue #798 (Codex round 22): resolve the cap for THIS workflow type,
        // raising the global by its declared override exactly as the worker does.
        // Bound before `workflow_name` is moved into the executor call below.
        let payload_limits = self.payload_limits_for(&workflow_name);
        let events = match self.maybe_inflate(snapshot.events).await {
            Ok(e) => e,
            Err(error) => {
                return ReplayReport {
                    execution_id: exec_id,
                    events_replayed: 0,
                    status: ReplayStatus::WorkflowFailed {
                        error,
                        event_index: 0,
                    },
                    mismatched_command_summary: None,
                };
            }
        };
        let total_events = events.len();
        let input = extract_input(&events);

        let headers = snapshot
            .context_headers
            .unwrap_or_else(|| self.context_headers.clone());
        let outcome = if self.use_advancing_clock {
            run_workflow_strict_advancing_clock(
                exec_id,
                events,
                handler,
                input,
                self.state.clone(),
                headers,
                self.metrics.clone(),
                execution_timeout,
                deadline_at,
                parent_execution_id,
                workflow_name,
                workflow_id,
                // Issue #798: the execution's task queue.
                queue_name,
                // Issue #798: the candidate build id. Deliberately read from the
                // replayer (never the fixture): the gate must answer what the build
                // about to be promoted does, not what the recording build did.
                self.build_id.clone(),
                // Issue #614: thread the replayer's history policy so a strict
                // replay of a `should_continue_as_new`-branching workflow stays
                // faithful to the live worker (which uses `registry.history_policy()`).
                self.history_policy,
                // Issue #798 (Codex round 20): the candidate worker's payload
                // limits, for the same reason as the build id above — a build
                // that lowers a cap breaks the very in-flight runs the gate
                // sampled, and the library default would certify it.
                payload_limits,
                // Issue #798 (Codex round 21): the candidate's declarative
                // `#[query]`/`#[update]` handlers. The live worker registers
                // these before the body runs, so a workflow that branches on
                // `ctx.list_query_names()` needs them to replay the same path.
                declarative.as_params(),
            )
            .await
        } else {
            run_workflow_strict(
                exec_id,
                events,
                handler,
                input,
                self.state.clone(),
                headers,
                self.metrics.clone(),
                execution_timeout,
                deadline_at,
                parent_execution_id,
                workflow_name,
                workflow_id,
                // Issue #798: the execution's task queue.
                queue_name,
                // Issue #798: the candidate build id. Deliberately read from the
                // replayer (never the fixture): the gate must answer what the build
                // about to be promoted does, not what the recording build did.
                self.build_id.clone(),
                // Issue #614: thread the replayer's history policy so a strict
                // replay of a `should_continue_as_new`-branching workflow stays
                // faithful to the live worker (which uses `registry.history_policy()`).
                self.history_policy,
                // Issue #798 (Codex round 20): the candidate worker's payload
                // limits, for the same reason as the build id above — a build
                // that lowers a cap breaks the very in-flight runs the gate
                // sampled, and the library default would certify it.
                payload_limits,
                // Issue #798 (Codex round 21): the candidate's declarative
                // `#[query]`/`#[update]` handlers. The live worker registers
                // these before the body runs, so a workflow that branches on
                // `ctx.list_query_names()` needs them to replay the same path.
                declarative.as_params(),
            )
            .await
        };
        outcome_to_report(exec_id, total_events, outcome, false)
    }

    /// Replay a snapshot in canary mode (used for deploy-time verify).
    ///
    /// Prefers the snapshot's own `execution_timeout`/`deadline_at` when carried,
    /// falling back to the global [`with_execution_timeout`](Self::with_execution_timeout)
    /// (issue #772); [`run_canary`](Self::run_canary) threads each sampled
    /// execution row's own values via
    /// [`replay_canary_snapshot_effective`](Self::replay_canary_snapshot_effective).
    pub async fn replay_canary_snapshot(&self, snapshot: HistorySnapshot) -> ReplayReport {
        let execution_timeout = snapshot.execution_timeout.or(self.execution_timeout);
        let deadline_at = snapshot.deadline_at;
        // Issue #698: prefer the snapshot's own parent id; fall back to the global.
        let parent_execution_id = snapshot.parent_execution_id.or(self.parent_execution_id);
        // Issue #698: prefer the snapshot's own `workflow_id`; fall back to the global.
        let workflow_id = snapshot
            .workflow_id
            .clone()
            .or_else(|| self.workflow_id.clone());
        // Issue #798: prefer the snapshot's own `queue_name`; fall back to the global.
        let queue_name = snapshot
            .queue_name
            .clone()
            .or_else(|| self.queue_name.clone());
        self.replay_canary_snapshot_effective(
            snapshot,
            execution_timeout,
            deadline_at,
            parent_execution_id,
            workflow_id,
            queue_name,
        )
        .await
    }

    /// Canary snapshot replay with an explicit per-execution `execution_timeout`
    /// / live `deadline_at` (issue #772) / spawning-parent id (issue #698) /
    /// task queue (issue #798).
    async fn replay_canary_snapshot_effective(
        &self,
        snapshot: HistorySnapshot,
        execution_timeout: Option<chrono::Duration>,
        deadline_at: Option<chrono::DateTime<chrono::Utc>>,
        parent_execution_id: Option<ExecutionId>,
        workflow_id: Option<String>,
        queue_name: Option<String>,
    ) -> ReplayReport {
        let Some(&handler) = self.handlers.get(&snapshot.workflow_name) else {
            return ReplayReport {
                execution_id: snapshot.execution_id,
                events_replayed: 0,
                status: ReplayStatus::WorkflowFailed {
                    error: format!(
                        "workflow '{}' not registered in this replayer",
                        snapshot.workflow_name
                    ),
                    event_index: 0,
                },
                mismatched_command_summary: None,
            };
        };

        // Issue #798 (Codex round 21): borrow the candidate's declarative
        // handlers for the duration of the executor call.
        let declarative = self.declarative_handlers();
        let exec_id = snapshot.execution_id;
        // Issue #698: the workflow type name (mechanism 1 — the handler-lookup key).
        let workflow_name = snapshot.workflow_name.clone();
        // Issue #798 (Codex round 22): per-workflow-type cap; see the strict path.
        let payload_limits = self.payload_limits_for(&workflow_name);
        let total_events = snapshot.events.len();
        let input = extract_input(&snapshot.events);

        let headers = snapshot
            .context_headers
            .unwrap_or_else(|| self.context_headers.clone());
        let outcome = run_workflow_canary(
            exec_id,
            snapshot.events,
            handler,
            input,
            self.state.clone(),
            headers,
            self.metrics.clone(),
            execution_timeout,
            deadline_at,
            parent_execution_id,
            workflow_name,
            workflow_id,
            // Issue #798: the execution's task queue.
            queue_name,
            // Issue #798: the candidate build id. Deliberately read from the
            // replayer (never the fixture): the gate must answer what the build
            // about to be promoted does, not what the recording build did.
            self.build_id.clone(),
            // Issue #614: thread the replayer's history policy so a canary replay
            // of a `should_continue_as_new`-branching workflow stays faithful to
            // the live worker (which uses `registry.history_policy()`).
            self.history_policy,
            // Issue #798 (Codex round 20): the candidate worker's payload limits.
            // This is the in-flight gate's own path, and an in-flight fixture
            // parks at the frontier — exactly where a fresh dispatch consults the
            // cap — so the library default here is what turns a cap-lowering
            // build into a false GREEN.
            payload_limits,
            // Issue #798 (Codex round 21): the candidate's declarative handlers
            // (see the strict path). This is the in-flight gate's own call.
            declarative.as_params(),
        )
        .await;
        outcome_to_report(exec_id, total_events, outcome, true)
    }

    /// Replay a raw event list against the **single** registered handler.
    ///
    /// This is the most concise API when the replayer has exactly one handler:
    ///
    /// ```rust,no_run
    /// # use autumn_harvest::testing::{WorkflowReplayer, ReplayStatus};
    /// # use autumn_harvest::event::WorkflowEvent;
    /// # use autumn_harvest::context::WorkflowContext;
    /// # use serde_json::Value;
    /// # use std::pin::Pin;
    /// # fn my_workflow<'a>(ctx: &'a WorkflowContext, input: Value)
    /// #   -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>
    /// # { Box::pin(async move { Ok(input) }) }
    /// # async fn example() {
    /// # let history: Vec<WorkflowEvent> = vec![];
    /// let report = WorkflowReplayer::new()
    ///     .register_fn("my_workflow", my_workflow)
    ///     .replay_from_events(history)
    ///     .await;
    ///
    /// assert!(
    ///     matches!(report.status, ReplayStatus::ReplaySucceeded),
    ///     "replay regression detected:\n{report}"
    /// );
    /// # }
    /// ```
    ///
    /// Returns `ReplayStatus::WorkflowFailed` when zero or more than one
    /// handler is registered — use [`replay_from_snapshot`](Self::replay_from_snapshot)
    /// or [`replay_from_json`](Self::replay_from_json) to route to a named
    /// handler when multiple are registered.
    ///
    /// # Panics
    ///
    /// Panics if the internal `HashMap::iter().next().unwrap()` is reached on
    /// an empty map — this is unreachable because the empty-map case returns
    /// early with `WorkflowFailed` before that line.
    pub async fn replay_from_events(&self, events: Vec<WorkflowEvent>) -> ReplayReport {
        if self.handlers.len() != 1 {
            let exec_id = self.execution_id.unwrap_or_default();
            let error = if self.handlers.is_empty() {
                "no workflow handlers registered; call register_fn() before replay_from_events()"
                    .to_string()
            } else {
                format!(
                    "replay_from_events() requires exactly one registered handler, but {} are \
                     registered; use replay_from_snapshot() or replay_from_json() to route by name",
                    self.handlers.len()
                )
            };
            return ReplayReport {
                execution_id: exec_id,
                events_replayed: 0,
                status: ReplayStatus::WorkflowFailed {
                    error,
                    event_index: 0,
                },
                mismatched_command_summary: None,
            };
        }

        // Issue #698: the single registered handler's KEY is the authoritative
        // workflow type name — apply it to the replayed context via
        // `.with_workflow_name(...)` so `ctx.info().workflow_type` is the real
        // name, not "". A raw-events fixture carries no `workflow_id`, so fall back
        // to the replayer's global (`with_workflow_id`). The raw-events path
        // likewise carries no `execution_id`, so use the replayer's global
        // (`with_execution_id`) when set — otherwise mint a fresh id (pre-#698
        // behaviour), which false-flags only a workflow that recorded its own
        // `ctx.info().execution_id` in a command-affecting value.
        let (name, &handler) = self.handlers.iter().next().unwrap();
        let workflow_name = name.clone();
        // Issue #798 (Codex round 22): per-workflow-type cap; see the strict path.
        let payload_limits = self.payload_limits_for(&workflow_name);
        let workflow_id = self.workflow_id.clone();
        // Issue #798: a raw-events fixture carries no `queue_name` either, so use
        // the replayer's global (`with_queue_name`) when set.
        let queue_name = self.queue_name.clone();
        // Issue #798 (Codex round 21): borrow the candidate's declarative
        // handlers for the duration of the executor call.
        let declarative = self.declarative_handlers();
        let exec_id = self.execution_id.unwrap_or_default();
        let events = match self.maybe_inflate(events).await {
            Ok(e) => e,
            Err(error) => {
                return ReplayReport {
                    execution_id: exec_id,
                    events_replayed: 0,
                    status: ReplayStatus::WorkflowFailed {
                        error,
                        event_index: 0,
                    },
                    mismatched_command_summary: None,
                };
            }
        };
        let total_events = events.len();
        let input = extract_input(&events);

        let outcome = if self.use_advancing_clock {
            run_workflow_strict_advancing_clock(
                exec_id,
                events,
                handler,
                input,
                self.state.clone(),
                self.context_headers.clone(),
                self.metrics.clone(),
                self.execution_timeout,
                // No per-row live deadline on the raw-events path (issue #772).
                None,
                // Issue #698: the replayer's global parent id (a raw-events
                // fixture carries no snapshot to override it).
                self.parent_execution_id,
                workflow_name,
                workflow_id,
                // Issue #798: the replayer's global queue (a raw-events fixture
                // carries no snapshot to override it).
                queue_name,
                // Issue #798: the candidate build id. Deliberately read from the
                // replayer (never the fixture): the gate must answer what the build
                // about to be promoted does, not what the recording build did.
                self.build_id.clone(),
                // Issue #614: thread the replayer's history policy so a strict
                // replay of a `should_continue_as_new`-branching workflow stays
                // faithful to the live worker (which uses `registry.history_policy()`).
                self.history_policy,
                // Issue #798 (Codex round 20): the candidate worker's payload
                // limits, for the same reason as the build id above — a build
                // that lowers a cap breaks the very in-flight runs the gate
                // sampled, and the library default would certify it.
                payload_limits,
                // Issue #798 (Codex round 21): the candidate's declarative
                // `#[query]`/`#[update]` handlers. The live worker registers
                // these before the body runs, so a workflow that branches on
                // `ctx.list_query_names()` needs them to replay the same path.
                declarative.as_params(),
            )
            .await
        } else {
            run_workflow_strict(
                exec_id,
                events,
                handler,
                input,
                self.state.clone(),
                self.context_headers.clone(),
                self.metrics.clone(),
                self.execution_timeout,
                None,
                self.parent_execution_id,
                workflow_name,
                workflow_id,
                queue_name,
                // Issue #798: the candidate build id. Deliberately read from the
                // replayer (never the fixture): the gate must answer what the build
                // about to be promoted does, not what the recording build did.
                self.build_id.clone(),
                // Issue #614: thread the replayer's history policy so a strict
                // replay of a `should_continue_as_new`-branching workflow stays
                // faithful to the live worker (which uses `registry.history_policy()`).
                self.history_policy,
                // Issue #798 (Codex round 20): the candidate worker's payload
                // limits, for the same reason as the build id above — a build
                // that lowers a cap breaks the very in-flight runs the gate
                // sampled, and the library default would certify it.
                payload_limits,
                // Issue #798 (Codex round 21): the candidate's declarative
                // `#[query]`/`#[update]` handlers. The live worker registers
                // these before the body runs, so a workflow that branches on
                // `ctx.list_query_names()` needs them to replay the same path.
                declarative.as_params(),
            )
            .await
        };
        outcome_to_report(exec_id, total_events, outcome, false)
    }

    /// Replay as if the workflow history were reset at `reset_to_event_id`.
    ///
    /// This helper truncates the supplied history through the chosen boundary,
    /// appends a synthetic [`WorkflowEvent::WorkflowResetFork`] marker, and runs
    /// the normal strict replay path. It is intentionally read-only: no
    /// database rows are copied or mutated.
    pub async fn replay_with_reset(
        &self,
        history: Vec<WorkflowEvent>,
        reset_to_event_id: i64,
    ) -> ReplayReport {
        if reset_to_event_id < 0 {
            return ReplayReport {
                execution_id: ExecutionId::new(),
                events_replayed: 0,
                status: ReplayStatus::WorkflowFailed {
                    error: format!("reset_to_event_id {reset_to_event_id} is negative"),
                    event_index: 0,
                },
                mismatched_command_summary: None,
            };
        }

        let Ok(target) = usize::try_from(reset_to_event_id) else {
            return ReplayReport {
                execution_id: ExecutionId::new(),
                events_replayed: 0,
                status: ReplayStatus::WorkflowFailed {
                    error: format!("reset_to_event_id {reset_to_event_id} cannot be represented"),
                    event_index: 0,
                },
                mismatched_command_summary: None,
            };
        };
        if target >= history.len() {
            return ReplayReport {
                execution_id: ExecutionId::new(),
                events_replayed: history.len(),
                status: ReplayStatus::WorkflowFailed {
                    error: format!(
                        "reset_to_event_id {reset_to_event_id} is outside history range"
                    ),
                    event_index: history.len(),
                },
                mismatched_command_summary: None,
            };
        }

        let mut reset_history = history.into_iter().take(target + 1).collect::<Vec<_>>();
        reset_history.push(WorkflowEvent::WorkflowResetFork {
            reset_from_exec_id: ExecutionId::new(),
            reset_to_event_id,
            reason: "replay_with_reset".to_string(),
            operator_id: "workflow-replayer".to_string(),
        });
        self.replay_from_events(reset_history).await
    }

    /// Replay from a JSON [`HistorySnapshot`] document.
    ///
    /// The JSON must be a serialised [`HistorySnapshot`] — it contains the
    /// workflow name, execution ID, and event list.
    ///
    /// # Errors
    ///
    /// Returns `serde_json::Error` if the input is not valid JSON or cannot
    /// be deserialised as a `HistorySnapshot`.
    pub async fn replay_from_json(&self, json: &str) -> Result<ReplayReport, serde_json::Error> {
        let snapshot: HistorySnapshot = serde_json::from_str(json)?;
        Ok(self.replay_from_snapshot(snapshot).await)
    }

    /// Replay a workflow execution directly from the Postgres event store.
    ///
    /// Pulls the event history from `harvest_events` and the workflow name
    /// from `harvest_workflow_executions`, then replays against the registered
    /// handler.
    ///
    /// # Errors
    ///
    /// Returns `HarvestError` on any database access failure or if the
    /// execution record is not found.
    #[cfg(feature = "db")]
    pub async fn replay_from_db(
        &self,
        conn: &mut diesel_async::AsyncPgConnection,
        exec_id: crate::types::ExecutionId,
    ) -> crate::error::HarvestResult<ReplayReport> {
        use crate::store::load_history;

        // Load event history.
        let history = load_history(conn, exec_id).await?;

        // Load workflow name, context headers, and the per-execution
        // deadline-aware CAN inputs (`execution_timeout` + live `deadline_at`)
        // from the executions table (issue #772). Threading the row's own values
        // — rather than the replayer-global `self.execution_timeout` — keeps the
        // deadline branch enabled and computed against the correct deadline, so a
        // row that recorded a `SideEffectRecorded{Now}` from the deadline branch
        // replays cleanly instead of surfacing false non-determinism.
        let meta = load_workflow_name_and_headers(conn, exec_id).await?;

        let workflow_id = Some(meta.workflow_id.clone());
        // Issue #798: the row's own task queue.
        let queue_name = Some(meta.queue_name.clone());
        let snapshot = HistorySnapshot {
            workflow_name: meta.workflow_name,
            execution_id: exec_id,
            events: history.events,
            context_headers: Some(meta.headers),
            execution_timeout: meta.execution_timeout,
            deadline_at: meta.deadline_at,
            // Issue #698: the row's own spawning-parent id, so a parent-aware
            // child replays deterministically from the store.
            parent_execution_id: meta.parent_execution_id,
            // Issue #698: the row's own business `workflow_id`.
            workflow_id: workflow_id.clone(),
            // Issue #798: the row's own task queue.
            queue_name: queue_name.clone(),
        };
        Ok(self
            .replay_from_snapshot_effective(
                snapshot,
                meta.execution_timeout,
                meta.deadline_at,
                meta.parent_execution_id,
                workflow_id,
                queue_name,
            )
            .await)
    }

    /// Run the replay canary over a sample of running executions.
    #[cfg(feature = "db")]
    #[allow(clippy::too_many_lines, clippy::missing_errors_doc)]
    pub async fn run_canary(
        &self,
        pool: &crate::shard::ShardedDbPool,
        options: ReplayCanaryOptions,
    ) -> crate::error::HarvestResult<ReplayCanaryReport> {
        let mut options = options;
        options.sample_size = options.sample_size.min(1000);

        let mut query_futures = Vec::new();
        for (shard_id, shard_pool) in pool.iter_shards() {
            let shard_pool = shard_pool.clone();
            let options_ref = options.clone();
            query_futures.push(async move {
                let mut conn = shard_pool
                    .get()
                    .await
                    .map_err(|e| crate::error::HarvestError::Database(e.to_string()))?;
                let executions = query_running_executions(&mut conn, &options_ref).await?;
                Ok::<_, crate::error::HarvestError>((shard_id, executions))
            });
        }

        let query_results = futures::future::try_join_all(query_futures).await?;

        let mut all_executions = Vec::new();
        for (shard_id, executions) in query_results {
            for (id, name, created, headers, exec_timeout, deadline_at, parent_id, wf_id, queue) in
                executions
            {
                all_executions.push(SampledExecution {
                    shard_id,
                    execution_id: crate::types::ExecutionId::from_uuid(id),
                    workflow_name: name,
                    created_at: created,
                    context_headers: headers,
                    execution_timeout: exec_timeout,
                    deadline_at,
                    // Issue #698: map the nullable `parent_id` column into the
                    // shard-encoding-preserving `ExecutionId`.
                    parent_execution_id: parent_id.map(crate::types::ExecutionId::from_uuid),
                    // Issue #698: the sampled row's business `workflow_id` column.
                    workflow_id: wf_id,
                    // Issue #798: the sampled row's task queue.
                    queue_name: queue,
                });
            }
        }

        all_executions.sort_by(|a, b| {
            b.created_at
                .cmp(&a.created_at)
                .then_with(|| b.execution_id.as_uuid().cmp(&a.execution_id.as_uuid()))
        });

        let total_available = all_executions.len();
        let truncated = total_available > options.sample_size;
        if truncated {
            all_executions.truncate(options.sample_size);
        }

        let semaphore = Arc::new(tokio::sync::Semaphore::new(20));
        let mut replay_futures = Vec::new();
        for exec in all_executions {
            let shard_pool = pool.pool_for(exec.shard_id).clone();
            let replayer_ref = self;
            let sem = Arc::clone(&semaphore);
            replay_futures.push(async move {
                let report = match sem.acquire_owned().await {
                    Ok(_permit) => {
                        let load_and_replay = async {
                            let snapshot = {
                                let mut conn = shard_pool
                                    .get()
                                    .await
                                    .map_err(|e| crate::error::HarvestError::Database(e.to_string()))?;

                                let history = crate::store::load_history(&mut conn, exec.execution_id).await?;
                                let headers = exec.context_headers.clone().and_then(|v| {
                                    serde_json::from_value::<std::collections::HashMap<String, String>>(v)
                                        .map_err(|e| {
                                            tracing::warn!(error = %e, "replay canary: failed to deserialize context headers");
                                            e
                                        })
                                        .ok()
                                });
                                HistorySnapshot {
                                    workflow_name: exec.workflow_name.clone(),
                                    execution_id: exec.execution_id,
                                    events: history.events,
                                    context_headers: headers,
                                    execution_timeout: exec.execution_timeout,
                                    deadline_at: exec.deadline_at,
                                    // Issue #698: the sampled row's own parent id.
                                    parent_execution_id: exec.parent_execution_id,
                                    // Issue #698: the sampled row's own business id.
                                    workflow_id: Some(exec.workflow_id.clone()),
                                    // Issue #798: the sampled row's own task queue.
                                    queue_name: Some(exec.queue_name.clone()),
                                }
                            }; // `conn` is dropped here

                            // Issue #772 / #698 / #798: thread the sampled execution
                            // row's own `execution_timeout` / live `deadline_at` /
                            // parent id / business `workflow_id` / task queue so the
                            // deadline-aware CAN branch and a workflow that branches
                            // on `ctx.info().workflow_id` / `parent_execution_id` /
                            // `ctx.queue_name()` all replay cleanly.
                            let report = replayer_ref
                                .replay_canary_snapshot_effective(
                                    snapshot,
                                    exec.execution_timeout,
                                    exec.deadline_at,
                                    exec.parent_execution_id,
                                    Some(exec.workflow_id.clone()),
                                    Some(exec.queue_name.clone()),
                                )
                                .await;
                            Ok::<_, crate::error::HarvestError>(report)
                        };

                        match load_and_replay.await {
                            Ok(r) => r,
                            Err(e) => ReplayReport {
                                execution_id: exec.execution_id,
                                events_replayed: 0,
                                status: ReplayStatus::WorkflowFailed {
                                    error: format!("canary loading failure: {e}"),
                                    event_index: 0,
                                },
                                mismatched_command_summary: None,
                            },
                        }
                    }
                    Err(e) => ReplayReport {
                        execution_id: exec.execution_id,
                        events_replayed: 0,
                        status: ReplayStatus::WorkflowFailed {
                            error: format!("semaphore acquire failed: {e}"),
                            event_index: 0,
                        },
                        mismatched_command_summary: None,
                    },
                };

                (exec.workflow_name, exec.execution_id, report)
            });
        }

        let replay_results = futures::future::join_all(replay_futures).await;

        let mut sampled = 0;
        let mut replay_succeeded = 0;
        let mut replay_failed = 0;
        let mut details = Vec::new();
        let mut summary_by_type = std::collections::HashMap::new();

        for (wf_name, exec_id, report) in replay_results {
            sampled += 1;
            let type_summary =
                summary_by_type
                    .entry(wf_name.clone())
                    .or_insert_with(|| CanaryTypeSummary {
                        sampled: 0,
                        replay_succeeded: 0,
                        replay_failed: 0,
                    });
            type_summary.sampled += 1;

            match report.status {
                ReplayStatus::ReplaySucceeded => {
                    replay_succeeded += 1;
                    type_summary.replay_succeeded += 1;
                }
                ReplayStatus::NonDeterminismDetected {
                    kind,
                    expected,
                    actual,
                    event_index,
                } => {
                    replay_failed += 1;
                    type_summary.replay_failed += 1;
                    let error = report
                        .mismatched_command_summary
                        .clone()
                        .unwrap_or_else(|| format!("Non-determinism detected: {kind:?}"));
                    details.push(CanaryFailureDetail {
                        execution_id: exec_id,
                        workflow_name: wf_name,
                        kind: Some(kind),
                        expected: Some(expected),
                        actual: Some(actual),
                        event_index: Some(event_index),
                        error,
                    });
                }
                ReplayStatus::WorkflowFailed { error, event_index } => {
                    replay_failed += 1;
                    type_summary.replay_failed += 1;
                    details.push(CanaryFailureDetail {
                        execution_id: exec_id,
                        workflow_name: wf_name,
                        kind: None,
                        expected: None,
                        actual: None,
                        event_index: Some(event_index),
                        error,
                    });
                }
            }
        }

        let verdict = if replay_failed > 0 {
            CanaryVerdict::Fail
        } else {
            CanaryVerdict::Pass
        };

        Ok(ReplayCanaryReport {
            verdict,
            sampled,
            replay_succeeded,
            replay_failed,
            details,
            summary_by_type,
            truncated,
        })
    }
}

#[cfg(feature = "db")]
async fn query_running_executions(
    conn: &mut diesel_async::AsyncPgConnection,
    options: &ReplayCanaryOptions,
) -> crate::error::HarvestResult<
    Vec<(
        uuid::Uuid,
        String,
        chrono::DateTime<chrono::Utc>,
        Option<serde_json::Value>,
        Option<chrono::Duration>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<uuid::Uuid>,
        String,
        String,
    )>,
> {
    use crate::schema::harvest_workflow_executions::dsl::{
        context_headers, created_at, deadline_at, execution_timeout, harvest_workflow_executions,
        id, parent_id, queue_name, state, workflow_id, workflow_name,
    };
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    let mut query = harvest_workflow_executions
        .filter(state.eq("RUNNING".to_string()))
        .into_boxed();

    if let Some(ref wf_name) = options.workflow_name {
        query = query.filter(workflow_name.eq(wf_name.clone()));
    }
    if let Some(ref q_name) = options.queue_name {
        query = query.filter(queue_name.eq(q_name.clone()));
    }

    let rows = query
        .select((
            id,
            workflow_name,
            created_at,
            context_headers,
            // Issue #772: thread the per-execution deadline-aware CAN inputs.
            execution_timeout,
            deadline_at,
            // Issue #698: thread the spawning-parent id.
            parent_id,
            // Issue #698: thread the business `workflow_id`.
            workflow_id,
            // Issue #798: thread the execution's task queue, so a canary replay of
            // a workflow branching on `ctx.queue_name()` does not run under "".
            queue_name,
        ))
        .order((created_at.desc(), id.desc()))
        .limit(i64::try_from(options.sample_size).unwrap_or(i64::MAX))
        .load::<(
            uuid::Uuid,
            String,
            chrono::DateTime<chrono::Utc>,
            Option<serde_json::Value>,
            Option<chrono::Duration>,
            Option<chrono::DateTime<chrono::Utc>>,
            Option<uuid::Uuid>,
            String,
            String,
        )>(conn)
        .await
        .map_err(crate::error::database_error)?;

    Ok(rows)
}

// ---------------------------------------------------------------------------
// ReplayCanary (db feature only)
// ---------------------------------------------------------------------------

/// Options for running the pre-deploy replay canary.
#[cfg(feature = "db")]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ReplayCanaryOptions {
    /// Maximum number of running workflow executions to sample.
    pub sample_size: usize,
    /// Optional filter to limit samples to a specific workflow type.
    pub workflow_name: Option<String>,
    /// Optional filter to limit samples to a specific task queue.
    pub queue_name: Option<String>,
}

#[cfg(feature = "db")]
impl Default for ReplayCanaryOptions {
    fn default() -> Self {
        Self {
            sample_size: 500,
            workflow_name: None,
            queue_name: None,
        }
    }
}

/// The overall pass/fail verdict of the replay canary.
#[cfg(feature = "db")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanaryVerdict {
    /// All sampled executions successfully replayed without non-determinism.
    Pass,
    /// One or more executions failed to replay.
    Fail,
}

/// Detailed information about a failed replay canary execution.
#[cfg(feature = "db")]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CanaryFailureDetail {
    /// The ID of the failed execution.
    pub execution_id: ExecutionId,
    /// The workflow type name.
    pub workflow_name: String,
    /// The kind of non-determinism detected (or None if it was a general execution failure).
    pub kind: Option<NonDeterminismKind>,
    /// Expected command string.
    pub expected: Option<String>,
    /// Actual command string.
    pub actual: Option<String>,
    /// Index of the event where the failure occurred.
    pub event_index: Option<usize>,
    /// The error message.
    pub error: String,
}

/// Aggregated counts by workflow type.
#[cfg(feature = "db")]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CanaryTypeSummary {
    /// Number of runs sampled of this type.
    pub sampled: usize,
    /// Number of runs of this type that replayed successfully.
    pub replay_succeeded: usize,
    /// Number of runs of this type that failed to replay.
    pub replay_failed: usize,
}

/// Complete report returned by the replay canary.
#[cfg(feature = "db")]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ReplayCanaryReport {
    /// Overall verdict (pass if 100% of samples succeed).
    pub verdict: CanaryVerdict,
    /// Total number of executions sampled across shards.
    pub sampled: usize,
    /// Number of executions that replayed successfully.
    pub replay_succeeded: usize,
    /// Number of executions that failed to replay.
    pub replay_failed: usize,
    /// Failure details (empty on Pass).
    pub details: Vec<CanaryFailureDetail>,
    /// Aggregated summary by workflow type.
    pub summary_by_type: std::collections::HashMap<String, CanaryTypeSummary>,
    /// Whether more running executions were available than the sample size.
    pub truncated: bool,
}

// ---------------------------------------------------------------------------
// DB helper (db feature only)
// ---------------------------------------------------------------------------

/// Loaded per-execution replay metadata for [`WorkflowReplayer::replay_from_db`]
/// (issue #772): workflow name, context headers, and the deadline-aware
/// continue-as-new inputs — the row's own `execution_timeout` and live
/// (pause/resume/redrive-shifted) `deadline_at`.
#[cfg(feature = "db")]
struct DbReplayMeta {
    workflow_name: String,
    headers: HashMap<String, String>,
    execution_timeout: Option<chrono::Duration>,
    deadline_at: Option<chrono::DateTime<chrono::Utc>>,
    // Issue #698: the row's spawning-parent id (`parent_id` column), so
    // `replay_from_db` replays a parent-aware child deterministically.
    parent_execution_id: Option<ExecutionId>,
    // Issue #698: the row's business `workflow_id` column, so `replay_from_db`
    // reports the real `ctx.info().workflow_id`.
    workflow_id: String,
    // Issue #798: the row's `queue_name` column, so `replay_from_db` reports the
    // real `ctx.queue_name()` instead of "".
    queue_name: String,
}

#[cfg(feature = "db")]
// The Diesel `.first()` binding annotation is a flat 5-tuple of column types
// (issue #698 added the nullable `parent_id`); it is a one-shot select, not a
// reused shape, so a `type` alias would obscure rather than clarify.
#[allow(clippy::type_complexity)]
async fn load_workflow_name_and_headers(
    conn: &mut diesel_async::AsyncPgConnection,
    exec_id: ExecutionId,
) -> crate::error::HarvestResult<DbReplayMeta> {
    use crate::error::{HarvestError, database_error};
    use crate::schema::harvest_workflow_executions::dsl::{
        context_headers as context_headers_col, deadline_at as deadline_at_col,
        execution_timeout as execution_timeout_col, harvest_workflow_executions, id as id_col,
        parent_id as parent_id_col, queue_name as queue_name_col, workflow_id as workflow_id_col,
        workflow_name,
    };
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    let exec_uuid = exec_id.as_uuid();

    let (name, raw_headers, execution_timeout, deadline_at, parent_uuid, workflow_id, queue_name): (
        String,
        Option<serde_json::Value>,
        Option<chrono::Duration>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<uuid::Uuid>,
        String,
        String,
    ) = harvest_workflow_executions
        .filter(id_col.eq(exec_uuid))
        .select((
            workflow_name,
            context_headers_col,
            execution_timeout_col,
            deadline_at_col,
            parent_id_col,
            workflow_id_col,
            // Issue #798: the row's task queue.
            queue_name_col,
        ))
        .first(conn)
        .await
        .map_err(|e| match e {
            diesel::result::Error::NotFound => HarvestError::NotFound(exec_id.to_string()),
            other => database_error(other),
        })?;

    let headers = raw_headers
        .and_then(|v| {
            serde_json::from_value::<std::collections::HashMap<String, String>>(v)
                .map_err(|e| {
                    tracing::warn!(error = %e, "replay_from_db: failed to deserialize context headers");
                    e
                })
                .ok()
        })
        .unwrap_or_default();

    Ok(DbReplayMeta {
        workflow_name: name,
        headers,
        execution_timeout,
        deadline_at,
        // Issue #698: map the nullable `parent_id` into an `ExecutionId`.
        parent_execution_id: parent_uuid.map(ExecutionId::from_uuid),
        // Issue #698: the row's business `workflow_id`.
        workflow_id,
        // Issue #798: the row's task queue.
        queue_name,
    })
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Extract the workflow input from the `WorkflowStarted` event.
fn extract_input(events: &[WorkflowEvent]) -> Value {
    events
        .first()
        .and_then(|e| match e {
            WorkflowEvent::WorkflowStarted { input, .. } => Some(input.clone()),
            _ => None,
        })
        .unwrap_or(Value::Null)
}

/// Convert a `WorkflowOutcome` into a `ReplayReport`.
fn outcome_to_report(
    exec_id: ExecutionId,
    total_events: usize,
    outcome: WorkflowOutcome,
    canary_mode: bool,
) -> ReplayReport {
    match outcome {
        WorkflowOutcome::Completed { .. } | WorkflowOutcome::ContinuedAsNew { .. } => {
            ReplayReport {
                execution_id: exec_id,
                events_replayed: total_events,
                status: ReplayStatus::ReplaySucceeded,
                mismatched_command_summary: None,
            }
        }

        // Suspension during strict replay means the workflow tried to issue a
        // new command with no matching history event (the oneshot is never
        // resolved in replay mode, so the 100 ms timeout fires).
        WorkflowOutcome::Suspended { .. } => {
            if canary_mode {
                ReplayReport {
                    execution_id: exec_id,
                    events_replayed: total_events,
                    status: ReplayStatus::ReplaySucceeded,
                    mismatched_command_summary: None,
                }
            } else {
                ReplayReport {
                    execution_id: exec_id,
                    events_replayed: total_events,
                    status: ReplayStatus::NonDeterminismDetected {
                        kind: NonDeterminismKind::Unknown,
                        expected: "<workflow to complete replay>".to_string(),
                        actual: "<workflow suspended — issued new command with no matching history event>"
                            .to_string(),
                        event_index: total_events,
                    },
                    mismatched_command_summary: Some(
                        "workflow suspended during replay (new command beyond recorded history)"
                            .to_string(),
                    ),
                }
            }
        }

        WorkflowOutcome::Failed {
            error,
            non_deterministic_details,
            ..
        } => try_parse_non_determinism(
            &error,
            exec_id,
            total_events,
            non_deterministic_details.as_ref(),
        )
        .unwrap_or(ReplayReport {
            execution_id: exec_id,
            events_replayed: total_events,
            status: ReplayStatus::WorkflowFailed {
                error,
                event_index: total_events,
            },
            mismatched_command_summary: None,
        }),
    }
}

/// Attempt to parse a `HarvestError::NonDeterministic` formatted error string
/// into a structured `ReplayReport`.  Returns `None` if the error is not a
/// non-determinism error.
fn try_parse_non_determinism(
    error: &str,
    exec_id: ExecutionId,
    event_index_fallback: usize,
    details: Option<&crate::error::NonDeterministicDetails>,
) -> Option<ReplayReport> {
    // HarvestError::NonDeterministic formats as "non-deterministic replay: {msg}"
    let msg = error.strip_prefix("non-deterministic replay: ")?;

    let (kind, expected, actual, event_index) = details.map_or_else(
        || {
            let (kind, expected, actual) = parse_nd_message(msg);
            (kind, expected, actual, event_index_fallback)
        },
        |d| {
            let (kind, _, _) = parse_nd_message(msg);
            (
                kind,
                d.expected.clone().unwrap_or_default(),
                d.actual.clone().unwrap_or_default(),
                d.event_index
                    .and_then(|idx| usize::try_from(idx).ok())
                    .unwrap_or(event_index_fallback),
            )
        },
    );

    let summary = format!("expected \"{expected}\", got \"{actual}\"");

    Some(ReplayReport {
        execution_id: exec_id,
        events_replayed: event_index,
        status: ReplayStatus::NonDeterminismDetected {
            kind,
            expected,
            actual,
            event_index,
        },
        mismatched_command_summary: Some(summary),
    })
}

/// Parse `"{kind} mismatch: expected {expected}, got {actual}"` into its parts.
fn parse_nd_message(msg: &str) -> (NonDeterminismKind, String, String) {
    // Common format: "X mismatch: expected Y, got Z"
    if let Some((kind_str, rest)) = msg.split_once(" mismatch: ")
        && let Some((exp_part, actual)) = rest.split_once(", got ")
    {
        let expected = exp_part
            .strip_prefix("expected ")
            .unwrap_or(exp_part)
            .to_string();
        let actual = actual.to_string();
        let kind = classify_kind(kind_str, &actual);
        return (kind, expected, actual);
    }
    // Fallback for unusual formats (e.g. "signal history contains unexpected failure")
    (NonDeterminismKind::Unknown, msg.to_string(), String::new())
}

/// Classify a non-determinism error into a [`NonDeterminismKind`].
///
/// `kind_str` is the prefix before `" mismatch:"` in the error message.
/// `actual` is the event type / name that was actually found at the cursor.
/// If `actual` names a `version:…` marker the cause is a renamed version gate,
/// which is always classified as [`NonDeterminismKind::VersionMarkerMismatch`]
/// regardless of which command kind triggered the mismatch. A `patch:…`
/// marker is likewise always classified as
/// [`NonDeterminismKind::PatchMarkerMismatch`] (issue #687).
fn classify_kind(kind_str: &str, actual: &str) -> NonDeterminismKind {
    // A version marker found where another event was expected means the version
    // gate's change_id was renamed — classify specifically so error messages
    // point at the version gate rather than the command that first noticed it.
    if actual.starts_with("MarkerRecorded(version:") {
        return NonDeterminismKind::VersionMarkerMismatch;
    }
    // A patch marker found where another event was expected means the
    // `patched()` call was removed/renamed before all marker-bearing
    // executions drained (issue #687) — classify specifically so error
    // messages point at the patch gate rather than the command that first
    // noticed it.
    if actual.starts_with("MarkerRecorded(patch:") {
        return NonDeterminismKind::PatchMarkerMismatch;
    }
    // A `TimerCancelled` found where another event was expected means a
    // `cancel_timer` / `handle.cancel` / `handle.reset` call was removed before
    // all cancel-bearing executions drained (issue #768) — classify specifically
    // so the message points at the cancelled timer rather than the command that
    // first noticed it.
    if actual.starts_with("TimerCancelled") {
        return NonDeterminismKind::TimerCancelMismatch;
    }
    // A `MutexGranted` found where another event was expected (or an acquire of
    // a different key) means a durable-mutex acquire (issue #691) diverged from
    // history — classify specifically so the message points at the mutex acquire.
    if actual.starts_with("MutexGranted") {
        return NonDeterminismKind::MutexGrantMismatch;
    }
    match kind_str {
        "activity" => NonDeterminismKind::ActivityScheduleMismatch,
        "timer-cancel" => NonDeterminismKind::TimerCancelMismatch,
        "local activity" => NonDeterminismKind::LocalActivityScheduleMismatch,
        "timer" => NonDeterminismKind::TimerMismatch,
        "signal" => NonDeterminismKind::SignalMismatch,
        "child workflow" => NonDeterminismKind::ChildWorkflowMismatch,
        "side effect" => NonDeterminismKind::SideEffectMismatch,
        "side-effect drift" => NonDeterminismKind::SideEffectDrift,
        "external activity" => NonDeterminismKind::ExternalActivityMismatch,
        "external signal" => NonDeterminismKind::ExternalSignalMismatch,
        "external await" => NonDeterminismKind::ExternalAwaitMismatch,
        s if s.contains("continue") => NonDeterminismKind::ContinueAsNewMismatch,
        "early completion" => NonDeterminismKind::EarlyCompletion,
        _ => NonDeterminismKind::Unknown,
    }
}

// ===========================================================================
// ReplayVerifier  — batch CI replay gate (issue #251)
// ===========================================================================

/// Category of non-determinism failure or harness error for a single fixture.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "kind")]
pub enum FixtureStatus {
    /// Replay completed without divergence.
    Passed,
    /// Replay detected non-determinism or the workflow function returned an error.
    Failed(ReplayStatus),
    /// The fixture could not be loaded or the workflow name has no registered handler.
    HarnessError(HarnessErrorKind),
    /// Workflow name has no handler but `allow_unregistered = true` — treated as a warning.
    Skipped { reason: String },
}

/// Reason a fixture could not be replayed (harness-side, not replay-side).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub enum HarnessErrorKind {
    /// The fixture's `workflow_name` is not registered in this verifier.
    UnregisteredWorkflow,
    /// The fixture file could not be read or is not valid [`HistorySnapshot`] JSON.
    InvalidFixture(String),
    /// The replay exceeded the per-fixture timeout.
    Timeout,
}

impl std::fmt::Display for HarnessErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnregisteredWorkflow => write!(f, "UnregisteredWorkflow"),
            Self::InvalidFixture(msg) => write!(f, "InvalidFixture({msg})"),
            Self::Timeout => write!(f, "Timeout"),
        }
    }
}

/// Result of replaying one fixture file.
#[derive(Debug, Clone, serde::Serialize)]
pub struct FixtureResult {
    /// Path to the source fixture file.
    #[serde(serialize_with = "serialize_path")]
    pub path: std::path::PathBuf,
    /// Workflow name from the fixture (empty string if the file was unparseable).
    pub workflow_name: String,
    /// Execution ID from the fixture (`None` if the file was unparseable).
    pub execution_id: Option<ExecutionId>,
    /// Outcome of this fixture replay.
    pub status: FixtureStatus,
}

#[allow(clippy::ptr_arg)] // serde requires &FieldType; &PathBuf cannot be replaced by &Path here
fn serialize_path<S: serde::Serializer>(
    path: &std::path::PathBuf,
    s: S,
) -> Result<S::Ok, S::Error> {
    s.serialize_str(&path.to_string_lossy())
}

/// Aggregate report returned by [`ReplayVerifier::verify_dir`] /
/// [`ReplayVerifier::verify_all`].
#[derive(Debug, Clone, serde::Serialize)]
pub struct BatchReplayReport {
    /// Total number of `.json` fixture files discovered.
    pub fixtures_total: usize,
    /// Number of fixtures that replayed without divergence.
    pub succeeded: usize,
    /// Number of fixtures that failed replay (non-determinism or workflow error).
    pub failed: usize,
    /// Number of fixtures that could not be processed (invalid JSON, no handler).
    pub harness_errors: usize,
    /// Number of fixtures skipped because `allow_unregistered = true`.
    pub skipped: usize,
    /// Per-fixture results in file-path order.
    pub results: Vec<FixtureResult>,
}

impl BatchReplayReport {
    /// Wrap in a [`CiReport`] with the default [`FailOnMode::Any`] exit-code policy.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn into_ci_report(self) -> CiReport {
        CiReport {
            report: self,
            fail_on: FailOnMode::Any,
        }
    }

    /// Wrap in a [`CiReport`] with a pass-rate threshold exit-code policy.
    ///
    /// `threshold` is a fraction in `[0.0, 1.0]`. Exit code 1 is returned
    /// only when the fraction of succeeded fixtures falls below `threshold`.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn into_ci_report_with_threshold(self, threshold: f64) -> CiReport {
        CiReport {
            report: self,
            fail_on: FailOnMode::Rate(threshold),
        }
    }
}

// ---------------------------------------------------------------------------
// CiReport, FailOnMode, ReportFormat
// ---------------------------------------------------------------------------

/// Controls when [`CiReport::exit_code`] returns `1`.
#[derive(Debug, Clone)]
pub enum FailOnMode {
    /// Exit `1` if any fixture fails (default).
    Any,
    /// Exit `1` if the pass rate (`succeeded / fixtures_total`) is below this fraction.
    Rate(f64),
}

/// Output format for [`CiReport::format_report`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportFormat {
    /// Human-readable summary printed to a string (default).
    Text,
    #[allow(clippy::doc_markdown)] // JUnit is a proper name, not a code item
    /// JUnit XML with one `<testcase>` per fixture.
    JUnit,
    /// Structured JSON serialization of [`BatchReplayReport`].
    Json,
    /// GitHub Actions `::error file=…` annotations, one per failed/errored fixture.
    GitHub,
}

/// CI-shaped wrapper around a [`BatchReplayReport`] that computes exit codes
/// and formats output for various CI systems.
pub struct CiReport {
    /// The underlying batch report.
    pub report: BatchReplayReport,
    fail_on: FailOnMode,
}

impl CiReport {
    /// Compute the process exit code.
    ///
    /// - `0` — every fixture replayed cleanly (or skipped when `allow_unregistered = true`).
    /// - `1` — one or more replay failures (subject to [`FailOnMode`]).
    /// - `2` — one or more harness errors (dominates over replay failures).
    #[must_use]
    #[allow(clippy::cast_precision_loss)] // fixture counts fit comfortably in f64 mantissa
    pub fn exit_code(&self) -> i32 {
        if self.report.harness_errors > 0 {
            return 2;
        }
        match &self.fail_on {
            FailOnMode::Any => i32::from(self.report.failed > 0),
            FailOnMode::Rate(threshold) => {
                let attempted = self.report.succeeded + self.report.failed;
                if attempted == 0 {
                    return 0;
                }
                let pass_rate = self.report.succeeded as f64 / attempted as f64;
                i32::from(pass_rate < *threshold)
            }
        }
    }

    /// Override the exit-code policy after construction.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn with_fail_on(mut self, mode: FailOnMode) -> Self {
        self.fail_on = mode;
        self
    }

    /// Render the report in the requested format as a `String`.
    #[must_use]
    pub fn format_report(&self, format: ReportFormat) -> String {
        match format {
            ReportFormat::Text => self.format_text(),
            ReportFormat::JUnit => self.format_junit(),
            ReportFormat::Json => self.format_json(),
            ReportFormat::GitHub => self.format_github(),
        }
    }

    fn format_text(&self) -> String {
        use std::fmt::Write as FmtWrite;
        let r = &self.report;
        let mut out = String::new();
        let _ = writeln!(
            out,
            "harvest replay-verify: {} fixture(s) total — {} PASS, {} FAIL, {} error(s), {} skipped",
            r.fixtures_total, r.succeeded, r.failed, r.harness_errors, r.skipped,
        );
        for result in &r.results {
            let file = result.path.file_name().map_or_else(
                || result.path.to_string_lossy().into_owned(),
                |n| n.to_string_lossy().into_owned(),
            );
            match &result.status {
                FixtureStatus::Passed => {
                    let _ = writeln!(out, "  PASS  {file} ({})", result.workflow_name);
                }
                FixtureStatus::Failed(ReplayStatus::NonDeterminismDetected {
                    kind,
                    expected,
                    actual,
                    event_index,
                }) => {
                    let _ = writeln!(
                        out,
                        "  FAIL  {file} ({}) — {kind} at event {event_index}: expected \"{expected}\", got \"{actual}\"",
                        result.workflow_name,
                    );
                }
                FixtureStatus::Failed(ReplayStatus::WorkflowFailed { error, .. }) => {
                    let _ = writeln!(
                        out,
                        "  FAIL  {file} ({}) — workflow error: {error}",
                        result.workflow_name,
                    );
                }
                FixtureStatus::Failed(ReplayStatus::ReplaySucceeded) => {
                    let _ = writeln!(
                        out,
                        "  FAIL  {file} ({}) — unexpected ReplaySucceeded",
                        result.workflow_name,
                    );
                }
                FixtureStatus::HarnessError(kind) => {
                    let _ = writeln!(
                        out,
                        "  ERR   {file} ({}) — harness error: {kind}",
                        result.workflow_name,
                    );
                }
                FixtureStatus::Skipped { reason } => {
                    let _ = writeln!(out, "  SKIP  {file} ({}) — {reason}", result.workflow_name);
                }
            }
        }
        out
    }

    fn format_junit(&self) -> String {
        use std::fmt::Write as FmtWrite;
        let r = &self.report;
        let mut out = String::new();
        out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        let _ = writeln!(
            out,
            "<testsuite name=\"harvest-replay-verify\" tests=\"{}\" failures=\"{}\" errors=\"{}\" skipped=\"{}\">",
            r.fixtures_total, r.failed, r.harness_errors, r.skipped,
        );
        for result in &r.results {
            let file = xml_escape(&result.path.file_name().map_or_else(
                || result.path.to_string_lossy().into_owned(),
                |n| n.to_string_lossy().into_owned(),
            ));
            let classname = xml_escape(&result.workflow_name);
            let _ = writeln!(
                out,
                "  <testcase name=\"{file}\" classname=\"{classname}\">"
            );
            match &result.status {
                FixtureStatus::Passed
                | FixtureStatus::Skipped { .. }
                | FixtureStatus::Failed(ReplayStatus::ReplaySucceeded) => {}
                FixtureStatus::Failed(ReplayStatus::NonDeterminismDetected {
                    kind,
                    expected,
                    actual,
                    event_index,
                }) => {
                    let _ = writeln!(
                        out,
                        "    <failure message=\"{kind}\" type=\"NonDeterminismDetected\">"
                    );
                    let _ = writeln!(
                        out,
                        "      {}",
                        xml_escape(&format!(
                            "kind={kind}, expected={expected:?}, actual={actual:?}, event_index={event_index}"
                        ))
                    );
                    out.push_str("    </failure>\n");
                }
                FixtureStatus::Failed(ReplayStatus::WorkflowFailed { error, .. }) => {
                    let escaped = xml_escape(error);
                    let _ = writeln!(
                        out,
                        "    <failure message=\"WorkflowFailed\" type=\"WorkflowFailed\">\n      {escaped}\n    </failure>"
                    );
                }
                FixtureStatus::HarnessError(kind) => {
                    let msg = xml_escape(&kind.to_string());
                    let detail = match kind {
                        HarnessErrorKind::UnregisteredWorkflow => format!(
                            "workflow '{}' not registered in this verifier",
                            result.workflow_name
                        ),
                        HarnessErrorKind::InvalidFixture(e) => e.clone(),
                        HarnessErrorKind::Timeout => "replay timed out".to_string(),
                    };
                    let detail = xml_escape(&detail);
                    let _ = writeln!(
                        out,
                        "    <error message=\"{msg}\" type=\"HarnessError\">\n      {detail}\n    </error>"
                    );
                }
            }
            out.push_str("  </testcase>\n");
        }
        out.push_str("</testsuite>\n");
        out
    }

    fn format_json(&self) -> String {
        serde_json::to_string_pretty(&self.report)
            .unwrap_or_else(|e| format!("{{\"error\": \"serialization failed: {e}\"}}"))
    }

    fn format_github(&self) -> String {
        use std::fmt::Write as FmtWrite;
        let mut out = String::new();
        for result in &self.report.results {
            // GitHub command properties are comma/colon-delimited; encode those too.
            let file = github_escape(&result.path.to_string_lossy());
            match &result.status {
                FixtureStatus::Passed
                | FixtureStatus::Skipped { .. }
                | FixtureStatus::Failed(ReplayStatus::ReplaySucceeded) => {}
                FixtureStatus::Failed(ReplayStatus::NonDeterminismDetected {
                    kind,
                    expected,
                    actual,
                    event_index,
                }) => {
                    let title = github_escape(&kind.to_string());
                    let msg = github_escape(&format!(
                        "{kind} at event {event_index}: expected \"{expected}\", got \"{actual}\""
                    ));
                    let _ = writeln!(out, "::error file={file},title={title}::{msg}");
                }
                FixtureStatus::Failed(ReplayStatus::WorkflowFailed { error, .. }) => {
                    let msg = github_escape(&format!("workflow error: {error}"));
                    let _ = writeln!(out, "::error file={file},title=WorkflowFailed::{msg}");
                }
                FixtureStatus::HarnessError(kind) => {
                    let msg = github_escape(&kind.to_string());
                    let _ = writeln!(out, "::error file={file},title=HarnessError::{msg}");
                }
            }
        }
        out
    }
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Escape a string for use in GitHub Actions workflow commands.
///
/// The `file=` and `title=` properties are comma-and-colon-delimited; the
/// message body treats `%`, `\r`, and `\n` as special. Encoding all five keeps
/// annotations well-formed regardless of fixture path or error content.
fn github_escape(s: &str) -> String {
    s.replace('%', "%25")
        .replace('\r', "%0D")
        .replace('\n', "%0A")
        .replace(',', "%2C")
        .replace(':', "%3A")
}

// ---------------------------------------------------------------------------
// ReplayDriftReport  — in-flight replay-drift gate (issue #798)
// ---------------------------------------------------------------------------

/// One fixture whose replay diverged from its recorded history.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ReplayDrift {
    /// The execution whose history diverged.
    pub execution_id: Option<ExecutionId>,
    /// The registered workflow type the history belongs to.
    pub workflow_name: String,
    /// The category of divergence.
    ///
    /// A workflow function that returned an error during replay (rather than
    /// diverging on a command) is reported as [`NonDeterminismKind::Unknown`]
    /// with a `workflow error:` prefixed [`first_divergence`](Self::first_divergence);
    /// it is still a gate failure, because the candidate code could not replay
    /// that history.
    pub kind: NonDeterminismKind,
    /// Human-readable description of the first divergence encountered.
    pub first_divergence: String,
    /// The fixture file the divergence came from, so an operator can open it.
    #[serde(serialize_with = "serialize_path")]
    pub fixture_path: std::path::PathBuf,
}

/// One workflow type whose fixtures in the bundle disagree with what the
/// manifest says the export wrote.
///
/// Produced by
/// [`ReplayDriftReport::bundle_inventory_mismatches`](ReplayDriftReport::bundle_inventory_mismatches).
#[derive(Debug, Clone, serde::Serialize)]
pub struct BundleInventoryMismatch {
    /// The workflow type the disagreement is about.
    pub workflow_name: String,
    /// How many fixtures of this type the manifest says were exported.
    pub declared: u64,
    /// How many fixtures of this type the bundle actually holds.
    pub found: usize,
    /// Executions the manifest says were exported but which the bundle does not
    /// contain — histories the gate silently never replayed.
    ///
    /// Empty when the manifest carries no identities (a pre-identity export),
    /// in which case only the counts were comparable.
    pub missing_execution_ids: Vec<ExecutionId>,
    /// Executions present in the bundle that the manifest does not list — the
    /// directory is not the sample the manifest describes.
    pub unexpected_execution_ids: Vec<ExecutionId>,
}

/// One fixture that could not be replayed at all (a harness-side problem, not a
/// determinism verdict).
#[derive(Debug, Clone, serde::Serialize)]
pub struct ReplayBlocked {
    /// The fixture file that could not be processed.
    #[serde(serialize_with = "serialize_path")]
    pub fixture_path: std::path::PathBuf,
    /// Workflow name from the fixture (empty when the file was unparseable).
    pub workflow_name: String,
    /// Execution ID from the fixture (`None` when the file was unparseable).
    pub execution_id: Option<ExecutionId>,
    /// Why the fixture could not be replayed.
    pub reason: HarnessErrorKind,
}

/// Aggregate result of replaying an exported in-flight history bundle
/// (issue #798), shaped for a CI release gate.
///
/// Produced by [`ReplayVerifier::replay_bundle`] and
/// [`WorkflowReplayer::replay_bundle`].
///
/// # Reading the verdict
///
/// Use [`exit_code`](Self::exit_code) (or [`is_clean`](Self::is_clean)) rather
/// than checking `diverged.is_empty()` by hand — the latter reports "no drift"
/// for a bundle that contained no fixtures at all, which is the one failure
/// mode a release gate must never have.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ReplayDriftReport {
    /// Total fixtures discovered in the bundle (excluding the manifest).
    pub total: usize,
    /// Fixtures that replayed with no divergence.
    pub succeeded: usize,
    /// Fixtures whose replay diverged from the recorded history.
    pub diverged: Vec<ReplayDrift>,
    /// Fixtures that could not be replayed (unparseable, unregistered, timeout).
    pub blocked: Vec<ReplayBlocked>,
    /// Fixtures skipped because `allow_unregistered` was set.
    pub skipped: usize,
    /// What the bundle actually held, per workflow type: the execution ids of
    /// every fixture discovered, whatever its replay outcome.
    ///
    /// The bundle-side half of the manifest reconciliation. Recorded for every
    /// fixture — passed, diverged, skipped, blocked — because presence in the
    /// directory is the question, not verdict: a fixture that failed to replay
    /// was still delivered, and counting only the successes would report every
    /// divergence as *also* a missing file.
    ///
    /// A fixture too malformed to yield an execution id contributes nothing
    /// here; it is already a rung-`2` block in its own right.
    pub found_execution_ids: std::collections::BTreeMap<String, Vec<ExecutionId>>,
    /// Coverage claimed by the bundle's manifest, when it has one.
    pub coverage: Option<crate::replay_sample::SampleManifest>,
    /// Why the bundle's manifest could not be read, when it was present but
    /// unreadable.
    ///
    /// `None` covers both a well-formed manifest and a legitimately absent one —
    /// see [`BundleManifest`] for why those two share a verdict and this one does
    /// not. When set, the gate blocks at rung `2`: every manifest-derived guard
    /// is unenforceable, so the gate cannot claim to have fully run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest_unreadable: Option<String>,
    /// Whether an empty bundle was explicitly permitted.
    pub allow_empty_bundle: bool,
    /// Whether incomplete cross-shard coverage blocks the gate.
    pub require_complete_coverage: bool,
}

impl ReplayDriftReport {
    /// Project a [`BatchReplayReport`] into the drift-gate shape.
    fn from_batch(
        batch: BatchReplayReport,
        manifest: BundleManifest,
        allow_empty_bundle: bool,
        require_complete_coverage: bool,
    ) -> Self {
        let (coverage, manifest_unreadable) = match manifest {
            BundleManifest::Present(m) => (Some(*m), None),
            BundleManifest::Absent => (None, None),
            BundleManifest::Unreadable(reason) => (None, Some(reason)),
        };
        let mut diverged = Vec::new();
        let mut blocked = Vec::new();
        let mut found_execution_ids: std::collections::BTreeMap<String, Vec<ExecutionId>> =
            std::collections::BTreeMap::new();

        for result in batch.results {
            // Inventory first, and for every status: the question this answers
            // is "was this history delivered?", which a divergence does not
            // change. Recorded before the match so no future arm can forget it.
            if let Some(execution_id) = result.execution_id {
                found_execution_ids
                    .entry(result.workflow_name.clone())
                    .or_default()
                    .push(execution_id);
            }
            match result.status {
                FixtureStatus::Passed | FixtureStatus::Skipped { .. } => {}
                FixtureStatus::Failed(ReplayStatus::NonDeterminismDetected {
                    kind,
                    expected,
                    actual,
                    event_index,
                }) => diverged.push(ReplayDrift {
                    execution_id: result.execution_id,
                    workflow_name: result.workflow_name,
                    kind,
                    first_divergence: format!(
                        "at event {event_index}: expected {expected}, got {actual}"
                    ),
                    fixture_path: result.path,
                }),
                // A workflow function that errored during replay is still a gate
                // failure — the candidate code could not replay that history —
                // but it is not a command-sequence divergence, so it carries the
                // `Unknown` kind and a distinguishable message.
                FixtureStatus::Failed(ReplayStatus::WorkflowFailed { error, event_index }) => {
                    diverged.push(ReplayDrift {
                        execution_id: result.execution_id,
                        workflow_name: result.workflow_name,
                        kind: NonDeterminismKind::Unknown,
                        first_divergence: format!(
                            "workflow error: at event {event_index}: {error}"
                        ),
                        fixture_path: result.path,
                    });
                }
                FixtureStatus::Failed(ReplayStatus::ReplaySucceeded) => {
                    // Unreachable: `replay_fixture_file` only wraps non-success
                    // statuses in `Failed`. Counted as success rather than
                    // silently dropped so the totals always reconcile.
                    debug_assert!(false, "ReplaySucceeded must not be wrapped in Failed");
                }
                FixtureStatus::HarnessError(reason) => blocked.push(ReplayBlocked {
                    fixture_path: result.path,
                    workflow_name: result.workflow_name,
                    execution_id: result.execution_id,
                    reason,
                }),
            }
        }

        for ids in found_execution_ids.values_mut() {
            ids.sort_unstable_by_key(ExecutionId::as_uuid);
        }

        Self {
            total: batch.fixtures_total,
            succeeded: batch.succeeded,
            diverged,
            blocked,
            skipped: batch.skipped,
            found_execution_ids,
            coverage,
            manifest_unreadable,
            allow_empty_bundle,
            require_complete_coverage,
        }
    }

    /// Whether the bundle contained no fixtures.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.total == 0
    }

    /// Whether the run replayed **no workflow at all**, whatever the reason.
    ///
    /// Broader than [`is_empty`](Self::is_empty): a bundle whose every fixture
    /// was *skipped* (`allow_unregistered`, a gate binary wired to the wrong
    /// bundle, or one that registered none of the sampled types) has
    /// `total > 0` yet verified exactly as much as an empty directory did —
    /// nothing. Certifying a release on that is the same false pass, so both
    /// share the empty-bundle rung and the same `allow_empty_bundle` opt-out.
    #[must_use]
    pub const fn verified_nothing(&self) -> bool {
        self.succeeded == 0 && self.diverged.is_empty() && self.blocked.is_empty()
    }

    /// Whether the manifest declares more fixtures than the bundle contains.
    ///
    /// The manifest is the export's own record of what it wrote. If fixtures go
    /// missing in transit — a truncated CI artifact upload, a partial copy, a
    /// `.gitignore`d subdirectory — every surviving fixture can replay cleanly
    /// while the gate silently certifies a strict subset of the sample. Neither
    /// the divergence rung nor `require_complete_coverage` catches that: the
    /// manifest's own shard status still reads `complete`, because the *export*
    /// was complete; it was the *bundle* that lost files afterwards.
    ///
    /// A surplus counts too: an unexpected extra fixture means the directory is
    /// not the bundle the manifest describes, so its verdict is not about the
    /// sample the operator thinks they took.
    #[must_use]
    pub fn fixture_count_disagrees_with_manifest(&self) -> bool {
        self.coverage.as_ref().is_some_and(|manifest| {
            // A `sampled_total` that does not even fit a `usize` cannot equal
            // the count of files actually walked, so a conversion failure is
            // itself a disagreement rather than a reason to stay silent.
            !usize::try_from(manifest.sampled_total).is_ok_and(|declared| declared == self.total)
        })
    }

    /// Per-workflow-type disagreements between the bundle and the manifest's
    /// inventory.
    ///
    /// [`fixture_count_disagrees_with_manifest`](Self::fixture_count_disagrees_with_manifest)
    /// compares one number against one number, so it accepts any corruption
    /// that preserves the *total*. Lose a fixture for workflow `A` while the
    /// artifact gains a duplicate for workflow `B` and the totals still match:
    /// both `B` fixtures replay cleanly, the gate exits `0`, and `A` was never
    /// verified at all. That is the same silent false pass the aggregate check
    /// exists to prevent, one level down.
    ///
    /// So this reconciles at the two finer grains the manifest supports:
    ///
    /// * **Per type** — `found` against `declared`, catching a substitution
    ///   across types that the total hides.
    /// * **Per execution** — the exact ids, catching a substitution *within* a
    ///   type, which per-type counts hide in turn. A bundle that swaps one
    ///   execution of `A` for a duplicate of another `A` matches on every
    ///   count there is; only identity separates it from the real sample.
    ///
    /// The identity comparison is skipped for a manifest that carries no
    /// [`sampled_execution_ids`](crate::replay_sample::SampleWorkflowCoverage::sampled_execution_ids)
    /// — a bundle from a pre-identity export makes *no claim* about which
    /// executions it holds, and reading that as a claim of *none* would report
    /// every fixture as unexpected on every such bundle. Those degrade to the
    /// per-type count check, which is still strictly stronger than the total.
    ///
    /// A type present in the bundle but absent from the manifest is reported
    /// with `declared: 0`, and one in the manifest but absent from the bundle
    /// with `found: 0`; neither can be expressed by comparing only the types
    /// the two happen to share.
    #[must_use]
    pub fn bundle_inventory_mismatches(&self) -> Vec<BundleInventoryMismatch> {
        let Some(manifest) = self.coverage.as_ref() else {
            return Vec::new();
        };
        let declared: std::collections::BTreeMap<
            &str,
            &crate::replay_sample::SampleWorkflowCoverage,
        > = manifest
            .per_workflow
            .iter()
            .map(|entry| (entry.workflow_name.as_str(), entry))
            .collect();

        // Union of both sides: a type only one of them knows about is exactly
        // the case a shared-keys comparison would miss.
        let names: std::collections::BTreeSet<&str> = declared
            .keys()
            .copied()
            .chain(self.found_execution_ids.keys().map(String::as_str))
            .collect();

        let mut mismatches = Vec::new();
        for name in names {
            let entry = declared.get(name).copied();
            let empty: Vec<ExecutionId> = Vec::new();
            let found_ids = self.found_execution_ids.get(name).unwrap_or(&empty);
            let declared_count = entry.map_or(0, |coverage| coverage.sampled);
            let count_disagrees =
                !u64::try_from(found_ids.len()).is_ok_and(|found| found == declared_count);

            // Only compare identities when the manifest actually asserts them.
            let declared_ids = entry.map_or(&empty, |coverage| &coverage.sampled_execution_ids);
            // Keyed on the inner UUID because `ExecutionId` is deliberately not
            // `Ord`; the set difference needs a total order, and a UUID has one.
            let (missing, unexpected): (Vec<ExecutionId>, Vec<ExecutionId>) =
                if declared_ids.is_empty() {
                    (Vec::new(), Vec::new())
                } else {
                    let declared_set: std::collections::BTreeSet<uuid::Uuid> =
                        declared_ids.iter().map(ExecutionId::as_uuid).collect();
                    let found_set: std::collections::BTreeSet<uuid::Uuid> =
                        found_ids.iter().map(ExecutionId::as_uuid).collect();
                    (
                        declared_set
                            .difference(&found_set)
                            .copied()
                            .map(ExecutionId::from_uuid)
                            .collect(),
                        found_set
                            .difference(&declared_set)
                            .copied()
                            .map(ExecutionId::from_uuid)
                            .collect(),
                    )
                };

            if count_disagrees || !missing.is_empty() || !unexpected.is_empty() {
                mismatches.push(BundleInventoryMismatch {
                    workflow_name: name.to_string(),
                    declared: declared_count,
                    found: found_ids.len(),
                    missing_execution_ids: missing,
                    unexpected_execution_ids: unexpected,
                });
            }
        }
        mismatches
    }

    /// Whether the bundle's contents disagree with the manifest's inventory.
    ///
    /// See [`bundle_inventory_mismatches`](Self::bundle_inventory_mismatches)
    /// for what is compared and why the total alone is not enough.
    #[must_use]
    pub fn bundle_inventory_disagrees_with_manifest(&self) -> bool {
        !self.bundle_inventory_mismatches().is_empty()
    }

    /// Whether the bundle's manifest reports incomplete cross-shard coverage.
    ///
    /// `false` when the bundle carries no manifest — an unclaimed coverage is
    /// not a *failed* coverage claim.
    #[must_use]
    pub fn has_incomplete_coverage(&self) -> bool {
        self.coverage
            .as_ref()
            .is_some_and(|manifest| !manifest.is_complete())
    }

    /// Whether the bundle's own manifest **contradicts** the claim that the
    /// fleet is idle.
    ///
    /// [`allow_empty_bundle`](ReplayVerifier::allow_empty_bundle) exists for a
    /// fleet that is legitimately idle. But a bundle is also empty when the
    /// *exporter* could not read the fleet — a total shard outage produces zero
    /// fixtures too, and the two are indistinguishable from the fixture count
    /// alone. Honoring the opt-out unconditionally lets that outage exit `0`: a
    /// release certified against nothing at all, the single worst verdict this
    /// gate can produce.
    ///
    /// Note that neither existing rung catches it. Rung `2`'s
    /// [`export_is_incomplete`](Self::export_is_incomplete) is
    /// `truncated_by_size || export_failures > 0`, and a *total* shard outage
    /// selects no candidates at all — so there are no per-candidate fetch
    /// failures to count and no size ceiling to hit, and both are `0`/`false`.
    /// Rung `4` reads the status but only under
    /// [`require_complete_coverage`](ReplayVerifier::require_complete_coverage),
    /// which is off by default. The manifest says `unavailable`, and nothing
    /// looks.
    ///
    /// So this consults exactly that: a manifest reporting anything other than
    /// complete coverage contradicts "the fleet is idle", because the export
    /// never saw the whole fleet to make that claim about.
    ///
    /// A bundle with **no** manifest reports `false` — the opt-out still
    /// applies. That is deliberate and is why this asks whether emptiness is
    /// *contradicted* rather than whether it is *proven*:
    ///
    /// * The exporter writes the manifest unconditionally, alongside the
    ///   fixtures and even when it produced none. So a Harvest-produced bundle
    ///   always carries one, and "no manifest" means the bundle did not come
    ///   from the exporter — a hand-assembled directory, or a gate binary
    ///   pointed at the wrong path. Absent-manifest is therefore not the
    ///   export-outage case this guards.
    /// * An operator who wants the stronger "prove coverage or fail" rule
    ///   already has [`require_complete_coverage`](ReplayVerifier::require_complete_coverage),
    ///   whose [`coverage_claim_unsatisfied`](Self::coverage_claim_unsatisfied)
    ///   *does* fail closed on an absent manifest. Applying that rule here
    ///   unconditionally would override an opt-out the caller set explicitly,
    ///   on evidence that does not exist, in a case the finding does not cover.
    #[must_use]
    pub fn emptiness_is_contradicted(&self) -> bool {
        self.has_incomplete_coverage()
    }

    /// Whether an opted-in `require_complete_coverage` claim is unsatisfied.
    ///
    /// Deliberately stricter than [`has_incomplete_coverage`](Self::has_incomplete_coverage),
    /// and the distinction is the whole point of the flag. That method answers
    /// "did the bundle *claim* partial coverage?", so an absent manifest is
    /// correctly `false` — nothing was claimed, so no claim failed.
    ///
    /// This method answers the operator's question instead: "was complete
    /// coverage *proven*?" A caller who sets `require_complete_coverage` has said
    /// they will not certify a build against a partial read of the fleet, and a
    /// bundle with **no** manifest proves nothing — the sample may have missed an
    /// entire shard. Reading an absent manifest as "complete" would hand that
    /// caller a green build precisely when the evidence went missing (a lost or
    /// unparseable manifest, a hand-assembled directory), which is the one
    /// outcome the flag exists to prevent. So the absence of proof fails closed.
    ///
    /// `read_bundle_manifest` is intentionally fail-open — a missing or corrupt
    /// manifest yields `None` rather than an error — so a corrupt manifest and a
    /// missing one are indistinguishable here, and both fail closed.
    #[must_use]
    pub fn coverage_claim_unsatisfied(&self) -> bool {
        if !self.require_complete_coverage {
            return false;
        }
        // `is_none_or`: no manifest is no proof of coverage at all, so an absent
        // one fails closed rather than reading as "complete".
        self.coverage.as_ref().is_none_or(|manifest| {
            !manifest.is_complete() || !self.zero_coverage_types().is_empty()
        })
    }

    /// Workflow types the manifest reports as having in-flight work but **zero**
    /// sampled fixtures.
    ///
    /// Distinct from ordinary truncation, and much worse. `sampled: 50` of
    /// `in_flight_total: 4000` still replays 50 real executions of that type, so
    /// a regression in it has 50 chances to surface. `sampled: 0` of
    /// `in_flight_total: 4000` means the type was **never replayed at all** — the
    /// gate can say nothing about it, yet the run looks green.
    ///
    /// Reachable when the global `MAX_SAMPLE_TOTAL` budget is exhausted by
    /// earlier types (the per-type floor bottoms out at 1, and beyond
    /// `MAX_SAMPLE_TOTAL` distinct in-flight types even that cannot be honoured),
    /// or when every candidate for a type failed the per-execution size ceiling.
    #[must_use]
    pub fn zero_coverage_types(&self) -> Vec<&str> {
        self.coverage.as_ref().map_or_else(Vec::new, |manifest| {
            manifest
                .per_workflow
                .iter()
                .filter(|coverage| coverage.sampled == 0 && coverage.in_flight_total > 0)
                .map(|coverage| coverage.workflow_name.as_str())
                .collect()
        })
    }

    /// Whether the export delivered fewer fixtures than the sample selected.
    ///
    /// See [`SampleManifest::is_incomplete_export`]. A bundle with no manifest
    /// reports `false` here — the absence of a manifest is rung `4`'s business
    /// (unproven coverage), not a claim that the export fell short.
    ///
    /// [`SampleManifest::is_incomplete_export`]: crate::replay_sample::SampleManifest::is_incomplete_export
    #[must_use]
    pub fn export_is_incomplete(&self) -> bool {
        self.coverage
            .as_ref()
            .is_some_and(crate::replay_sample::SampleManifest::is_incomplete_export)
    }

    /// Whether the bundle's manifest was present but could not be read.
    ///
    /// A blocking condition in its own right, and deliberately **not** folded
    /// into [`has_incomplete_coverage`](Self::has_incomplete_coverage) or
    /// [`coverage_claim_unsatisfied`](Self::coverage_claim_unsatisfied): both of
    /// those answer questions about what a manifest *said*, and the whole
    /// problem here is that nothing can be read from it.
    ///
    /// This blocks at rung `2` rather than rung `4` because rung `4` is gated on
    /// the opt-in [`require_complete_coverage`](ReplayVerifier::require_complete_coverage).
    /// An unreadable manifest disables the rung-`2` and rung-`3` guards for
    /// *every* caller, including the default gate that set no flags, so a verdict
    /// that only fires under an opt-in would leave the common case green.
    ///
    /// An **absent** manifest is not this — see [`BundleManifest`].
    #[must_use]
    pub const fn manifest_is_unreadable(&self) -> bool {
        self.manifest_unreadable.is_some()
    }

    /// Whether the gate passes.
    ///
    /// True when every fixture replayed without divergence **and** the bundle
    /// actually verified something. An empty bundle is deliberately *not* clean
    /// — see [`ReplayVerifier::allow_empty_bundle`] — and neither is a bundle
    /// with a knowingly-partial sample when
    /// [`require_complete_coverage`](ReplayVerifier::require_complete_coverage)
    /// is set.
    ///
    /// This always agrees with [`exit_code`](Self::exit_code): `is_clean()` is
    /// true exactly when `exit_code()` is `0`.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.exit_code() == 0
    }

    /// Process exit code for a CI step.
    ///
    /// | Code | Meaning |
    /// |------|---------|
    /// | `0` | Every fixture replayed cleanly |
    /// | `1` | One or more fixtures diverged — a determinism regression |
    /// | `2` | The gate could not fully run: a fixture failed to replay, the bundle's manifest was present but unreadable, the bundle holds a different number of fixtures than its manifest declares, the bundle's per-type or per-execution inventory disagrees with the manifest, **or** the export itself delivered fewer fixtures than the sample selected (dominates over `1`, because a gate that did not fully run cannot be trusted — mirrors [`CiReport::exit_code`]) |
    /// | `3` | Nothing was verified — the bundle was empty, or every fixture was skipped. [`allow_empty_bundle`](ReplayVerifier::allow_empty_bundle) opts out, but **not** when the bundle's manifest reports incomplete shard coverage: an export that could not read the fleet is empty too, and certifying a release against it is a false green |
    /// | `4` | `require_complete_coverage` is set and complete coverage was not proven — the sample is knowingly incomplete, a workflow type with in-flight work was sampled zero times, **or** the bundle carries no readable manifest at all |
    ///
    /// The rung-`2` export-shortfall condition is deliberately *unconditional* —
    /// unlike rung `4`, it does not wait for
    /// [`require_complete_coverage`](ReplayVerifier::require_complete_coverage).
    /// Rung `4` is about the sample the operator **asked for** being a slice of
    /// the fleet, which is the gate's normal mode and only a failure if they say
    /// so. This is about the bundle being a silent, biased subset of *that
    /// slice*, which nobody opted into and which no amount of reading the report
    /// would reveal to a CI step that only inspects the exit code.
    #[must_use]
    pub fn exit_code(&self) -> i32 {
        if !self.blocked.is_empty()
            || self.manifest_is_unreadable()
            || self.fixture_count_disagrees_with_manifest()
            || self.bundle_inventory_disagrees_with_manifest()
            || self.export_is_incomplete()
        {
            return 2;
        }
        if !self.diverged.is_empty() {
            return 1;
        }
        // `verified_nothing` subsumes the empty bundle: both replayed zero
        // workflows, so both are the same non-answer.
        //
        // The opt-out is void when the bundle's own manifest contradicts it: an
        // export that could not read the fleet is empty too, and exiting 0 on
        // one certifies a release against nothing. See
        // `emptiness_is_contradicted`.
        let empty_is_licensed = self.allow_empty_bundle && !self.emptiness_is_contradicted();
        if self.verified_nothing() && !empty_is_licensed {
            return 3;
        }
        if self.coverage_claim_unsatisfied() {
            return 4;
        }
        0
    }
}

impl ReplayDriftReport {
    /// Render the manifest-derived coverage block, or nothing when the bundle
    /// carries no manifest.
    ///
    /// Split out of [`Display`](std::fmt::Display) so the two "the gate did not
    /// verify what you think it did" lines — a workflow type sampled zero times,
    /// and a fixture count that disagrees with the manifest — sit next to the
    /// coverage numbers they qualify, and so `fmt` itself stays readable.
    fn fmt_coverage(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Rendered before the early return: an unreadable manifest yields no
        // `coverage` to print, and it is precisely the case an operator must not
        // mistake for "this bundle simply made no claim".
        if let Some(reason) = &self.manifest_unreadable {
            writeln!(
                f,
                "  MANIFEST UNREADABLE: the bundle carries a coverage manifest that \
                 {reason}. Its presence means this bundle came from the exporter, so \
                 the claim it recorded was damaged rather than never made — and every \
                 manifest-derived check (fixture count, export shortfall, shard \
                 coverage) is unenforceable. Re-export the sample, or re-fetch the \
                 artifact if it was truncated in transfer."
            )?;
        }
        let Some(coverage) = &self.coverage else {
            return Ok(());
        };
        writeln!(
            f,
            "  coverage: {} of {} in-flight execution(s) sampled across {} shard(s) [{}]",
            coverage.sampled_total,
            coverage.in_flight_total,
            coverage.inspected_shards.len(),
            if coverage.is_complete() {
                "complete"
            } else {
                "PARTIAL"
            },
        )?;
        for shard in &coverage.unavailable_shards {
            writeln!(f, "    unavailable: {shard}")?;
        }
        let zero_coverage = self.zero_coverage_types();
        if !zero_coverage.is_empty() {
            writeln!(
                f,
                "    NOT REPLAYED AT ALL ({} workflow type(s) have in-flight work but were \
                 sampled zero times): {}",
                zero_coverage.len(),
                zero_coverage.join(", "),
            )?;
        }
        if coverage.truncated_by_size {
            writeln!(
                f,
                "    TRUNCATED BY SIZE: the export stopped early on the response byte \
                 budget, so this bundle is smaller than the sample that was requested. \
                 Narrow the export (fewer states, a single shard, a lower max_bytes) \
                 rather than raising --per-workflow."
            )?;
        }
        if coverage.export_failures > 0 {
            writeln!(
                f,
                "    EXPORT DROPPED {} SELECTED CANDIDATE(S): the sample chose them but \
                 the export could not produce a fixture (over max_bytes, or the shard \
                 became unreadable), so this bundle is a biased subset — biased against \
                 the largest histories, which are the longest-running and the most likely \
                 to span the change under test. Re-export with a higher --max-bytes, or \
                 narrow the sample until every selected candidate fits.",
                coverage.export_failures,
            )?;
        }
        if self.fixture_count_disagrees_with_manifest() {
            writeln!(
                f,
                "  BUNDLE INCOMPLETE: manifest declares {} fixture(s) but {} were found — \
                 the bundle is not the sample the manifest describes (files lost in \
                 transit?), so this verdict covers only a subset of what was exported",
                coverage.sampled_total, self.total,
            )?;
        }
        self.fmt_inventory_mismatches(f)?;
        Ok(())
    }

    /// Render the per-type / per-execution bundle-vs-manifest disagreements.
    ///
    /// Split out from the aggregate line because the operator action differs:
    /// the total being wrong says "files went missing", while a matching total
    /// with a mismatched inventory says "the files you have are not the files
    /// that were exported" — which a re-count would never reveal.
    fn fmt_inventory_mismatches(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mismatches = self.bundle_inventory_mismatches();
        if mismatches.is_empty() {
            return Ok(());
        }
        writeln!(
            f,
            "  BUNDLE INVENTORY MISMATCH: the fixtures present are not the ones the \
             manifest says were exported, so some in-flight histories were never \
             replayed even though the totals may agree. Re-export, or re-fetch the \
             artifact.",
        )?;
        for mismatch in mismatches {
            writeln!(
                f,
                "    {}: manifest declares {}, bundle holds {}",
                mismatch.workflow_name, mismatch.declared, mismatch.found,
            )?;
            for execution_id in &mismatch.missing_execution_ids {
                writeln!(f, "      MISSING    {execution_id}")?;
            }
            for execution_id in &mismatch.unexpected_execution_ids {
                writeln!(f, "      UNEXPECTED {execution_id}")?;
            }
        }
        Ok(())
    }

    /// Render the trailing "this gate did not run" errors: a bundle that
    /// verified nothing, and an unsatisfied `require_complete_coverage` claim.
    ///
    /// Each branch names the concrete operator action, because both are harness
    /// failures rather than statements about the candidate build.
    fn fmt_errors(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let empty_is_licensed = self.allow_empty_bundle && !self.emptiness_is_contradicted();
        if self.verified_nothing() && !empty_is_licensed {
            if self.allow_empty_bundle {
                writeln!(
                    f,
                    "  ERROR: nothing was verified, and the bundle's manifest reports \
                     coverage {:?} over {} unreachable shard(s) — the export did not read \
                     the whole fleet, so an export outage cannot be told apart from an \
                     idle fleet. allow_empty_bundle(true) does not apply when the bundle \
                     itself says coverage was incomplete. Fix the export and re-run.",
                    self.coverage.as_ref().map(|manifest| manifest.status),
                    self.coverage
                        .as_ref()
                        .map_or(0, |manifest| manifest.unavailable_shards.len()),
                )?;
            } else if self.is_empty() {
                writeln!(
                    f,
                    "  ERROR: the bundle contained no fixtures — nothing was verified. \
                     Check the export step, or pass allow_empty_bundle(true) if the fleet \
                     is legitimately idle."
                )?;
            } else {
                writeln!(
                    f,
                    "  ERROR: all {} fixture(s) were skipped — no workflow was replayed, so \
                     nothing was verified. Register the sampled workflow types on this gate \
                     binary (or point it at the right bundle); pass allow_empty_bundle(true) \
                     only if verifying nothing is genuinely acceptable.",
                    self.total,
                )?;
            }
        }
        if self.coverage_claim_unsatisfied() {
            // Name which of the two failure modes occurred: "a shard was
            // unreachable" and "the manifest is missing or unreadable" call for
            // completely different operator actions.
            if self.coverage.is_none() {
                writeln!(
                    f,
                    "  ERROR: require_complete_coverage is set, but the bundle carries no \
                     readable coverage manifest ({}), so complete coverage could not be \
                     proven. Re-export the bundle with `harvest history export-sample \
                     --output-dir <dir>`.",
                    crate::replay_sample::SampleManifest::FILE_NAME
                )?;
            } else {
                writeln!(
                    f,
                    "  ERROR: the sample is incomplete (a shard could not be inspected) \
                     and require_complete_coverage is set."
                )?;
            }
        }
        Ok(())
    }
}

impl std::fmt::Display for ReplayDriftReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "harvest replay-drift: {} fixture(s) — {} clean, {} diverged, {} blocked, {} skipped",
            self.total,
            self.succeeded,
            self.diverged.len(),
            self.blocked.len(),
            self.skipped,
        )?;

        self.fmt_coverage(f)?;

        for drift in &self.diverged {
            writeln!(
                f,
                "  DRIFT  {} [{}] {} — {}",
                drift.workflow_name,
                drift.kind,
                drift
                    .execution_id
                    .map_or_else(|| "<unknown>".to_string(), |id| id.to_string()),
                drift.first_divergence,
            )?;
            writeln!(f, "         {}", drift.fixture_path.display())?;
        }
        for blocked in &self.blocked {
            writeln!(
                f,
                "  BLOCKED  {} — {}",
                blocked.fixture_path.display(),
                blocked.reason,
            )?;
        }

        self.fmt_errors(f)
    }
}

// ---------------------------------------------------------------------------
// ReplayVerifier
// ---------------------------------------------------------------------------

/// Batch CI replay gate for `#[workflow]` functions.
///
/// Walk a fixtures directory, replay every `*.json` [`HistorySnapshot`] against
/// registered workflow handlers, and return a [`BatchReplayReport`] suitable for
/// CI exit-code gating.
///
/// # Example
///
/// ```rust,no_run
/// # use autumn_harvest::testing::{ReplayVerifier, ReportFormat};
/// # async fn example() {
/// let report = ReplayVerifier::new()
///     // .register(workflows![onboarding, refund_saga, billing])
///     .fixtures_dir("./fixtures/replay")
///     .verify_all()
///     .await;
///
/// let ci = report.into_ci_report();
/// println!("{}", ci.format_report(ReportFormat::Text));
/// std::process::exit(ci.exit_code());
/// # }
/// ```
pub struct ReplayVerifier {
    handlers: HashMap<String, WorkflowHandlerFn>,
    state: SharedState,
    concurrency: usize,
    timeout: std::time::Duration,
    allow_unregistered: bool,
    fixtures_dir: Option<std::path::PathBuf>,
    /// Drift-gate policy (issue #798): treat a bundle with zero fixtures as
    /// clean instead of refusing to certify it.
    allow_empty_bundle: bool,
    /// Drift-gate policy (issue #798): refuse to certify a bundle whose manifest
    /// reports that a shard could not be inspected.
    require_complete_coverage: bool,
    /// Replay-context values threaded into every fixture's replayer, including
    /// the issue #614 history policy set by
    /// [`with_history_policy`](Self::with_history_policy).
    ///
    /// The four non-policy fallbacks are not builder-settable on the verifier:
    /// they exist so a caller who configured them on a `WorkflowReplayer`
    /// (`with_context_headers`, `with_execution_timeout`,
    /// `with_parent_execution_id`, `with_workflow_id`) does not silently lose
    /// them by switching to the bundle path. Configure them there; a fixture
    /// carrying its own value still overrides.
    replay_defaults: FixtureReplayDefaults,
}

/// The replay-context fallbacks a bundle walk threads into every fixture's
/// per-fixture [`WorkflowReplayer`].
///
/// Each is a *fallback*, not an override: a [`HistorySnapshot`] that carries its
/// own value (every field here is exported by the real #798 export path) wins,
/// so these only take effect for a hand-built or legacy fixture that omits the
/// metadata — exactly the contract the equivalent `WorkflowReplayer` globals
/// document.
///
/// Bundled into one struct rather than five more positional parameters so
/// `replay_fixture_file` stays under the `too_many_arguments` bar and a future
/// field is a one-line addition instead of another call-site sweep.
#[derive(Debug, Clone, Default)]
struct FixtureReplayDefaults {
    context_headers: HashMap<String, String>,
    execution_timeout: Option<chrono::Duration>,
    parent_execution_id: Option<ExecutionId>,
    workflow_id: Option<String>,
    queue_name: Option<String>,
    /// The **candidate** build id (issue #798).
    ///
    /// The one field here that is *not* a fixture fallback: no `HistorySnapshot`
    /// carries a build id, deliberately. The live worker reports its own
    /// configured build rather than the execution's recorded `assigned_build_id`,
    /// so a gate must apply the build about to be promoted uniformly to every
    /// fixture — sourcing it from the fixture would replay the historical branch
    /// and hide candidate-only drift.
    build_id: Option<String>,
    history_policy: crate::context::WorkflowHistoryPolicy,
    /// The **candidate** worker's payload limits (issue #798, Codex round 20).
    ///
    /// Same category as `build_id` above: not a fixture fallback. Payload caps
    /// and the offload threshold live in no `WorkflowEvent` and the live worker
    /// supplies its own, so a gate that leaves them at the library defaults
    /// certifies a cap-lowering build that will then reject the very in-flight
    /// runs it sampled.
    payload_limits: crate::executor::ReplayPayloadLimits,
    /// The candidate's declarative `#[query]` / `#[update]` handlers (issue #798).
    ///
    /// Like the build id and payload limits, authoritative rather than a
    /// fallback: the bundle describes executions, not the runtime that will
    /// resume them, so the candidate's registrations come from the caller.
    declarative_queries: Vec<crate::info::QueryHandlerInfo>,
    /// See `declarative_queries`.
    declarative_updates: Vec<crate::info::UpdateHandlerInfo>,
    /// Per-workflow `#[workflow(max_input_bytes = …)]` overrides, keyed by
    /// workflow type name (issue #798, Codex round 22).
    ///
    /// Populated by [`ReplayVerifier::register`] from the registered
    /// [`WorkflowInfo`](crate::info::WorkflowInfo)s. Carried per fixture because
    /// the live worker resolves this cap per workflow type, not fleet-wide; see
    /// `WorkflowReplayer::payload_limits_for`.
    workflow_input_caps: HashMap<String, u64>,
}

impl Default for ReplayVerifier {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplayVerifier {
    /// Create a new verifier with sensible defaults (concurrency = available CPUs, timeout = 60s).
    #[must_use]
    pub fn new() -> Self {
        let concurrency =
            std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get);
        Self {
            handlers: HashMap::new(),
            state: crate::context::empty_shared_state(),
            concurrency,
            timeout: std::time::Duration::from_secs(60),
            allow_unregistered: false,
            fixtures_dir: None,
            allow_empty_bundle: false,
            require_complete_coverage: false,
            replay_defaults: FixtureReplayDefaults::default(),
        }
    }

    /// Replay fixtures under the deployment's history policy (issue #614).
    ///
    /// The live worker evaluates
    /// [`should_continue_as_new`](crate::context::WorkflowContext::should_continue_as_new)
    /// with the registry's policy (`registry.history_policy()`). A workflow that
    /// branches on it emits a *different command* under a different
    /// `continue_as_new_threshold` — so replaying a bundle under the default
    /// policy while production runs a customized one reports drift for a
    /// workflow that never drifted, blocking a healthy deploy.
    ///
    /// Pass the same value the deployment's `HarvestBuilder` was given:
    ///
    /// ```rust,no_run
    /// # use autumn_harvest::testing::ReplayVerifier;
    /// # use autumn_harvest::context::WorkflowHistoryPolicy;
    /// # async fn demo(policy: WorkflowHistoryPolicy) {
    /// let report = ReplayVerifier::new()
    ///     .with_history_policy(policy)
    ///     .replay_bundle("./fixtures/in-flight")
    ///     .await;
    /// # }
    /// ```
    ///
    /// Defaults to [`WorkflowHistoryPolicy::default()`](crate::context::WorkflowHistoryPolicy),
    /// which is correct for any deployment that never customized it.
    #[must_use]
    pub const fn with_history_policy(
        mut self,
        history_policy: crate::context::WorkflowHistoryPolicy,
    ) -> Self {
        self.replay_defaults.history_policy = history_policy;
        self
    }

    /// Set the **candidate** build id threaded into every fixture's replay
    /// context (issue #798).
    ///
    /// Pass the build id of the worker you are **about to deploy**. The live
    /// worker reports its own configured
    /// [`WorkerConfig::build_id`](crate::builder::WorkerConfig::build_id) via
    /// span metadata rather than the execution's recorded `assigned_build_id`,
    /// so a replay gate only answers "what will the candidate do with these
    /// in-flight histories?" when the candidate's id reaches every fixture.
    ///
    /// Unlike the other replay-context values a bundle threads, this is **not** a
    /// fixture fallback: no [`HistorySnapshot`] carries a build id, because one
    /// sourced from the fixture would be the *recording* build and would replay
    /// the historical branch — reporting clean for candidate-only code that
    /// diverges on promotion.
    ///
    /// Leaving it unset makes `ctx.build_id()` report `None` during the gate.
    #[must_use]
    pub fn with_build_id(mut self, build_id: impl Into<String>) -> Self {
        self.replay_defaults.build_id = Some(build_id.into());
        self
    }

    /// Apply the **candidate** worker's payload caps to every fixture replay
    /// (issue #798).
    ///
    /// Bytes: `(max_activity_input, max_signal_payload, max_workflow_input)`;
    /// `0` means "no cap". Pass what the candidate build configures on its
    /// `HarvestBuilder`. Without this the gate replays under the library defaults
    /// and a build that *lowers* a cap replays clean, then rejects the sampled
    /// in-flight executions with `PayloadTooLarge` once promoted.
    #[must_use]
    pub const fn with_payload_caps(
        mut self,
        max_activity_input: u64,
        max_signal_payload: u64,
        max_workflow_input: u64,
    ) -> Self {
        self.replay_defaults.payload_limits.max_activity_input = max_activity_input;
        self.replay_defaults.payload_limits.max_signal_payload = max_signal_payload;
        self.replay_defaults.payload_limits.max_workflow_input = max_workflow_input;
        self
    }

    /// Apply the **candidate** worker's large-payload offload threshold (#524)
    /// to every fixture replay (issue #798).
    ///
    /// A payload above the threshold is offloaded rather than capped, so without
    /// this a gate that knows the cap reports drift the promoted worker would
    /// never hit. `None` (the default) models a worker with no `PayloadStore`.
    #[must_use]
    pub const fn with_payload_offload_threshold(mut self, threshold: Option<u64>) -> Self {
        self.replay_defaults.payload_limits.offload_threshold = threshold;
        self
    }

    /// Register the **candidate's** declarative `#[query]` handlers on every
    /// fixture's replay context (issue #798).
    ///
    /// The live worker registers a workflow's declarative handlers *before any
    /// workflow code runs*, and `ctx.list_query_names()` merges them into its
    /// result — so a workflow that branches on which handlers exist, or that
    /// dispatches a query, observes them. They live in no `WorkflowEvent`, so
    /// replay cannot recover them from a fixture and the gate must be told.
    ///
    /// Like [`with_build_id`](Self::with_build_id), this is **not** a fixture
    /// fallback: pass the same `queries![...]` collection the build you are about
    /// to deploy registers.
    ///
    /// ```rust,no_run
    /// # use autumn_harvest::testing::ReplayVerifier;
    /// # use autumn_harvest::info::QueryHandlerInfo;
    /// # fn make_queries() -> Vec<QueryHandlerInfo> { vec![] }
    /// let verifier = ReplayVerifier::new().queries(make_queries());
    /// ```
    #[must_use]
    pub fn queries(mut self, queries: Vec<crate::info::QueryHandlerInfo>) -> Self {
        self.replay_defaults.declarative_queries = queries;
        self
    }

    /// Register the **candidate's** declarative `#[update]` handlers on every
    /// fixture's replay context (issue #798). See [`queries`](Self::queries).
    #[must_use]
    pub fn updates(mut self, updates: Vec<crate::info::UpdateHandlerInfo>) -> Self {
        self.replay_defaults.declarative_updates = updates;
        self
    }

    /// Register a batch of workflow handlers from a `workflows![…]` collector call.
    #[must_use]
    pub fn register(mut self, workflows: Vec<crate::info::WorkflowInfo>) -> Self {
        for wf in workflows {
            // Issue #798 (Codex round 22): retain the per-workflow input-cap
            // override so each fixture replays under the cap the promoted worker
            // resolves for *its* workflow type, not one bundle-wide value.
            if let Some(per) = wf.max_input_bytes {
                self.replay_defaults
                    .workflow_input_caps
                    .insert(wf.name.to_string(), per);
            }
            self.handlers.insert(wf.name.to_string(), wf.handler);
        }
        self
    }

    /// Register a single handler by name.
    #[must_use]
    pub fn register_fn(mut self, name: impl Into<String>, handler: WorkflowHandlerFn) -> Self {
        self.handlers.insert(name.into(), handler);
        self
    }

    /// Inject a typed shared-state value available to workflow handlers via
    /// `ctx.state::<T>()` during replay.
    ///
    /// # Panics
    ///
    /// Panics if the state `Arc` has already been cloned (unreachable in normal builder usage).
    #[must_use]
    pub fn with_state<T: Send + Sync + 'static>(mut self, value: T) -> Self {
        std::sync::Arc::get_mut(&mut self.state)
            .expect("state Arc has no other references during ReplayVerifier construction")
            .insert(std::any::TypeId::of::<T>(), Box::new(value));
        self
    }

    /// Set the maximum number of fixtures replayed concurrently (default = available CPUs).
    #[must_use]
    pub fn with_concurrency(mut self, n: usize) -> Self {
        self.concurrency = n.max(1);
        self
    }

    /// Set the per-fixture replay timeout (default = 60 seconds).
    #[must_use]
    pub const fn with_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// When `true`, fixtures whose `workflow_name` is not registered are counted as
    /// [`FixtureStatus::Skipped`] rather than a [`HarnessErrorKind::UnregisteredWorkflow`]
    /// harness error.  Use this when a single fixtures directory holds histories from
    /// multiple binaries.
    #[must_use]
    pub const fn allow_unregistered(mut self, allow: bool) -> Self {
        self.allow_unregistered = allow;
        self
    }

    /// Set the fixtures directory used by [`verify_all`](Self::verify_all).
    #[must_use]
    pub fn fixtures_dir(mut self, path: impl AsRef<std::path::Path>) -> Self {
        self.fixtures_dir = Some(path.as_ref().to_owned());
        self
    }

    /// Treat a bundle containing zero fixtures as clean (default: `false`).
    ///
    /// The drift gate refuses to certify an empty bundle by default, because a
    /// gate that passes vacuously is worse than no gate: a typo'd
    /// `--output-dir`, a failed export, or a filter that matched nothing would
    /// all report "green" while verifying nothing at all.
    ///
    /// Set this when a fleet legitimately has no executions in flight (a fresh
    /// environment, or a workflow type that is only triggered on demand).
    ///
    /// This opt-out is **void when the bundle's own manifest reports incomplete
    /// shard coverage**: an export that could not read the fleet produces zero
    /// fixtures too, so honoring the flag there would certify a release against
    /// an export outage. See
    /// [`ReplayDriftReport::emptiness_is_contradicted`]. A bundle with no
    /// manifest is unaffected — use
    /// [`require_complete_coverage`](Self::require_complete_coverage) to demand
    /// positive proof of coverage.
    #[must_use]
    pub const fn allow_empty_bundle(mut self, allow: bool) -> Self {
        self.allow_empty_bundle = allow;
        self
    }

    /// Refuse to certify a bundle whose manifest reports incomplete cross-shard
    /// coverage (default: `false`).
    ///
    /// A sample drawn while a shard was unreachable covers less than the fleet,
    /// so "no drift" means "no drift in the part we could see". By default that
    /// is reported but not enforced — the export is an explicit *sample*. Turn
    /// this on for a release gate that must not go green on a knowingly-partial
    /// read of the fleet.
    ///
    /// Has no effect on a bundle with no
    /// [`SampleManifest`](crate::replay_sample::SampleManifest); see
    /// [`replay_bundle`](Self::replay_bundle) for how a manifest-less bundle is
    /// treated.
    #[must_use]
    pub const fn require_complete_coverage(mut self, require: bool) -> Self {
        self.require_complete_coverage = require;
        self
    }

    /// Replay an exported **in-flight** history bundle and return an aggregate
    /// [`ReplayDriftReport`] suitable for gating a deploy (issue #798).
    ///
    /// `dir` is a bundle produced by
    /// `harvest history export-sample --output-dir <dir>`: a flat directory of
    /// `*.json` [`HistorySnapshot`] fixtures plus a
    /// [`SampleManifest`](crate::replay_sample::SampleManifest) carrying the
    /// coverage the sample achieved. The manifest is read (and excluded from the
    /// fixture walk) automatically; a directory without one still replays, and
    /// [`ReplayDriftReport::coverage`] is simply `None`.
    ///
    /// A manifest that is **present but unreadable** is a different case and
    /// blocks the gate at exit `2`. Its presence means the bundle claims to be an
    /// exporter product, so an unreadable one is damaged evidence rather than an
    /// absent claim — and every manifest-derived guard would otherwise switch
    /// itself off silently. See [`ReplayDriftReport::manifest_is_unreadable`].
    ///
    /// # How this differs from [`verify_dir`](Self::verify_dir)
    ///
    /// `verify_dir` replays **strictly**: the workflow must consume its entire
    /// recorded history. That is right for captured *completed* histories, and
    /// wrong for this gate — a healthy **in-flight** execution parks at its
    /// recorded frontier, which strict replay classifies as non-determinism. A
    /// strict gate over sampled in-flight runs would therefore be false-red on
    /// every fixture. `replay_bundle` replays frontier-tolerantly instead, so a
    /// parked run is clean while a genuine mid-history divergence still fails.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use autumn_harvest::testing::ReplayVerifier;
    /// # async fn example() {
    /// let report = ReplayVerifier::new()
    ///     // .register(workflows![onboarding, refund_saga, billing])
    ///     .replay_bundle("./fixtures/in-flight")
    ///     .await;
    ///
    /// println!("{report}");
    /// std::process::exit(report.exit_code());
    /// # }
    /// ```
    pub async fn replay_bundle(&self, dir: impl AsRef<std::path::Path>) -> ReplayDriftReport {
        let dir = dir.as_ref();
        let batch = self.replay_dir(dir, FixtureReplayMode::InFlight).await;
        let manifest = read_bundle_manifest(dir).await;
        ReplayDriftReport::from_batch(
            batch,
            manifest,
            self.allow_empty_bundle,
            self.require_complete_coverage,
        )
    }

    /// Walk the directory set by [`fixtures_dir`](Self::fixtures_dir) and replay all
    /// `*.json` fixtures.
    ///
    /// # Panics
    ///
    /// Panics if [`fixtures_dir`](Self::fixtures_dir) was not called before this method.
    pub async fn verify_all(&self) -> BatchReplayReport {
        let dir = self
            .fixtures_dir
            .as_deref()
            .expect("call fixtures_dir(path) before verify_all(), or use verify_dir(path)");
        self.verify_dir(dir).await
    }

    /// Walk `dir` recursively, collect all `*.json` files, replay each one against
    /// the registered handlers, and return a [`BatchReplayReport`].
    ///
    /// If `dir` cannot be read (missing, wrong permissions, or a typo in the path),
    /// the report contains a single `HarnessError` so CI exits 2 instead of silently
    /// succeeding with zero fixtures.
    ///
    /// # Panics
    ///
    /// Panics if the internal semaphore is closed, which cannot happen under normal use.
    pub async fn verify_dir(&self, dir: &std::path::Path) -> BatchReplayReport {
        self.replay_dir(dir, FixtureReplayMode::Strict).await
    }

    /// Shared directory-replay engine behind [`verify_dir`](Self::verify_dir) and
    /// [`replay_bundle`](Self::replay_bundle) — one walker, one concurrency
    /// budget, one aggregation, two replay modes.
    async fn replay_dir(
        &self,
        dir: &std::path::Path,
        mode: FixtureReplayMode,
    ) -> BatchReplayReport {
        let files = match collect_json_files(dir).await {
            Ok(f) => f,
            Err(e) => {
                let result = FixtureResult {
                    path: dir.to_path_buf(),
                    workflow_name: String::new(),
                    execution_id: None,
                    status: FixtureStatus::HarnessError(HarnessErrorKind::InvalidFixture(format!(
                        "cannot read fixtures directory: {e}"
                    ))),
                };
                return BatchReplayReport {
                    fixtures_total: 1,
                    succeeded: 0,
                    failed: 0,
                    harness_errors: 1,
                    skipped: 0,
                    results: vec![result],
                };
            }
        };

        if files.is_empty() {
            return BatchReplayReport {
                fixtures_total: 0,
                succeeded: 0,
                failed: 0,
                harness_errors: 0,
                skipped: 0,
                results: vec![],
            };
        }

        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.concurrency));
        let timeout = self.timeout;
        let allow_unregistered = self.allow_unregistered;
        let defaults = Arc::new(self.replay_defaults.clone());
        let handlers = Arc::new(self.handlers.clone());
        let state = self.state.clone();

        let mut tasks = Vec::with_capacity(files.len());
        for path in files {
            let sem = Arc::clone(&semaphore);
            let handlers = Arc::clone(&handlers);
            let defaults = Arc::clone(&defaults);
            let state = state.clone();

            tasks.push(tokio::spawn(async move {
                let _permit = sem.acquire_owned().await.unwrap();
                replay_fixture_file(
                    &handlers,
                    state,
                    &path,
                    timeout,
                    allow_unregistered,
                    mode,
                    &defaults,
                )
                .await
            }));
        }

        let mut results: Vec<FixtureResult> = futures::future::join_all(tasks)
            .await
            .into_iter()
            .enumerate()
            .map(|(i, join_result)| {
                join_result.unwrap_or_else(|e| FixtureResult {
                    path: std::path::PathBuf::from(format!("<task-{i}>")),
                    workflow_name: String::new(),
                    execution_id: None,
                    status: FixtureStatus::HarnessError(HarnessErrorKind::InvalidFixture(format!(
                        "task panicked or was cancelled: {e}"
                    ))),
                })
            })
            .collect();

        results.sort_by(|a, b| a.path.cmp(&b.path));

        let mut succeeded = 0usize;
        let mut failed = 0usize;
        let mut harness_errors = 0usize;
        let mut skipped = 0usize;
        for r in &results {
            match &r.status {
                FixtureStatus::Passed => succeeded += 1,
                FixtureStatus::Failed(_) => failed += 1,
                FixtureStatus::HarnessError(_) => harness_errors += 1,
                FixtureStatus::Skipped { .. } => skipped += 1,
            }
        }

        BatchReplayReport {
            fixtures_total: results.len(),
            succeeded,
            failed,
            harness_errors,
            skipped,
            results,
        }
    }
}

/// The outcome of looking for a bundle's coverage manifest.
///
/// Three states, not two, because "the file is not there" and "the file is there
/// but I could not read it" are different facts about the bundle and call for
/// opposite verdicts.
enum BundleManifest {
    /// A well-formed manifest. The bundle carries a coverage claim.
    Present(Box<crate::replay_sample::SampleManifest>),
    /// No manifest file at all.
    ///
    /// Deliberately **not** an error. The exporter always writes one, so an
    /// absent manifest means the bundle did not come from the exporter — a
    /// hand-assembled directory of fixtures, which is a supported way to drive
    /// the gate. It simply makes no coverage claim, so there is nothing to
    /// verify and nothing to contradict.
    Absent,
    /// The manifest **exists** but could not be read or parsed.
    ///
    /// The opposite situation from [`Absent`](Self::Absent), and the reason this
    /// is a three-state enum. The file's presence is the bundle asserting *"I am
    /// an exporter product and here is my record of what I sampled"*. If that
    /// record is truncated, corrupted in transfer, or unreadable, the assertion
    /// stands but the evidence is gone — and every manifest-derived guard reads
    /// through `Option::is_some_and`, so a `None` silently turns each one off:
    ///
    /// * the `sampled_total` fixture-count cross-check (rung `2`)
    /// * the `export_failures` / `truncated_by_size` shortfall check (rung `2`)
    /// * the `status` / `unavailable_shards` partial-shard check (rung `3`)
    ///
    /// The surviving fixtures would then replay to a green exit `0`: a release
    /// certified against an export whose own record of what it did was lost.
    Unreadable(String),
}

/// Read a bundle's coverage manifest.
///
/// Distinguishes a missing manifest (fail-open — a hand-assembled bundle carries
/// no coverage claim) from a present-but-unreadable one (fail-closed — the claim
/// existed and was damaged). See [`BundleManifest`] for why that distinction is
/// load-bearing rather than pedantic.
async fn read_bundle_manifest(dir: &std::path::Path) -> BundleManifest {
    let path = dir.join(crate::replay_sample::SampleManifest::FILE_NAME);
    let raw = match tokio::fs::read_to_string(&path).await {
        Ok(raw) => raw,
        // Only a genuine "not there" is fail-open. A permissions error, a
        // partial read, or any other I/O failure means the file exists in some
        // form we could not consume — evidence we were meant to have and do not.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return BundleManifest::Absent;
        }
        Err(error) => {
            tracing::warn!(
                path = %path.display(),
                %error,
                "harvest: replay-sample manifest is present but unreadable",
            );
            return BundleManifest::Unreadable(format!("could not be read: {error}"));
        }
    };
    match serde_json::from_str(&raw) {
        Ok(manifest) => BundleManifest::Present(Box::new(manifest)),
        Err(error) => {
            tracing::warn!(
                path = %path.display(),
                %error,
                "harvest: replay-sample manifest is present but unparseable",
            );
            BundleManifest::Unreadable(format!("could not be parsed: {error}"))
        }
    }
}

/// Recursively collect `*.json` files under `dir`.
///
/// Returns `Err` if the top-level `dir` cannot be read so the caller can
/// surface it as a harness error rather than silently returning zero fixtures.
async fn collect_json_files(
    dir: &std::path::Path,
) -> Result<Vec<std::path::PathBuf>, std::io::Error> {
    let mut files = Vec::new();
    let mut dirs_to_visit = vec![dir.to_path_buf()];

    // Probe the top-level directory explicitly so a missing/unreadable path
    // is distinguishable from a legitimately empty directory.
    let _ = tokio::fs::read_dir(dir).await?;

    while let Some(current_dir) = dirs_to_visit.pop() {
        if let Ok(mut entries) = tokio::fs::read_dir(&current_dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                if let Ok(file_type) = entry.file_type().await {
                    if file_type.is_dir() {
                        dirs_to_visit.push(path);
                    } else if path.extension().and_then(|s| s.to_str()) == Some("json")
                        // A replay-drift bundle (issue #798) carries its coverage
                        // manifest alongside the fixtures. It is not a history, so
                        // replaying it would produce a spurious harness error and
                        // fail the gate for a well-formed bundle.
                        && path.file_name().and_then(|s| s.to_str())
                            != Some(crate::replay_sample::SampleManifest::FILE_NAME)
                    {
                        files.push(path);
                    }
                }
            }
        }
    }

    files.sort();
    Ok(files)
}

/// The two export-document fields the fixture guard consults.
///
/// Deliberately a *side* struct parsed from the same fixture JSON rather than
/// two more fields on [`HistorySnapshot`]: both are properties of how a history
/// was *exported*, not inputs the replay itself consumes, so putting them on the
/// snapshot would force every hand-built fixture in the workspace to declare
/// export metadata it has no notion of.
///
/// Every field is `#[serde(default)]`, so a hand-built fixture (or a legacy
/// export produced before these fields existed) parses to all-`None` and is
/// treated as replayable.
#[derive(Debug, Default, Clone, serde::Deserialize)]
struct FixtureGuardFields {
    #[serde(default)]
    payload_policy: Option<crate::history_export::HistoryPayloadPolicy>,
    #[serde(default)]
    status: Option<crate::history_export::HistoryExportStatus>,
}

/// Why a fixture cannot be honestly replayed by `mode`, or `None` if it can.
///
/// Pure and separate from the replay so the "would this verdict be a lie?"
/// decision is unit-testable without touching disk or running a workflow.
///
/// Both cases return a *harness error* rather than a divergence, because in
/// neither case did the candidate build do anything wrong — the bundle is simply
/// not evaluable. Telling an operator "your code regressed" when the real problem
/// is a stripped payload or a mis-aimed directory is the worst outcome a gate can
/// produce, so each message names the concrete fix.
///
/// A fixture that declares neither field (a legacy export, or a fixture built by
/// hand in Rust) is treated as replayable — hand-built fixtures carry real
/// inputs, and refusing them would break every existing directory fixture.
/// Refuse a fixture carrying large-payload **offload reference envelopes**
/// (issue #524) instead of the real payloads.
///
/// The export path loads history with `store::load_history_undecoded`
/// (deliberately, so an encrypted history is not failed before the payload
/// policy runs), so on a deployment with a `PayloadStore` registered any payload
/// over the offload threshold is exported as a claim-check envelope. The
/// directory-replay path builds its `WorkflowReplayer` with
/// `payload_offloader: None` and therefore cannot inflate it.
///
/// Left unguarded, the candidate workflow computes the *real* input while
/// history holds the envelope, so `match_activity_strict` diverges on every
/// affected fixture — the gate would report a determinism regression that does
/// not exist and block a healthy deploy. That is the same false-red class as a
/// redacted bundle, and gets the same treatment: a harness error (exit 2) that
/// names the fix, rather than a confidently wrong "your code drifted".
///
/// Applies to both replay modes: an un-inflatable envelope is un-replayable
/// regardless of which gate is asking, and a fixture with no envelope is
/// untouched — so this can never turn a genuinely-clean run red.
fn offloaded_fixture_reason(json: &str, snapshot: &HistorySnapshot) -> Option<String> {
    // Fast reject on the raw text first: the discriminator is a fixed key, so a
    // substring miss proves no envelope is present without re-serializing a
    // single event. Only a hit pays for the structured confirmation below.
    if !json.contains(crate::payload_store::OFFLOAD_ENVELOPE_KEY) {
        return None;
    }

    let offloaded: Vec<String> = snapshot
        .events
        .iter()
        .filter_map(|event| serde_json::to_value(event).ok())
        .flat_map(|value| crate::payload_store::refs_in_event_value(&value))
        .map(|reference| reference.blob_key)
        .collect();
    if offloaded.is_empty() {
        // The key appeared in payload *data* rather than as a real envelope
        // (e.g. a workflow whose own JSON mentions it). Nothing to inflate.
        return None;
    }

    Some(format!(
        "fixture carries {} offloaded payload reference(s) (issue #524) that this \
         gate cannot inflate, so replaying it would compare the candidate's real \
         inputs against claim-check envelopes and report drift that does not \
         exist. Sample a workflow whose payloads stay under the offload \
         threshold, or raise `payload_offload_threshold` for the exported \
         window. First blob key: {}",
        offloaded.len(),
        offloaded.first().map_or("<unknown>", String::as_str),
    ))
}

/// Refuse a fixture whose payloads are still **codec envelopes** (issue #608),
/// or carry the marker a decode attempt left behind when it failed.
///
/// The codec half of [`offloaded_fixture_reason`], and reachable in strictly
/// more deployments: the export loads history with
/// `store::load_history_undecoded`, and the plugin's `read_path_decoder`
/// returns `None` whenever `decode_payloads_on_read` is left at its default
/// `false` — so on a codec-**encrypting** deployment *every* payload-bearing
/// field reaches the bundle as ciphertext, not just those over a size
/// threshold.
///
/// The directory-replay path builds its `WorkflowReplayer` with no codec
/// registry and cannot decode it. Left unguarded, the candidate workflow
/// computes the real plaintext while history holds the envelope, so
/// `match_activity_strict` diverges on every affected fixture — a determinism
/// regression that does not exist, blocking a healthy deploy. Same false-red
/// class as a redacted or offloaded bundle, same treatment: a harness error
/// (exit 2) naming the fix.
///
/// Two discriminators, because a payload can be opaque for two different
/// reasons and both are equally un-replayable:
/// - [`CODEC_ENVELOPE_KEY`](crate::payload_codec::CODEC_ENVELOPE_KEY) — never
///   decoded (flag off, or the caller was not an admin).
/// - [`UNDECODABLE_MARKER_KEY`](crate::payload_codec::UNDECODABLE_MARKER_KEY) —
///   a lossy decode *was* attempted and failed (unknown codec, bad base64,
///   codec error, invalid JSON), so the plaintext is gone either way.
///
/// Scoped to the payload-bearing fields of each event's `data` object — exactly
/// where the codec transform writes — so business data nested deeper cannot
/// trip it. Envelope detection delegates to
/// [`is_codec_envelope`](crate::payload_codec::is_codec_envelope), the crate's
/// own authoritative shape check, so this can never drift from what the decoder
/// recognises.
/// Refuse a fixture whose payloads were **erased** (issue #495) rather than
/// exported.
///
/// The third member of the opaque-payload family, alongside
/// [`offloaded_fixture_reason`] and [`codec_opaque_fixture_reason`]: in all
/// three the fixture holds something that is not the payload, so replaying it
/// compares the candidate's real inputs against a placeholder. Erasure is the
/// one that cannot be undone — the plaintext is gone by design — so the answer
/// is never "decode it", only "do not certify a release against it".
///
/// Two ways a bundle acquires one, and the second needs no race at all:
/// * The sample export selects an execution while it is in flight, and it
///   completes and is erased before the (sequential) per-candidate fetch
///   reaches it. The candidate row still reads `RUNNING`, so nothing upstream
///   notices.
/// * The **batch** export has no in-flight restriction, and erasure is
///   terminal-only — so the executions it exports are exactly the ones eligible
///   for erasure. No timing window is required.
///
/// Guarding here rather than only at export covers both, plus a hand-curated
/// bundle and a fixture erased *after* it was written. Reported as a harness
/// error (exit 2), never skipped: a silently dropped fixture shrinks coverage
/// while the manifest still claims it, which is the one failure this gate must
/// not have.
fn erased_fixture_reason(json: &str, snapshot: &HistorySnapshot) -> Option<String> {
    // Cheap reject first: no tombstone key anywhere in the document means no
    // erased payload, and skips the per-event walk for every healthy fixture.
    if !json.contains(crate::erase::ERASURE_TOMBSTONE_KEY) {
        return None;
    }

    let mut tombstones = 0usize;
    for value in snapshot
        .events
        .iter()
        .filter_map(|event| serde_json::to_value(event).ok())
    {
        let Some(data) = value.get("data").and_then(Value::as_object) else {
            continue;
        };
        for key in crate::payload_store::PAYLOAD_FIELD_KEYS {
            let Some(field) = data.get(key) else { continue };
            if field
                .as_object()
                .is_some_and(|obj| obj.contains_key(crate::erase::ERASURE_TOMBSTONE_KEY))
            {
                tombstones += 1;
            }
        }
    }
    if tombstones == 0 {
        // The key occurred inside payload *data* rather than as a tombstone in
        // a payload field — a workflow whose own JSON happens to mention it.
        // Not erased; replay it normally.
        return None;
    }

    Some(format!(
        "fixture carries {tombstones} erased-payload tombstone(s) (issue #495) instead of \
         the real payloads, so replaying it would compare the candidate's real inputs \
         against scrubbed placeholders — reporting drift that does not exist, or a clean \
         result that certifies nothing. Erasure is irreversible, so re-export a sample \
         that excludes this execution (it is terminal, and the gate verifies in-flight \
         work) rather than trying to recover the payloads."
    ))
}

fn codec_opaque_fixture_reason(json: &str, snapshot: &HistorySnapshot) -> Option<String> {
    use crate::payload_codec::{CODEC_ENVELOPE_KEY, UNDECODABLE_MARKER_KEY};

    // Fast reject on the raw text first: both discriminators are fixed keys, so
    // a substring miss proves neither is present without re-serializing a single
    // event. Only a hit pays for the structured confirmation below.
    if !json.contains(CODEC_ENVELOPE_KEY) && !json.contains(UNDECODABLE_MARKER_KEY) {
        return None;
    }

    let mut envelopes = 0usize;
    let mut undecodable = 0usize;
    for value in snapshot
        .events
        .iter()
        .filter_map(|event| serde_json::to_value(event).ok())
    {
        let Some(data) = value.get("data").and_then(Value::as_object) else {
            continue;
        };
        for key in crate::payload_store::PAYLOAD_FIELD_KEYS {
            let Some(field) = data.get(key) else { continue };
            if crate::payload_codec::is_codec_envelope(field) {
                envelopes += 1;
            } else if field
                .as_object()
                .is_some_and(|obj| obj.contains_key(UNDECODABLE_MARKER_KEY))
            {
                undecodable += 1;
            }
        }
    }
    if envelopes == 0 && undecodable == 0 {
        // The key appeared in payload *data* rather than as a real envelope
        // (e.g. a workflow whose own JSON mentions it). Nothing to decode.
        return None;
    }

    Some(format!(
        "fixture carries {envelopes} undecoded codec envelope(s) and {undecodable} \
         undecodable-payload marker(s) (issue #608) instead of the real payloads, so \
         replaying it would compare the candidate's real inputs against ciphertext and \
         report drift that does not exist. Re-export with payload decoding enabled \
         (`HarvestPlugin::decode_payloads_on_read()`, and call the route as an admin), \
         or run the gate against a deployment with no payload codec registered."
    ))
}

fn unreplayable_fixture_reason(
    guard: &FixtureGuardFields,
    mode: FixtureReplayMode,
) -> Option<String> {
    use crate::history_export::HistoryPayloadPolicy;

    // Redaction rewrites payload-bearing fields in place. Both replay paths run
    // with `strict_replay = true`, so `match_activity_strict` compares the
    // redacted stub against the input the workflow computes -> divergence on
    // every fixture that passes a non-trivial activity input.
    if guard.payload_policy == Some(HistoryPayloadPolicy::Redacted) {
        return Some(
            "fixture was exported with payload_policy=redacted, which rewrites \
             activity inputs and outputs and therefore cannot be replayed \
             (every fixture would report a false divergence); re-export the \
             bundle with `--payload-policy full`"
                .to_string(),
        );
    }

    // A bundle aimed at the wrong gate: each mode's verdict for the other's
    // population is misleading rather than merely wrong.
    if let Some(status) = guard.status.as_ref() {
        match mode {
            FixtureReplayMode::InFlight if status.terminal => {
                return Some(format!(
                    "fixture is a terminal execution (state={}), but the \
                     replay-drift gate replays in-flight executions; use \
                     `ReplayVerifier::verify_dir` for completed histories",
                    status.state
                ));
            }
            FixtureReplayMode::Strict if !status.terminal => {
                return Some(format!(
                    "fixture is an in-flight execution (state={}), but \
                     `verify_dir` replays completed histories strictly and would \
                     report a false divergence; use \
                     `ReplayVerifier::replay_bundle` for an in-flight bundle",
                    status.state
                ));
            }
            _ => {}
        }
    }

    None
}

/// How a directory fixture is replayed.
///
/// The two modes exist because the two gates protect different populations, and
/// the correct verdict for a parked workflow is the opposite in each.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FixtureReplayMode {
    /// Strict replay — the workflow must consume its entire recorded history and
    /// reach a terminal outcome. A suspension means the code issued a command
    /// with no matching history event.
    ///
    /// Used by [`ReplayVerifier::verify_dir`] / [`ReplayVerifier::verify_all`],
    /// whose fixtures are captured **completed** histories.
    Strict,
    /// Frontier-tolerant replay — a workflow that parks at the end of its
    /// recorded history replayed cleanly.
    ///
    /// Used by [`ReplayVerifier::replay_bundle`] (issue #798), whose fixtures are
    /// **in-flight** (non-terminal) executions. Such a run *always* suspends at
    /// its recorded frontier — that is its correct outcome, not a divergence — so
    /// a strict gate over this population would be false-red on every fixture.
    /// A genuine mid-history divergence still fails in this mode: it surfaces as
    /// `WorkflowOutcome::Failed { non_deterministic_details: Some(_) }`, and
    /// leftover unconsumed history at completion is still caught.
    InFlight,
}

/// Replay a single fixture file and return a [`FixtureResult`].
/// Build the single-use replayer one bundle fixture is replayed on.
///
/// Every replay-context value here is a *fallback* carried from the caller's
/// builder (Codex round-10 P2): the real #798 export writes `workflow_id`,
/// `parent_execution_id`, `execution_timeout` and `context_headers` onto the
/// snapshot, and `replay_from_snapshot` prefers the snapshot's own value — so
/// these only take effect for a hand-built or legacy fixture that omits the
/// metadata. Dropping them (as this construction used to) silently ignored
/// `WorkflowReplayer::with_workflow_id` and friends on the bundle path,
/// reporting drift for a workflow that never drifted — the same bug class the
/// issue #614 history-policy carry-through already fixed.
fn fixture_replayer(
    handlers: &HashMap<String, WorkflowHandlerFn>,
    state: SharedState,
    defaults: &FixtureReplayDefaults,
) -> WorkflowReplayer {
    WorkflowReplayer {
        handlers: handlers.clone(),
        state,
        context_headers: defaults.context_headers.clone(),
        payload_offloader: None,
        use_advancing_clock: false,
        metrics: std::sync::Arc::new(crate::telemetry::NoOpMetrics),
        // Issue #772 fallback: a snapshot carrying its own `execution_timeout`
        // still wins, so a deadline-aware fixture is validated either way.
        execution_timeout: defaults.execution_timeout,
        parent_execution_id: defaults.parent_execution_id,
        workflow_id: defaults.workflow_id.clone(),
        // Issue #798 fallback: a snapshot carrying its own `queue_name` still
        // wins, so an exported fixture is validated against its real queue.
        queue_name: defaults.queue_name.clone(),
        // Issue #798: NOT a fallback — the candidate build id is authoritative
        // for every fixture, because no snapshot carries one (by design: a
        // fixture-sourced build would replay the historical branch and hide the
        // candidate-only drift this gate exists to catch).
        build_id: defaults.build_id.clone(),
        // Issue #698: the directory-fixture path routes through
        // `replay_from_snapshot`, which always sources `execution_id` from the
        // snapshot (a required field); no raw-events global override applies.
        execution_id: None,
        // Issue #614: the bundle carries no history-policy field (it describes
        // executions, not the runtime that produced them), so the caller supplies
        // it via `ReplayVerifier::with_history_policy`. Defaults to the default
        // policy — unchanged pre-#614 behavior for anyone who never customized it.
        history_policy: defaults.history_policy,
        // Issue #798 (Codex round 20): like the build id, authoritative rather
        // than a fallback — the bundle describes executions, not the runtime that
        // will resume them, so the candidate's limits come from the caller.
        payload_limits: defaults.payload_limits,
        // Issue #798 (Codex round 21): same authoritative treatment — the live
        // worker registers these before any workflow code runs, so a replay that
        // omits them is not replaying the candidate.
        declarative_queries: defaults.declarative_queries.clone(),
        declarative_updates: defaults.declarative_updates.clone(),
        // Issue #798 (Codex round 22): the per-workflow input-cap overrides the
        // verifier retained from `register`. Each fixture resolves its own cap
        // from this map by workflow type, mirroring the live worker.
        workflow_input_caps: defaults.workflow_input_caps.clone(),
    }
}

async fn replay_fixture_file(
    handlers: &HashMap<String, WorkflowHandlerFn>,
    state: SharedState,
    path: &std::path::Path,
    timeout: std::time::Duration,
    allow_unregistered: bool,
    mode: FixtureReplayMode,
    defaults: &FixtureReplayDefaults,
) -> FixtureResult {
    // Read file.
    let json = match tokio::fs::read_to_string(path).await {
        Ok(s) => s,
        Err(e) => {
            return FixtureResult {
                path: path.to_owned(),
                workflow_name: String::new(),
                execution_id: None,
                status: FixtureStatus::HarnessError(HarnessErrorKind::InvalidFixture(format!(
                    "failed to read file: {e}"
                ))),
            };
        }
    };

    // Parse snapshot.
    let snapshot: HistorySnapshot = match serde_json::from_str(&json) {
        Ok(s) => s,
        Err(e) => {
            return FixtureResult {
                path: path.to_owned(),
                workflow_name: String::new(),
                execution_id: None,
                status: FixtureStatus::HarnessError(HarnessErrorKind::InvalidFixture(format!(
                    "invalid HistorySnapshot JSON: {e}"
                ))),
            };
        }
    };

    let workflow_name = snapshot.workflow_name.clone();
    let execution_id = snapshot.execution_id;

    // Refuse a fixture this gate cannot honestly evaluate, BEFORE replaying it.
    //
    // Both checks exist to stop the gate returning a *confidently wrong* verdict:
    // a redacted fixture would diverge on every activity input, and a fixture
    // aimed at the other gate would be judged in the wrong replay mode. Either
    // way the operator would be told something false about their code. A harness
    // error (exit 2) is the honest answer, and it names the fix.
    //
    // The two fields consulted are export-document metadata, not replay inputs,
    // so they are parsed from the same JSON via a side struct rather than living
    // on `HistorySnapshot`. A fixture that declares neither (hand-built, or a
    // legacy export) parses to all-`None` and is treated as replayable.
    let guard: FixtureGuardFields = serde_json::from_str(&json).unwrap_or_default();
    if let Some(reason) = unreplayable_fixture_reason(&guard, mode)
        .or_else(|| offloaded_fixture_reason(&json, &snapshot))
        .or_else(|| codec_opaque_fixture_reason(&json, &snapshot))
        .or_else(|| erased_fixture_reason(&json, &snapshot))
    {
        return FixtureResult {
            path: path.to_owned(),
            workflow_name,
            execution_id: Some(execution_id),
            status: FixtureStatus::HarnessError(HarnessErrorKind::InvalidFixture(reason)),
        };
    }

    // Check handler registration.
    if !handlers.contains_key(&workflow_name) {
        if allow_unregistered {
            return FixtureResult {
                path: path.to_owned(),
                status: FixtureStatus::Skipped {
                    reason: format!(
                        "workflow '{workflow_name}' not registered (--allow-unregistered)"
                    ),
                },
                workflow_name,
                execution_id: Some(execution_id),
            };
        }
        return FixtureResult {
            path: path.to_owned(),
            workflow_name,
            execution_id: Some(execution_id),
            status: FixtureStatus::HarnessError(HarnessErrorKind::UnregisteredWorkflow),
        };
    }

    let replayer = fixture_replayer(handlers, state, defaults);

    let replay_result = match mode {
        FixtureReplayMode::Strict => {
            tokio::time::timeout(timeout, replayer.replay_from_snapshot(snapshot)).await
        }
        FixtureReplayMode::InFlight => {
            tokio::time::timeout(timeout, replayer.replay_canary_snapshot(snapshot)).await
        }
    };

    let Ok(report) = replay_result else {
        return FixtureResult {
            path: path.to_owned(),
            workflow_name,
            execution_id: Some(execution_id),
            status: FixtureStatus::HarnessError(HarnessErrorKind::Timeout),
        };
    };

    let status = match report.status {
        ReplayStatus::ReplaySucceeded => FixtureStatus::Passed,
        other => FixtureStatus::Failed(other),
    };

    FixtureResult {
        path: path.to_owned(),
        workflow_name,
        execution_id: Some(execution_id),
        status,
    }
}

// ===========================================================================
// WorkflowTestEnv  — in-process unit-test harness for workflow functions
// ===========================================================================
//
// Design notes
// ──────────────
// `WorkflowTestEnv` drives a workflow function to completion by repeatedly
// running it through the executor, processing the `WorkflowCommand`s emitted
// on each suspension, appending mock results to an in-memory event history,
// and re-running with the updated history.
//
// No Postgres, no worker process, no Docker — all side effects are satisfied
// by closures registered before the run.
//
// Execution order
// ───────────────
// Signals are ingested first, at task-prep: before each handler dispatch cycle
// every currently-queued signal is appended to history in queued order,
// mirroring production's `worker::ingest_due_timers_and_signals` (which ingests
// pending signals BEFORE the handler runs, NOT gated on a `WaitForSignal`). This
// makes a workflow that immediately drains/polls a pending signal
// (`drain_signals_raw` / `try_receive_signal`, with no prior blocking
// `wait_for_signal`) observe it (issue #775). A signal in history resolves a
// blocking `wait_for_signal` / signal-or-timer race synchronously, so it never
// emits a `WaitForSignal` command.
//
// On each suspension:
//   1. Regular and local activities are resolved immediately via registered
//      mocks (either per-call-count or general fallback).
//   2. Child-workflow spawns are resolved via registered child mocks.
//   3. A `WaitForSignal` command means the workflow is blocked on a signal that
//      was never queued (an available signal is already in history from
//      task-prep, so no command is emitted) — no progress is made for it.
//   4. Timers auto-fire when they suspend — a classic-timer select! branch only
//      suspends when its competing signal branch was not resolvable (the signal
//      already won the race synchronously from history otherwise).
//
// The loop terminates when the workflow returns `Completed` or `Failed`, or
// when no commands can be resolved (workflow stuck) or the iteration cap is
// reached.

/// Maximum number of executor iterations before declaring an infinite loop.
const MAX_TEST_ITERATIONS: usize = 1_000;

/// Synthetic host worker id auto-resolved for the internal worker-session
/// acquire activity (issue #606) when no explicit mock/`attempt_result` is
/// registered for it. Stable across a run and its replay -- see
/// `resolve_activity`.
const TEST_SESSION_HOST_WORKER_ID: &str = "test-worker";

/// Type alias for the mock closure stored in `WorkflowTestEnv`.
type MockFn = Arc<dyn Fn(Value) -> Result<Value, String> + Send + Sync>;

/// Accumulate a cycle's [`WorkflowCommand::RecordLog`] commands into the
/// run-wide durable-log view (issue #790).
///
/// Keyed by `seq` with **first-write-wins**, mirroring the store's
/// `UNIQUE (workflow_exec_id, seq)` + `ON CONFLICT DO NOTHING`: a cycle that is
/// re-driven at an unchanged history position re-mints the same `seq`, and the
/// re-emitted line collapses onto the one already recorded rather than
/// duplicating. The `BTreeMap` also gives emission (`seq`) ordering for free.
fn accumulate_recorded_logs(
    commands: &[WorkflowCommand],
    acc: &mut std::collections::BTreeMap<u64, RecordedLogLine>,
) {
    for cmd in commands {
        if let WorkflowCommand::RecordLog {
            seq,
            level,
            message,
        } = cmd
        {
            acc.entry(*seq).or_insert_with(|| RecordedLogLine {
                seq: *seq,
                level: *level,
                message: message.clone(),
            });
        }
    }
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// One durable workflow log line a test run would have persisted (issue #790).
///
/// This is the harness-side view of what the worker writes to
/// `harvest_workflow_logs`, surfaced by [`TestRunOutcome::recorded_logs`] so a
/// no-DB test can assert on an author's `ctx.log_*` output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedLogLine {
    /// Deterministic logical-position identity and total emission order — the
    /// same value that becomes the `seq` column (and the exactly-once dedup
    /// key) in production. **Not** a 0-based counter.
    pub seq: u64,
    /// Severity, as emitted through the `ctx.log_*` entry point.
    pub level: crate::context::WorkflowLogLevel,
    /// The author's message, already truncated to the policy's per-line byte
    /// cap on a UTF-8 character boundary (the context applies that cap, so the
    /// harness models it for free).
    pub message: String,
}

/// The outcome of a [`WorkflowTestEnv::run`] call.
///
/// Contains the workflow's final `result` (success or failure) and the full
/// ordered event log produced during the run.  Use [`TestRunOutcome::events`]
/// for event-log assertions and [`TestRunOutcome::replay_check`] to verify
/// that the produced history is replay-deterministic.
pub struct TestRunOutcome {
    /// The workflow's terminal result: `Ok(output)` or `Err(error_string)`.
    pub result: Result<Value, String>,
    /// The complete ordered event log built during the test run.
    events: Vec<WorkflowEvent>,
    /// Execution ID used for the run (stable for replay checks).
    exec_id: ExecutionId,
    /// Shared state from the test env — forwarded to `replay_check` so the
    /// replayer sees the same typed state the workflow saw during the run.
    state: SharedState,
    /// Construction-time `simulated_now` (= `WorkflowStarted` timestamp), used
    /// to compute `final_now()` and `elapsed()` (issue #526).
    start_time: DateTime<Utc>,
    /// Effective `execution_timeout` budget carried from the `WorkflowTestEnv`
    /// that produced this outcome (issue #772). `replay_check` re-applies it to
    /// the `WorkflowReplayer` it builds so the deadline branch is enabled during
    /// the self-check — otherwise a deadline-aware history's recorded
    /// `SideEffectRecorded{Now}` deadline probe is left unconsumed and reported
    /// as a FALSE non-determinism. `None` (the default / no-timeout run) leaves
    /// the replayer's deadline branch off, matching the live run.
    execution_timeout: Option<chrono::Duration>,
    /// The spawning-parent execution id carried from the `WorkflowTestEnv` that
    /// produced this outcome (issue #698). `replay_check` re-applies it so a
    /// **child** workflow whose command-affecting control flow branches on
    /// `ctx.info().parent_execution_id` self-checks deterministically — otherwise
    /// the replayer sees `parent = None` and the child's parent-taken branch is
    /// reported as a FALSE non-determinism. `None` (the default) models a
    /// top-level run.
    parent_execution_id: Option<ExecutionId>,
    /// The logical workflow type name carried from the producing `WorkflowTestEnv`
    /// (issue #698, Codex P2). `replay_check` uses it for BOTH the replay
    /// snapshot's `workflow_name` AND the handler-registration key — otherwise the
    /// replay context reports `ctx.info().workflow_type == "__test__"` while the
    /// live run recorded the configured value (e.g. embedded in an activity
    /// input), producing a FALSE non-determinism in the harness's own self-check.
    /// Defaults to `""` (matching the live run's default), so the live run and its
    /// replay always agree whatever the value.
    workflow_name: String,
    /// The business-level `workflow_id` carried from the producing
    /// `WorkflowTestEnv` (issue #698, Codex P2). Like `workflow_name`, it lives in
    /// no `WorkflowEvent`, so `replay_check` threads it onto the replay snapshot so
    /// a workflow that records `ctx.info().workflow_id` (e.g. in an activity input)
    /// self-checks deterministically instead of false-flagging on the live value
    /// vs the replay's empty default. Defaults to `""` (matching the live run).
    workflow_id: String,
    /// The task queue carried from the producing `WorkflowTestEnv` (issue #798).
    /// Like `workflow_name`/`workflow_id`, the live run receives it via `span_meta`
    /// while it lives in no `WorkflowEvent`, so `replay_check` threads it onto the
    /// replay snapshot — otherwise a workflow that records `ctx.queue_name()` (e.g.
    /// in an activity input) self-checks against `""` and false-flags
    /// non-determinism. Defaults to `""` (matching the live run).
    queue_name: String,
    /// Issue #798: the configured build id, threaded into the live run via
    /// span metadata AND onto the replay snapshot, so the harness self-check
    /// stays symmetric for a build-gated workflow.
    build_id: Option<String>,
    /// The durable log lines this run would have persisted (issue #790), in
    /// `seq` order and de-duplicated by `seq` — exactly the shape the store's
    /// `UNIQUE (workflow_exec_id, seq)` + `ON CONFLICT DO NOTHING` produces.
    /// Empty unless the env opted in via [`WorkflowTestEnv::with_log_policy`].
    recorded_logs: Vec<RecordedLogLine>,
}

/// Reconstruct the final virtual-clock elapsed (in seconds) from the durable
/// timers that **actually fired** in `events` (issue #768, Codex P2 rounds 8 and
/// 16).
///
/// Simulates the virtual clock as a monotonic `now`: each `TimerStarted` for an
/// id is enqueued (FIFO) with the CURRENT `now` as its deadline **anchor**; a
/// `TimerFired` for that id dequeues the earliest pending arm and advances
/// `now = max(now, anchor + duration)`; a `TimerCancelled` dequeues (and
/// discards) the earliest pending arm without advancing. In a valid history an
/// id has at most one pending arm at a time (arm → fire/cancel → re-arm → …), so
/// this counts exactly the fired arms and excludes cancelled / reset ones.
///
/// The `max`-of-deadlines model (round 16) handles both timer shapes correctly:
/// - **Sequential** timers (each armed *after* the previous fired) have a later
///   anchor, so their deadlines chain and the result is the SUM of durations —
///   including every classic `ctx.timer` (each parks the workflow, so it always
///   arms after the previous one fired).
/// - **Concurrently-armed** cancellable timers (one `await_fire` batch /
///   overlapping arm windows) share an anchor (no fire advanced `now` between
///   their `TimerStarted`s), so the result is the MAX deadline — matching
///   production, where all deadlines in one batch start at the same instant.
///
/// This is the read-side counterpart to the live-clock advance in
/// `WorkflowContext::await_timer_fire`; the two must agree at terminal.
fn fired_timer_duration_secs(events: &[WorkflowEvent]) -> u64 {
    use std::collections::{HashMap, VecDeque};
    // Per-id FIFO queue of pending arms, each carrying its deadline anchor
    // (the `now` at which it was armed).
    let mut pending: HashMap<&str, VecDeque<u64>> = HashMap::new();
    let mut now: u64 = 0;
    for event in events {
        match event {
            WorkflowEvent::TimerStarted {
                timer_id,
                duration_secs,
            } => {
                // Deadline = anchor (now) + duration. Store the deadline directly.
                pending
                    .entry(timer_id.as_str())
                    .or_default()
                    .push_back(now.saturating_add(*duration_secs));
            }
            WorkflowEvent::TimerFired { timer_id } => {
                if let Some(deadline) = pending
                    .get_mut(timer_id.as_str())
                    .and_then(VecDeque::pop_front)
                {
                    now = now.max(deadline);
                }
            }
            WorkflowEvent::TimerCancelled { timer_id } => {
                if let Some(queue) = pending.get_mut(timer_id.as_str()) {
                    queue.pop_front();
                }
            }
            _ => {}
        }
    }
    now
}

impl TestRunOutcome {
    /// Returns a reference to the ordered event log.
    ///
    /// Use this to assert ordering invariants such as `ActivityCompleted` for
    /// `charge_card` came after `SignalReceived(approve)`.
    #[must_use]
    pub fn events(&self) -> &[WorkflowEvent] {
        &self.events
    }

    /// The durable workflow log lines this run would have persisted, in
    /// emission (`seq`) order (issue #790).
    ///
    /// Empty unless the producing env opted in via
    /// [`WorkflowTestEnv::with_log_policy`] — which is exactly the sink-disabled
    /// contract (AC6): `ctx.log_*` still works and still reaches `tracing`, it
    /// simply records nothing durable.
    ///
    /// **Fidelity.** The harness models the two properties an author's test can
    /// meaningfully assert on: per-message byte truncation (applied by the
    /// context) and exactly-once-per-`seq` de-duplication (the store's
    /// `UNIQUE (workflow_exec_id, seq)` + `ON CONFLICT DO NOTHING`, reproduced
    /// here as first-write-wins). It deliberately does **not** model the
    /// per-execution line cap or its truncation marker — those are store-layer
    /// behaviours covered by the database-backed tests.
    #[must_use]
    pub fn recorded_logs(&self) -> &[RecordedLogLine] {
        &self.recorded_logs
    }

    /// The virtual "now" at the end of the run (issue #526).
    ///
    /// Computed as `start_time + max`-of-deadlines over the durable timers that
    /// **actually fired** along the taken path — each `TimerStarted` anchors a
    /// deadline (`now-at-arm + duration`) and every matching `TimerFired` (per id,
    /// FIFO) advances the virtual clock to `max(now, deadline)`. Sequential timers
    /// therefore SUM (each armed after the previous fired) and concurrently-armed
    /// cancellable timers take the MAX (they share an anchor, matching production
    /// where all deadlines in one `await_fire` batch start at the same instant —
    /// issue #768, Codex P2 round 16). A `TimerStarted` that was cancelled or reset
    /// never fired, so its deadline is **excluded** — otherwise a cancelled/reset
    /// arm would advance the virtual clock even though the workflow never observed
    /// its fire, disagreeing with `ctx.now()` (Codex P2 round 8). Signal-preempted
    /// timers produce no `TimerStarted` event and likewise do not advance this
    /// clock. The classic `ctx.timer` path always fires and is always sequential,
    /// so the sum is unchanged from the pre-round-16 model.
    #[must_use]
    pub fn final_now(&self) -> DateTime<Utc> {
        let total_secs: u64 = fired_timer_duration_secs(&self.events);
        self.start_time
            + chrono::Duration::seconds(
                i64::try_from(total_secs)
                    .unwrap_or(i64::MAX / 1000)
                    .min(i64::MAX / 1000),
            )
    }

    /// Total virtual time elapsed during the run (issue #526).
    ///
    /// Equivalent to `final_now() - start_time`.
    #[must_use]
    pub fn elapsed(&self) -> chrono::Duration {
        self.final_now() - self.start_time
    }

    /// Run the recorded event history through [`WorkflowReplayer`] and return
    /// the replay report.
    ///
    /// If the workflow function is deterministic, this will always return
    /// [`ReplayStatus::ReplaySucceeded`].  A failure here means the workflow
    /// code is non-deterministic and would cause problems in production replay.
    ///
    /// This check is free — it reuses the event history already produced by
    /// the test run, so there is no extra DB or network call.
    ///
    /// The advancing virtual clock is enabled automatically (issue #526) so
    /// that time-branching workflows — those that branch on `ctx.now()` —
    /// replay correctly without a false `ReplayStatus::Failed`.
    ///
    /// The effective `execution_timeout` budget the producing
    /// [`WorkflowTestEnv`] used (issue #772) is carried into the replayer so the
    /// deadline branch is enabled during the self-check. Without it, a
    /// deadline-aware history — one whose `should_continue_as_new()` recorded a
    /// `SideEffectRecorded{Now}` deadline probe — would replay against a
    /// deadline-disabled context, leave that probe unconsumed, and report a
    /// FALSE non-determinism.
    pub async fn replay_check(&self, handler: WorkflowHandlerFn) -> ReplayReport {
        let snapshot = crate::testing::HistorySnapshot {
            // Issue #698 (Codex P2): use the producing env's configured workflow
            // type name for BOTH the snapshot `workflow_name` (so the replay
            // context reports the same `ctx.info().workflow_type` the live run
            // recorded) AND the handler-registration key below — the two MUST be
            // the same string or the replayer cannot find the handler. Was
            // hardcoded `"__test__"`, which mismatched a live run configured via
            // `with_workflow_name(...)` and false-flagged non-determinism.
            workflow_name: self.workflow_name.clone(),
            execution_id: self.exec_id,
            events: self.events.clone(),
            context_headers: None,
            // Carry the test env's deadline budget on the snapshot too (issue
            // #772); `replay_from_snapshot` prefers it, and the global
            // `with_execution_timeout` below is retained as an equivalent
            // fallback for older callers.
            execution_timeout: self.execution_timeout,
            deadline_at: None,
            // Issue #698: carry the spawning-parent id onto the snapshot so a
            // parent-aware child self-checks deterministically (matches the live
            // run, which received it via span_meta).
            parent_execution_id: self.parent_execution_id,
            // Issue #698 (Codex P2): carry the producing env's business
            // `workflow_id` so a workflow recording `ctx.info().workflow_id`
            // (e.g. in an activity input) self-checks deterministically. `None`
            // (empty env default) resolves to `""` in the replay context, which
            // matches the live run's empty default.
            workflow_id: if self.workflow_id.is_empty() {
                None
            } else {
                Some(self.workflow_id.clone())
            },
            // Issue #798: same reasoning as `workflow_id` above — the live run
            // receives the env's configured queue via span_meta, so a workflow
            // recording `ctx.queue_name()` (e.g. in an activity input) would
            // self-check against `""` and false-flag non-determinism without this.
            queue_name: if self.queue_name.is_empty() {
                None
            } else {
                Some(self.queue_name.clone())
            },
        };
        let mut replayer = WorkflowReplayer::new()
            .with_shared_state(self.state.clone())
            // Register under the SAME name carried on the snapshot above so the
            // replayer's handler lookup (keyed by `snapshot.workflow_name`)
            // resolves — consistently changed together with the snapshot name.
            .register_fn(self.workflow_name.clone(), handler)
            .with_advancing_timer_clock();
        if let Some(execution_timeout) = self.execution_timeout {
            replayer = replayer.with_execution_timeout(execution_timeout);
        }
        // Issue #798: carry the producing env's build id. Unlike `workflow_id` /
        // `queue_name` above this rides the *replayer*, not the snapshot, because
        // `HistorySnapshot` deliberately carries no build id (a fixture-sourced
        // build would be the recording build, hiding candidate-only drift). The
        // symmetry still matters here: the live run saw this build via span_meta,
        // so the self-check must replay under it or a build-gated workflow
        // false-flags non-determinism.
        if let Some(build_id) = self.build_id.clone() {
            replayer = replayer.with_build_id(build_id);
        }
        replayer.replay_from_snapshot(snapshot).await
    }
}

// ---------------------------------------------------------------------------
// WorkflowTestEnv
// ---------------------------------------------------------------------------

/// In-process unit-test harness for `#[workflow]` functions.
///
/// Run a workflow to completion without Postgres, workers, or Docker.
/// Activities are satisfied by registered closures; timers auto-fire;
/// signals are injected from a pre-queued list; child workflows are stubbed.
///
/// # Quick start
///
/// ```rust,no_run
/// # use autumn_harvest::testing::WorkflowTestEnv;
/// # use autumn_harvest::context::WorkflowContext;
/// # use serde_json::{Value, json};
/// # use std::pin::Pin;
/// # fn my_workflow<'a>(ctx: &'a WorkflowContext, _: Value)
/// #   -> Pin<Box<dyn std::future::Future<Output=Result<Value,String>>+Send+'a>>
/// # { Box::pin(async move { Ok(json!(null)) }) }
/// # #[tokio::main] async fn main() {
/// let outcome = WorkflowTestEnv::new()
///     .mock_activity("send_email", |_| Ok(json!("delivered")))
///     .run(my_workflow, json!({"user_id": 1}))
///     .await;
///
/// assert_eq!(outcome.result, Ok(json!("delivered")));
/// # }
/// ```
pub struct WorkflowTestEnv {
    /// Fallback mocks: activity name → closure(input) → result.
    activity_mocks: HashMap<String, MockFn>,
    /// Per-call-count mocks: (name, 1-based call number) → result.
    ///
    /// "Call number" is the number of times the workflow has issued a command
    /// for this activity name (across all iterations).  This corresponds to
    /// explicit workflow-level retries, not worker-level retry attempts.
    attempt_results: HashMap<(String, u32), Result<Value, String>>,
    /// Worker-level retry sequences: activity name → queue of per-invocation
    /// attempt result lists. Each inner `Vec` models one scheduling of the
    /// activity with multiple worker-level retry attempts.
    ///
    /// Registered via `mock_activity_retries`. When a `ScheduleActivity`
    /// command is processed, the first queued sequence for that name is popped
    /// and each result is emitted as a separate `ActivityFailed` (with
    /// incrementing `attempt` numbers) or a terminal `ActivityCompleted`.
    retry_sequences: HashMap<String, std::collections::VecDeque<Vec<Result<Value, String>>>>,
    /// Child-workflow stubs: workflow name → closure(input) → result.
    child_mocks: HashMap<String, MockFn>,
    /// Simulated wall-clock time.  Used as the `WorkflowStarted` timestamp so
    /// `ctx.now()` inside the workflow function is deterministic.
    simulated_now: DateTime<Utc>,
    /// Signals pre-queued for delivery when the workflow calls `wait_for_signal`.
    queued_signals: Vec<(String, Value)>,
    /// If `Some`, a `WorkflowCancelled` event is prepended to the history so
    /// `ctx.is_cancelled()` returns `true` from the first execution cycle.
    cancellation_reason: Option<String>,
    /// Shared typed state injected into the `WorkflowContext`.
    state: SharedState,
    /// Simulated last-completion-result for testing incremental scheduled jobs (issue #488).
    /// Injected as `last_completion_result` into the seed `WorkflowStarted` event.
    last_completion_result: Option<serde_json::Value>,
    /// Simulated last-error for testing incremental scheduled jobs (issue #488).
    last_error: Option<String>,
    /// Simulated scheduled fire-time (logical slot) for testing scheduled workflows (issue #508).
    scheduled_time: Option<chrono::DateTime<chrono::Utc>>,
    /// Metrics recorder injected into the `WorkflowContext` on each iteration.
    /// Defaults to `NoOpMetrics`; inject a counting recorder to assert that
    /// `ctx.metrics()` calls fire exactly once per logical occurrence.
    metrics: std::sync::Arc<dyn crate::telemetry::MetricsRecorder>,
    /// Logical workflow type name threaded into the `WorkflowContext` so
    /// engine metrics emitted from inside the workflow (e.g. the saga
    /// compensation counters, issue #801) carry a real `workflow` label.
    /// Defaults to `""` (matching legacy contexts).
    workflow_name: String,
    /// Business-level `workflow_id` threaded into the `WorkflowContext` (issue
    /// #698) so a no-DB test can prove `ctx.info().workflow_id` reports the
    /// configured value. Lives in no `WorkflowEvent`, so it is also carried onto
    /// the `replay_check` snapshot to keep the harness's own replay self-check
    /// consistent. Defaults to `""` (matching legacy contexts).
    workflow_id: String,
    /// Task queue name threaded into the `WorkflowContext` so in-context
    /// engine metrics carry a real `queue` label. Defaults to `""`.
    queue_name: String,
    /// Issue #798: the configured build id, threaded into the live run via
    /// span metadata AND onto the replay snapshot, so the harness self-check
    /// stays symmetric for a build-gated workflow.
    build_id: Option<String>,
    /// Effective `execution_timeout` budget threaded into the `WorkflowContext`
    /// (issue #772) so a run can exercise deadline-aware `should_continue_as_new`.
    /// `None` (the default) matches a workflow with no execution timeout.
    execution_timeout: Option<chrono::Duration>,
    /// Durable per-execution log policy (issue #790) threaded into the
    /// `WorkflowContext` this env builds. `None` (the default) reproduces a
    /// deployment with the durable sink DISABLED, so `ctx.log_*` records
    /// nothing durable -- exactly today's behaviour.
    workflow_log_policy: Option<crate::context::WorkflowLogPolicy>,
    /// Spawning parent's execution id threaded into the `WorkflowContext`
    /// (issue #698) so a no-DB test can prove `ctx.info().parent_execution_id` /
    /// `ctx.parent_execution_id()`. `None` (the default) models a top-level run.
    parent_execution_id: Option<ExecutionId>,
    /// Canned `await_external_workflow` outcomes (issue #757): target → the
    /// target's `COMPLETED` output. An awaited target present here resolves to
    /// `Ok(output)`.
    external_await_results: HashMap<ExecutionId, Value>,
    /// Canned `await_external_workflow` failure outcomes (issue #757): target →
    /// the target's terminal cause (`reason_code`, message, typed fields). An
    /// awaited target present here resolves to the typed `Err` branch. Takes
    /// precedence over `external_await_results` if both are set for a target.
    external_await_failures: HashMap<ExecutionId, ExternalAwaitFailureFixture>,
    /// Mutex keys (issue #691) whose grant the harness WITHHOLDS: an
    /// `AcquireMutex` for a key in this set records nothing and leaves the
    /// workflow parked (the same "blocked on an unavailable event" signal an
    /// unqueued `WaitForSignal` uses), modelling a lock already held by another
    /// contender. A key NOT in this set is auto-granted. Populated via
    /// [`with_mutex_contended`](Self::with_mutex_contended).
    mutex_contended: HashSet<String>,
}

/// A canned `await_external_workflow` failure outcome (issue #757):
/// `(reason_code, message, error_type, details, non_retryable)`.
type ExternalAwaitFailureFixture = (
    String,
    Option<String>,
    Option<String>,
    Option<Value>,
    Option<bool>,
);

impl Default for WorkflowTestEnv {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkflowTestEnv {
    // ── Construction ─────────────────────────────────────────────────────

    /// Create an empty test environment.
    #[must_use]
    pub fn new() -> Self {
        Self {
            activity_mocks: HashMap::new(),
            attempt_results: HashMap::new(),
            retry_sequences: HashMap::new(),
            child_mocks: HashMap::new(),
            simulated_now: Utc::now(),
            queued_signals: Vec::new(),
            build_id: None,
            cancellation_reason: None,
            state: empty_shared_state(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
            metrics: std::sync::Arc::new(crate::telemetry::NoOpMetrics),
            workflow_name: String::new(),
            workflow_id: String::new(),
            queue_name: String::new(),
            execution_timeout: None,
            workflow_log_policy: None,
            parent_execution_id: None,
            external_await_results: HashMap::new(),
            external_await_failures: HashMap::new(),
            mutex_contended: HashSet::new(),
        }
    }

    // ── Fluent builder ───────────────────────────────────────────────────

    /// Register a fallback mock for an activity (or local activity) by name.
    ///
    /// The closure receives the deserialized input payload and must return the
    /// activity result.  This mock is used for every call whose call-number
    /// does not have a [`mock_activity_attempt`](Self::mock_activity_attempt)
    /// registered.
    ///
    /// The same mock covers both `execute_activity_raw` and
    /// `execute_local_activity_raw` — the name is the only routing key.
    #[must_use]
    pub fn mock_activity<F>(mut self, name: impl Into<String>, mock: F) -> Self
    where
        F: Fn(Value) -> Result<Value, String> + Send + Sync + 'static,
    {
        self.activity_mocks.insert(name.into(), Arc::new(mock));
        self
    }

    /// Register a result for a specific per-call invocation of an activity.
    ///
    /// `call_number` is 1-based and counts how many times the workflow code
    /// has called `execute_activity_raw` / `execute_local_activity_raw` for
    /// this activity name.  This lets you test explicit workflow-level retry
    /// logic:
    ///
    /// ```rust,no_run
    /// # use autumn_harvest::testing::WorkflowTestEnv;
    /// # use serde_json::json;
    /// let env = WorkflowTestEnv::new()
    ///     .mock_activity_attempt("charge_card", 1, Err("transient".into()))
    ///     .mock_activity_attempt("charge_card", 2, Ok(json!({"status": "charged"})));
    /// ```
    #[must_use]
    pub fn mock_activity_attempt(
        mut self,
        name: impl Into<String>,
        call_number: u32,
        result: Result<Value, String>,
    ) -> Self {
        self.attempt_results
            .insert((name.into(), call_number), result);
        self
    }

    /// Simulate worker-level retry attempts for one activity invocation.
    ///
    /// Each element in `attempts` is the result of one worker-level attempt for a
    /// **single** `execute_activity_raw` call from the workflow.  Mirrors real
    /// worker behavior: each attempt emits `ActivityStarted`; non-terminal
    /// failures call `requeue_for_retry` (no event written); the last failure
    /// emits `ActivityFailed { non_retryable: true }`; success emits
    /// `ActivityCompleted`.  The replay engine skips `ActivityStarted`, so the
    /// workflow sees only the final terminal outcome.
    ///
    /// This models the case where the activity succeeds on attempt 3 of 3:
    ///
    /// ```rust,no_run
    /// # use autumn_harvest::testing::WorkflowTestEnv;
    /// # use serde_json::json;
    /// let env = WorkflowTestEnv::new()
    ///     .mock_activity_retries("charge_card", vec![
    ///         Err("transient_1".into()),
    ///         Err("transient_2".into()),
    ///         Ok(json!({"status": "charged"})),
    ///     ]);
    /// ```
    ///
    /// The resulting history contains:
    /// - `ActivityScheduled`
    /// - `ActivityStarted` (attempt 1 — skipped by replay engine)
    /// - `ActivityStarted` (attempt 2 — skipped by replay engine)
    /// - `ActivityStarted` (attempt 3 — skipped by replay engine)
    /// - `ActivityCompleted`
    ///
    /// Calling this method multiple times for the same activity name registers
    /// independent sequences consumed in FIFO order.
    ///
    /// # Panics
    ///
    /// Panics if `attempts` is empty, since an empty sequence would leave the
    /// activity without a terminal event and silently hang the test.
    #[must_use]
    pub fn mock_activity_retries(
        mut self,
        name: impl Into<String>,
        attempts: Vec<Result<Value, String>>,
    ) -> Self {
        assert!(
            !attempts.is_empty(),
            "mock_activity_retries requires at least one attempt"
        );
        self.retry_sequences
            .entry(name.into())
            .or_default()
            .push_back(attempts);
        self
    }

    /// Stub a child workflow by name.
    ///
    /// When the workflow calls `ctx.spawn_child_workflow_raw("name", input)`,
    /// the closure is invoked instead of actually running the child.
    #[must_use]
    pub fn mock_child_workflow<F>(mut self, name: impl Into<String>, mock: F) -> Self
    where
        F: Fn(Value) -> Result<Value, String> + Send + Sync + 'static,
    {
        self.child_mocks.insert(name.into(), Arc::new(mock));
        self
    }

    /// Pre-queue a signal for delivery when the workflow calls
    /// `ctx.wait_for_signal(name)`.
    ///
    /// Signals are delivered in the order they are queued, matched by name.
    /// Queuing a signal for name "approve" will satisfy the first
    /// `wait_for_signal("approve")` the workflow issues.
    #[must_use]
    pub fn queue_signal(mut self, name: impl Into<String>, payload: Value) -> Self {
        self.queued_signals.push((name.into(), payload));
        self
    }

    /// Mark a mutex `key` (issue #691) as already held, so the harness
    /// WITHHOLDS the grant: an `ctx.mutex(key).acquire()` for this key never
    /// records a `MutexGranted` and the workflow stays parked (modelling
    /// contention with another holder). A key not marked contended is
    /// auto-granted on first acquire.
    #[must_use]
    pub fn with_mutex_contended(mut self, key: impl Into<String>) -> Self {
        self.mutex_contended.insert(key.into());
        self
    }

    /// Configure `ctx.await_external_workflow(target)` to resolve to `Ok(output)`
    /// (the target reached `COMPLETED` with `output`) — issue #757.
    #[must_use]
    pub fn with_external_await_result(mut self, target: ExecutionId, output: Value) -> Self {
        self.external_await_results.insert(target, output);
        self
    }

    /// Configure `ctx.await_external_workflow(target)` to resolve to a typed
    /// `Err` (the target reached a non-`COMPLETED` terminal state) — issue #757.
    ///
    /// `reason_code` is one of `target_failed`/`target_timed_out`/
    /// `target_cancelled`/`target_terminated`. The optional typed fields carry a
    /// [`crate::failure::WorkflowFailure`]-style cause; pass `None`s for an
    /// untyped failure.
    #[must_use]
    pub fn with_external_await_failure(
        mut self,
        target: ExecutionId,
        reason_code: impl Into<String>,
        message: Option<String>,
        error_type: Option<String>,
        details: Option<Value>,
        non_retryable: Option<bool>,
    ) -> Self {
        self.external_await_failures.insert(
            target,
            (
                reason_code.into(),
                message,
                error_type,
                details,
                non_retryable,
            ),
        );
        self
    }

    /// Inject a `WorkflowCancelled` event so `ctx.is_cancelled()` returns
    /// `true` and `ctx.check_cancellation()` returns `Err(Cancelled(...))`.
    ///
    /// The cancellation is visible from the very first execution cycle.
    #[must_use]
    pub fn with_cancellation(mut self, reason: impl Into<String>) -> Self {
        self.cancellation_reason = Some(reason.into());
        self
    }

    /// Seed the test environment with a prior successful run's result, as if the
    /// same schedule had previously completed with `value` as its output.
    ///
    /// The value is frozen into the seed `WorkflowStarted` event, mirroring
    /// `ctx.last_completion_result::<T>()` in production scheduled runs.
    ///
    /// # Panics
    /// Panics if `value` cannot be serialized (unreachable for well-formed types).
    #[must_use]
    pub fn with_last_completion_result<T: serde::Serialize>(mut self, value: T) -> Self {
        self.last_completion_result =
            Some(serde_json::to_value(value).expect("last_completion_result must be serializable"));
        self
    }

    /// Seed the test environment with a prior run's error, as if the most recent
    /// terminal run ended with `FAILED` or `TIMED_OUT`.
    ///
    /// Mirrors `ctx.last_error()` in production scheduled runs.
    #[must_use]
    pub fn with_last_error(mut self, error: impl Into<String>) -> Self {
        self.last_error = Some(error.into());
        self
    }

    /// Seed the test environment with a nominal scheduled fire-time (logical slot),
    /// as if this run was fired by the scheduler for a specific time slot.
    ///
    /// Mirrors `ctx.scheduled_time()` in production scheduled runs (issue #508).
    #[must_use]
    pub const fn with_scheduled_time(mut self, slot: chrono::DateTime<chrono::Utc>) -> Self {
        self.scheduled_time = Some(slot);
        self
    }

    /// Inject a [`MetricsRecorder`](crate::telemetry::MetricsRecorder) into the
    /// `WorkflowContext` used on each test iteration.
    ///
    /// Use this to assert that `ctx.metrics()` calls fire exactly once per
    /// logical occurrence across the iterations `WorkflowTestEnv` drives.
    #[must_use]
    pub fn with_metrics(
        mut self,
        metrics: std::sync::Arc<dyn crate::telemetry::MetricsRecorder>,
    ) -> Self {
        self.metrics = metrics;
        self
    }

    /// Set the logical workflow type name for the contexts this env builds,
    /// so engine metrics emitted from inside the workflow — e.g. the saga
    /// compensation counters (issue #801) — carry a real `workflow` label
    /// instead of the legacy `""` default. Pairs with
    /// [`with_metrics`](Self::with_metrics) for label-content assertions.
    #[must_use]
    pub fn with_workflow_name(mut self, name: impl Into<String>) -> Self {
        self.workflow_name = name.into();
        self
    }

    /// Set the business-level `workflow_id` for the contexts this env builds
    /// (issue #698), so a no-DB test can prove `ctx.info().workflow_id` reports
    /// the configured value. `workflow_id` lives in no `WorkflowEvent`, so it is
    /// threaded into the live run via span-meta and carried onto the
    /// [`replay_check`](TestRunOutcome::replay_check) snapshot, keeping the
    /// harness's own replay self-check consistent for a workflow that records
    /// `ctx.info().workflow_id` (e.g. in an activity input). Defaults to `""`.
    #[must_use]
    pub fn with_workflow_id(mut self, workflow_id: impl Into<String>) -> Self {
        self.workflow_id = workflow_id.into();
        self
    }

    /// Set the task queue name for the contexts this env builds, so
    /// in-context engine metrics carry a real `queue` label instead of the
    /// legacy `""` default. Pairs with [`with_metrics`](Self::with_metrics).
    #[must_use]
    pub fn with_queue_name(mut self, queue: impl Into<String>) -> Self {
        self.queue_name = queue.into();
        self
    }

    /// Set the worker build id for the contexts this env builds (issue #798), so
    /// a no-DB test can exercise a workflow whose control flow branches on
    /// `ctx.build_id()` — the build-routing pattern (issue #171) a replay gate
    /// exists to protect.
    ///
    /// Threaded into the live run via span metadata **and** carried onto the
    /// [`replay_check`](TestRunOutcome::replay_check) snapshot. Both halves are
    /// load-bearing: setting it on only the live side would make the harness's
    /// own replay self-check report false non-determinism for a build-gated
    /// workflow, which is precisely the asymmetry that made `queue_name` a bug.
    /// Defaults to `None`, matching a worker with no build id configured.
    #[must_use]
    pub fn with_build_id(mut self, build_id: impl Into<String>) -> Self {
        self.build_id = Some(build_id.into());
        self
    }

    /// Set the spawning parent's execution id for the contexts this env builds
    /// (issue #698), so a no-DB test can prove `ctx.info().parent_execution_id`
    /// / `ctx.parent_execution_id()` report the configured parent. `None` (the
    /// default) models a top-level run with no parent.
    #[must_use]
    pub const fn with_parent_execution_id(mut self, parent: Option<ExecutionId>) -> Self {
        self.parent_execution_id = parent;
        self
    }

    /// Set the effective `execution_timeout` budget for the contexts this env
    /// builds (issue #772), so `ctx.deadline()` /
    /// `ctx.should_continue_as_new()` can reason about deadline-aware
    /// continue-as-new inside a live test run.
    #[must_use]
    pub const fn with_execution_timeout(mut self, execution_timeout: chrono::Duration) -> Self {
        self.execution_timeout = Some(execution_timeout);
        self
    }

    /// Enable the **durable per-execution log sink** (issue #790) for the
    /// contexts this env builds, so `ctx.log_info` / `log_warn` / `log_error`
    /// produce the bookkeeping commands the worker persists -- readable back
    /// via [`TestRunOutcome::recorded_logs`].
    ///
    /// `None` (the default) reproduces a deployment with the sink DISABLED:
    /// `ctx.log_*` still works and still reaches `tracing`, but records
    /// nothing durable. That is the opt-in boundary, so a test can assert both
    /// halves without a database.
    #[must_use]
    pub const fn with_log_policy(
        mut self,
        policy: Option<crate::context::WorkflowLogPolicy>,
    ) -> Self {
        self.workflow_log_policy = policy;
        self
    }

    /// Inject typed shared state accessible via `ctx.state::<T>()` inside the
    /// workflow function.
    ///
    /// # Panics
    ///
    /// Panics if the internal `Arc` has been cloned — unreachable in normal
    /// builder usage.
    #[must_use]
    pub fn with_state<T: Send + Sync + 'static>(mut self, value: T) -> Self {
        std::sync::Arc::get_mut(&mut self.state)
            .expect("state Arc has no other references during WorkflowTestEnv construction")
            .insert(std::any::TypeId::of::<T>(), Box::new(value));
        self
    }

    /// Return the construction-time simulated wall-clock (`WorkflowStarted` timestamp).
    ///
    /// This is the **starting** value of the virtual clock, not the final value.
    /// Inside the workflow function `ctx.now()` advances with each durable timer
    /// that fires (issue #526); use [`TestRunOutcome::final_now`] to read the
    /// clock after all timers have fired, or [`TestRunOutcome::elapsed`] for the
    /// total virtual time elapsed.
    ///
    /// [`TestRunOutcome::final_now`]: crate::testing::TestRunOutcome::final_now
    /// [`TestRunOutcome::elapsed`]: crate::testing::TestRunOutcome::elapsed
    #[must_use]
    pub const fn now(&self) -> DateTime<Utc> {
        self.simulated_now
    }

    // ── Execution ────────────────────────────────────────────────────────

    /// Run the workflow function to completion and return the outcome.
    ///
    /// The workflow is executed in a loop:
    /// 1. Run the workflow with the current history.
    /// 2. If suspended: resolve each command (activities, timers, signals,
    ///    child workflows) and append events to history.
    /// 3. Repeat until `Completed`, `Failed`, or stuck.
    ///
    /// Pending signals are ingested into history at task-prep (before each
    /// handler dispatch), so a signal wins a `tokio::select!` race by resolving
    /// its branch synchronously from history; a classic timer only fires when it
    /// genuinely suspends (the no-signal, timer-wins case).
    #[allow(clippy::too_many_lines)]
    pub async fn run(&self, handler: WorkflowHandlerFn, input: Value) -> TestRunOutcome {
        let exec_id = ExecutionId::new();

        let mut history = vec![WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: self.simulated_now,
            last_completion_result: self.last_completion_result.clone(),
            last_error: self.last_error.clone(),
            scheduled_time: self.scheduled_time,
        }];
        if let Some(reason) = &self.cancellation_reason {
            history.push(WorkflowEvent::WorkflowCancelled {
                reason: reason.clone(),
            });
        }

        let mut call_counts: HashMap<String, u32> = HashMap::new();
        let mut remaining_signals = self.queued_signals.clone();
        let mut retry_sequences = self.retry_sequences.clone();
        // Issue #790: the durable log lines this run would have persisted,
        // accumulated across every decision cycle and de-duplicated by `seq`
        // exactly the way the store's unique index does.
        let mut recorded_logs: std::collections::BTreeMap<u64, RecordedLogLine> =
            std::collections::BTreeMap::new();

        let start_time = self.simulated_now;

        // Thread the configured workflow/queue names into the context (via
        // the executor's span-meta plumbing, the same path the worker uses)
        // so engine metrics emitted from inside the workflow carry real
        // labels a test can assert on.
        let span_meta = if self.workflow_name.is_empty()
            && self.workflow_id.is_empty()
            && self.queue_name.is_empty()
            && self.execution_timeout.is_none()
            && self.parent_execution_id.is_none()
            // Issue #798: without this a `with_build_id`-only env falls into the
            // `None` arm and the configured build never reaches the live context,
            // so `ctx.build_id()` reports `None` on both sides — self-consistent,
            // but silently ignoring the caller's setting.
            && self.build_id.is_none()
        {
            None
        } else {
            Some(WorkflowExecuteSpanMeta {
                workflow_name: self.workflow_name.clone(),
                // Issue #698: thread the configured business `workflow_id` so
                // `ctx.info().workflow_id` reports it inside the live test run
                // (was hardcoded empty).
                workflow_id: self.workflow_id.clone(),
                shard_id: 0,
                queue_name: self.queue_name.clone(),
                is_replay: false,
                link_traceparent: None,
                // Issue #798: thread the configured build id so `ctx.build_id()`
                // reports it inside the live test run (was hardcoded `None`).
                build_id: self.build_id.clone(),
                // Issue #772: thread the deadline budget so a live test run can
                // exercise deadline-aware continue-as-new.
                execution_timeout: self.execution_timeout,
                // The test-env carries only the timeout; `ctx.deadline()` falls
                // back to `start + execution_timeout` (no resume/redrive shift
                // to model here).
                deadline_at: None,
                // Issue #698: thread the configured parent so `ctx.info()` /
                // `ctx.parent_execution_id()` report it inside the test run.
                parent_execution_id: self.parent_execution_id,
            })
        };

        for _iter in 0..MAX_TEST_ITERATIONS {
            // Task-prep ingest (issue #775, Codex P2): mirror production's
            // `worker::ingest_due_timers_and_signals`, which appends every
            // pending signal into history *before* the workflow handler runs
            // for this task pickup — NOT gated on the workflow emitting a
            // `WaitForSignal`. Draining all currently-queued signals here (in
            // queued order) makes a workflow that *starts* by non-blockingly
            // draining/polling a pending signal (`drain_signals_raw` /
            // `try_receive_signal`, with no prior blocking `wait_for_signal`)
            // observe it, matching production. All signals are queued up front
            // via `queue_signal`, so this drains on the first iteration and is a
            // no-op on every resume cycle thereafter.
            for (name, payload) in std::mem::take(&mut remaining_signals) {
                history.push(WorkflowEvent::SignalReceived {
                    signal_name: name,
                    payload,
                });
            }

            let (outcome, pending_cmds, _span) = run_workflow_with_state_advancing_clock(
                exec_id,
                history.clone(),
                handler,
                input.clone(),
                self.state.clone(),
                span_meta.as_ref(),
                self.metrics.clone(),
                // Issue #790: the durable-log policy this env was configured
                // with (`None` = the sink disabled, the default).
                self.workflow_log_policy,
            )
            .await;

            // Issue #790: harvest this cycle's durable-log commands before the
            // outcome is consumed. A suspension carries them on
            // `outcome.commands`; a terminal cycle carries them on
            // `pending_cmds` (the executor returns an empty `pending_cmds` for
            // a suspension), so collecting from both covers every cycle shape.
            if let WorkflowOutcome::Suspended { commands } = &outcome {
                accumulate_recorded_logs(commands, &mut recorded_logs);
            }
            accumulate_recorded_logs(&pending_cmds, &mut recorded_logs);

            match outcome {
                WorkflowOutcome::Suspended { commands } => {
                    let made_progress = match self.process_suspension(
                        commands,
                        &mut history,
                        &mut call_counts,
                        &mut retry_sequences,
                    ) {
                        Ok(p) => p,
                        Err(e) => {
                            return TestRunOutcome {
                                result: Err(e),
                                events: history,
                                exec_id,
                                state: self.state.clone(),
                                start_time,
                                execution_timeout: self.execution_timeout,
                                // Issue #698: carry the spawning-parent id for replay_check.
                                parent_execution_id: self.parent_execution_id,
                                // Issue #698 (Codex P2): carry the configured
                                // workflow type / business id for replay_check.
                                workflow_name: self.workflow_name.clone(),
                                workflow_id: self.workflow_id.clone(),
                                // Issue #798: carry the env's task queue so `replay_check`
                                // self-checks against the same queue the live run saw.
                                queue_name: self.queue_name.clone(),
                                // Issue #798: likewise carry the env's build id, so a build-gated
                                // workflow's self-check replays under the same build the live run saw.
                                build_id: self.build_id.clone(),
                                recorded_logs: recorded_logs.into_values().collect(),
                            };
                        }
                    };
                    if !made_progress {
                        return TestRunOutcome {
                            result: Err("WorkflowTestEnv: workflow suspended with no resolvable \
                                 commands (check that all signals are queued and activities \
                                 are mocked)"
                                .to_string()),
                            events: history,
                            exec_id,
                            state: self.state.clone(),
                            start_time,
                            execution_timeout: self.execution_timeout,
                            // Issue #698: carry the spawning-parent id for replay_check.
                            parent_execution_id: self.parent_execution_id,
                            // Issue #698 (Codex P2): carry the configured workflow
                            // type / business id for replay_check.
                            workflow_name: self.workflow_name.clone(),
                            workflow_id: self.workflow_id.clone(),
                            // Issue #798: carry the env's task queue so `replay_check`
                            // self-checks against the same queue the live run saw.
                            queue_name: self.queue_name.clone(),
                            // Issue #798: likewise carry the env's build id, so a build-gated
                            // workflow's self-check replays under the same build the live run saw.
                            build_id: self.build_id.clone(),
                            recorded_logs: recorded_logs.into_values().collect(),
                        };
                    }
                }
                terminal => {
                    return self.finish_terminal_outcome(
                        terminal,
                        &pending_cmds,
                        history,
                        exec_id,
                        start_time,
                        recorded_logs.into_values().collect(),
                    );
                }
            }
        }

        TestRunOutcome {
            result: Err(format!(
                "WorkflowTestEnv: workflow exceeded {MAX_TEST_ITERATIONS} iterations \
                 (possible infinite loop or unresolvable suspension)"
            )),
            events: history,
            exec_id,
            state: self.state.clone(),
            start_time,
            execution_timeout: self.execution_timeout,
            // Issue #698: carry the spawning-parent id for replay_check.
            parent_execution_id: self.parent_execution_id,
            // Issue #698 (Codex P2): carry the configured workflow type /
            // business id for replay_check.
            workflow_name: self.workflow_name.clone(),
            workflow_id: self.workflow_id.clone(),
            // Issue #798: carry the env's task queue so `replay_check`
            // self-checks against the same queue the live run saw.
            queue_name: self.queue_name.clone(),
            // Issue #798: likewise carry the env's build id, so a build-gated
            // workflow's self-check replays under the same build the live run saw.
            build_id: self.build_id.clone(),
            recorded_logs: recorded_logs.into_values().collect(),
        }
    }

    fn finish_terminal_outcome(
        &self,
        outcome: WorkflowOutcome,
        pending_cmds: &[WorkflowCommand],
        mut history: Vec<WorkflowEvent>,
        exec_id: ExecutionId,
        start_time: DateTime<Utc>,
        recorded_logs: Vec<RecordedLogLine>,
    ) -> TestRunOutcome {
        Self::record_terminal_pending_commands(pending_cmds, &mut history);
        let should_record_cascades = matches!(
            outcome,
            WorkflowOutcome::Completed { .. } | WorkflowOutcome::Failed { .. }
        );
        let result = match outcome {
            WorkflowOutcome::Completed { output, .. } => {
                history.push(WorkflowEvent::WorkflowCompleted {
                    output: output.clone(),
                });
                Ok(output)
            }
            WorkflowOutcome::Failed { error, .. } => {
                // Decode the failure payload so a `#[workflow] -> Result<_, WorkflowFailure>`
                // run records the typed `error_type`/`details`/`non_retryable` fields and
                // surfaces the human message (not the raw `harvest_workflow_failure_v1`
                // envelope), matching the worker and simulator paths (issue #767, Codex P2).
                let decoded = crate::failure::decode_workflow_failure(&error);
                history.push(WorkflowEvent::workflow_failed_typed(&decoded));
                Err(decoded.message)
            }
            WorkflowOutcome::ContinuedAsNew {
                input,
                new_workflow_type,
            } => {
                // A cross-type continuation (issue #803) is recorded faithfully
                // so `replay_check` round-trips the target type; the harness
                // stops at the transition either way and never dispatches the
                // successor, so it needs no handler for the target type.
                history.push(WorkflowEvent::WorkflowContinuedAsNew {
                    new_exec_id: ExecutionId::new(),
                    input: input.clone(),
                    new_workflow_type,
                });
                Ok(input)
            }
            WorkflowOutcome::Suspended { .. } => {
                unreachable!("suspended outcomes are handled in run")
            }
        };
        if should_record_cascades {
            Self::record_terminal_parent_close_cascades(&mut history);
        }

        TestRunOutcome {
            result,
            events: history,
            exec_id,
            state: self.state.clone(),
            start_time,
            execution_timeout: self.execution_timeout,
            // Issue #698: carry the spawning-parent id for replay_check.
            parent_execution_id: self.parent_execution_id,
            // Issue #698 (Codex P2): carry the configured workflow type /
            // business id for replay_check.
            workflow_name: self.workflow_name.clone(),
            workflow_id: self.workflow_id.clone(),
            // Issue #798: carry the env's task queue so `replay_check`
            // self-checks against the same queue the live run saw.
            queue_name: self.queue_name.clone(),
            // Issue #798: likewise carry the env's build id, so a build-gated
            // workflow's self-check replays under the same build the live run saw.
            build_id: self.build_id.clone(),
            recorded_logs,
        }
    }

    fn record_terminal_pending_commands(
        commands: &[WorkflowCommand],
        history: &mut Vec<WorkflowEvent>,
    ) {
        for cmd in commands {
            match cmd {
                WorkflowCommand::RecordMarker { name, details } => {
                    history.push(WorkflowEvent::MarkerRecorded {
                        name: name.clone(),
                        details: details.clone(),
                    });
                }
                WorkflowCommand::RecordSideEffect { kind, name, value } => {
                    history.push(WorkflowEvent::SideEffectRecorded {
                        kind: *kind,
                        name: name.clone(),
                        value: value.clone(),
                    });
                }
                WorkflowCommand::SpawnDetachedChildWorkflow {
                    child_id,
                    workflow_name,
                    input,
                    parent_close_policy,
                } => {
                    history.push(WorkflowEvent::ChildWorkflowSpawnedDetached {
                        child_id: *child_id,
                        workflow_name: workflow_name.clone(),
                        input: input.clone(),
                        parent_close_policy: *parent_close_policy,
                    });
                }
                // Cancellable/renewable timer bookkeeping on a terminal cycle
                // (issue #768): a sealing run never awaits, so any arm is a fresh
                // arm (`for_await: false`) that records TimerStarted (idempotent);
                // a `for_await: true` re-arm cannot reach a terminal cycle (await
                // parks) and records nothing. Cancel records TimerCancelled. No
                // fire is deferred — the run is sealing.
                WorkflowCommand::ArmTimer {
                    timer_id,
                    duration_secs,
                    for_await: false,
                } => {
                    if !Self::timer_is_active_in_history(history, timer_id) {
                        history.push(WorkflowEvent::TimerStarted {
                            timer_id: timer_id.clone(),
                            duration_secs: *duration_secs,
                        });
                    }
                }
                WorkflowCommand::CancelTimer { timer_id } => {
                    history.push(WorkflowEvent::TimerCancelled {
                        timer_id: timer_id.clone(),
                    });
                }
                // Durably cancel the losing branches of a resolved race
                // (issue #600 / #779) that resolved on the terminal cycle: append
                // a synthetic terminal for each still-open loser so a subsequent
                // replay resolves it to a terminal instead of looping on
                // `ActivityInProgress`/`ChildInProgress`. Mirrors the suspension
                // path's `CancelRaceLosers` handling in `process_command`. Timers
                // carry no history footprint in the harness.
                WorkflowCommand::CancelRaceLosers {
                    activities,
                    children,
                    timers: _,
                } => {
                    for activity_id in activities {
                        history.push(WorkflowEvent::ActivityFailed {
                            activity_id: *activity_id,
                            error: "lost race to a sibling branch".to_string(),
                            attempt: 1,
                            error_type: "Error".to_string(),
                            non_retryable: true,
                            details: None,
                        });
                    }
                    for child_id in children {
                        history.push(WorkflowEvent::child_workflow_failed(
                            *child_id,
                            "lost race to a sibling branch".to_string(),
                        ));
                    }
                }
                // A `for_await: true` re-arm cannot reach a terminal cycle (await
                // parks), so it records nothing here.
                _ => {}
            }
        }
    }

    fn record_terminal_parent_close_cascades(history: &mut Vec<WorkflowEvent>) {
        let cascaded_children = history
            .iter()
            .filter_map(|event| match event {
                WorkflowEvent::ChildWorkflowSpawnedDetached {
                    child_id,
                    parent_close_policy,
                    ..
                } if *parent_close_policy != ParentClosePolicy::Abandon => {
                    let action = match parent_close_policy {
                        ParentClosePolicy::Abandon => unreachable!("filtered above"),
                        ParentClosePolicy::RequestCancel => "request_cancel",
                        ParentClosePolicy::Terminate => "terminate",
                    };
                    Some((*child_id, *parent_close_policy, action.to_string()))
                }
                _ => None,
            })
            .collect::<Vec<_>>();

        for (child_id, policy, action) in cascaded_children {
            history.push(WorkflowEvent::ChildWorkflowCascadeApplied {
                child_id,
                policy,
                action,
            });
        }
    }

    /// Process one suspension batch: resolve commands and append events.
    ///
    /// Returns `Ok(true)` if at least one command was resolved, `Ok(false)` if
    /// no progress was made, or `Err(msg)` if a harness configuration error was
    /// encountered (e.g. a missing activity mock or child-workflow stub).
    fn process_suspension(
        &self,
        commands: Vec<WorkflowCommand>,
        history: &mut Vec<WorkflowEvent>,
        call_counts: &mut HashMap<String, u32>,
        retry_sequences: &mut HashMap<
            String,
            std::collections::VecDeque<Vec<Result<Value, String>>>,
        >,
    ) -> Result<bool, String> {
        // Whether this batch also carries a genuine non-timer suspension
        // (activity/signal/child-workflow/classic timer). In production the
        // worker records an `ArmTimer` and leaves the workflow parked on that
        // other wait — it only reschedules the armed cancellable timer to fire
        // on the bookkeeping-only `await_fire` path. When a competing suspension
        // is present the harness must NOT auto-fire the armed timer, or it would
        // synthesise impossible history such as
        // `[TimerStarted, ActivityScheduled, TimerFired, ActivityCompleted]`
        // (Codex P2, issue #768).
        let batch_has_competing_suspension = commands.iter().any(Self::is_competing_suspension);

        // Deadline-ordered firing for concurrent awaited cancellable timers
        // (issue #768, Codex P2 round 15). Production inserts every
        // `for_await: true` row, reschedules the parked task to the MINIMUM
        // `fires_at`, and on the next claim ingests only DUE timers in `fires_at`
        // order. So in a bookkeeping-only batch with multiple awaited arms, only
        // the arm(s) with the smallest deadline fire this cycle; a strictly-later
        // timer stays parked and fires on a subsequent cycle when the clock reaches
        // it. All await arms in one batch are armed at the same instant, so the
        // minimum `fires_at` is simply the minimum `duration_secs`. Without this,
        // `tokio::join!(slow.await_fire(), fast.await_fire())` would record
        // `TimerFired(slow)` before `TimerFired(fast)` in poll order — an ordering
        // a live worker (deadline-ordered ingest) can never produce.
        let min_await_deadline_secs = commands
            .iter()
            .filter_map(|cmd| match cmd {
                WorkflowCommand::ArmTimer {
                    duration_secs,
                    for_await: true,
                    ..
                } => Some(*duration_secs),
                _ => None,
            })
            .min();

        // A child-workflow-vs-deadline race (issue #779) suspends on a single
        // mixed `StartChildWorkflow + StartTimer` batch whose timer id carries
        // the `__child_timeout:` prefix. When no child mock is registered the
        // child "hangs" and the deadline timer fires (the timeout branch),
        // exactly like an unqueued `receive_signal_timeout`; the harness must
        // record the child start with no terminal instead of erroring on the
        // missing mock.
        let batch_is_child_timeout_race = commands.iter().any(|cmd| {
            matches!(
                cmd,
                WorkflowCommand::StartTimer { timer_id, .. }
                    if timer_id.as_str().starts_with("__child_timeout:")
            )
        });

        let mut made_progress = false;
        let mut deferred_events = Vec::new();
        for cmd in commands {
            made_progress |= self.process_command(
                cmd,
                batch_has_competing_suspension,
                batch_is_child_timeout_race,
                min_await_deadline_secs,
                history,
                &mut deferred_events,
                call_counts,
                retry_sequences,
            )?;
        }
        history.extend(deferred_events);
        Ok(made_progress)
    }

    /// Whether a command is a genuine non-timer suspension that would keep the
    /// workflow parked on an external event (activity result, signal, child
    /// result, or a classic `ctx.timer` fire) — as opposed to author-controlled
    /// cancellable-timer bookkeeping (`ArmTimer`/`CancelTimer`) or an inline
    /// local activity. Used to gate cancellable-timer auto-firing in the harness
    /// so it only fires on the bookkeeping-only `await_fire` path, matching the
    /// real worker (Codex P2, issue #768).
    const fn is_competing_suspension(cmd: &WorkflowCommand) -> bool {
        matches!(
            cmd,
            WorkflowCommand::ScheduleActivity { .. }
                | WorkflowCommand::WaitForActivity { .. }
                | WorkflowCommand::ScheduleExternalActivity { .. }
                | WorkflowCommand::WaitForSignal { .. }
                | WorkflowCommand::StartChildWorkflow { .. }
                | WorkflowCommand::StartTimer { .. }
        )
    }

    /// Resolve a single workflow command and append the resulting events.
    ///
    /// Returns `Ok(true)` when a command produced progress, `Ok(false)` when
    /// the command was a no-op, or `Err(msg)` when a mock/stub lookup failed
    /// (harness configuration error — the test must be fixed, not the workflow).
    #[allow(clippy::too_many_lines, clippy::too_many_arguments)]
    fn process_command(
        &self,
        cmd: WorkflowCommand,
        batch_has_competing_suspension: bool,
        batch_is_child_timeout_race: bool,
        min_await_deadline_secs: Option<u64>,
        history: &mut Vec<WorkflowEvent>,
        deferred_events: &mut Vec<WorkflowEvent>,
        call_counts: &mut HashMap<String, u32>,
        retry_sequences: &mut HashMap<
            String,
            std::collections::VecDeque<Vec<Result<Value, String>>>,
        >,
    ) -> Result<bool, String> {
        match cmd {
            WorkflowCommand::ScheduleActivity {
                activity_id,
                name,
                input: act_input,
                queue,
                ..
            } => {
                history.push(WorkflowEvent::ActivityScheduled {
                    activity_id,
                    name: name.clone(),
                    input: act_input.clone(),
                    queue,
                });
                // Worker-level retry sequence takes priority over per-call mocks.
                // Increment the per-name call counter regardless so that any
                // subsequent workflow-level calls for the same activity name see
                // the correct call number when resolved against per-call mocks.
                let call_num = Self::next_call_count(call_counts, &name);
                if let Some(seq) = retry_sequences.get_mut(&name)
                    && let Some(attempts) = seq.pop_front()
                {
                    Self::push_activity_retry_sequence(deferred_events, activity_id, attempts);
                    return Ok(true);
                }
                let result = self.resolve_activity(&name, act_input, call_num)?;
                Self::push_activity_terminal(deferred_events, activity_id, result);
                Ok(true)
            }

            WorkflowCommand::RunLocalActivity {
                activity_id,
                name,
                input: act_input,
                retry_policy,
                start_to_close,
                ..
            } => {
                let call_num = Self::next_call_count(call_counts, &name);
                let result = self.resolve_activity(&name, act_input.clone(), call_num)?;
                history.push(WorkflowEvent::LocalActivityScheduled {
                    activity_id,
                    name: name.clone(),
                    input: act_input,
                    // Issue #620 (Codex P2): mirror the worker anchor — this is a
                    // #620+ schedule, so record the resolution marker plus the
                    // resolved retry/STC the command carried, so harness-based
                    // replay fixtures observe the same frozen budget/timeout the
                    // worker writes. STC frozen as full-precision nanos.
                    resolved: true,
                    retry_policy,
                    start_to_close_nanos: start_to_close
                        .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)),
                });
                Self::push_local_activity_terminal(deferred_events, activity_id, result);
                Ok(true)
            }

            WorkflowCommand::StartTimer {
                timer_id,
                duration_secs,
                ..
            } => {
                // A classic-timer suspension in a select! only reaches here when
                // its competing signal branch is NOT resolvable — a queued signal
                // is now ingested into history at task-prep (see `run`, issue
                // #775 Codex P2), so a signal that should win the race already
                // resolved the select synchronously without ever suspending. When
                // the timer genuinely suspends, it fires (timer wins).
                history.push(WorkflowEvent::TimerStarted {
                    timer_id: timer_id.clone(),
                    duration_secs,
                });
                deferred_events.push(WorkflowEvent::TimerFired { timer_id });
                Ok(true)
            }

            WorkflowCommand::WaitForSignal { .. } => {
                // Signals are now ingested into history at task-prep (see the
                // pre-dispatch drain in `run`, issue #775 Codex P2), mirroring
                // production's `worker::ingest_due_timers_and_signals`. A
                // `WaitForSignal` command therefore only reaches here when the
                // awaited signal was NOT queued (it is already in history and
                // resolved by the matcher otherwise, so the workflow never emits
                // the command). This is the "blocked on an unavailable signal"
                // case: no progress. The old WaitForSignal-gated batch promotion
                // is removed — draining queued signals here would double-append
                // signals already ingested at task-prep, and it never fired for a
                // drain-first workflow that emits no `WaitForSignal`.
                Ok(false)
            }

            WorkflowCommand::StartChildWorkflow {
                child_id,
                workflow_name,
                input: child_input,
                ..
            } => {
                let result = match self.resolve_child(&workflow_name, child_input.clone()) {
                    Ok(result) => result,
                    // A child-timeout race (issue #779) with no registered mock:
                    // the child "hangs", the deadline timer wins. Record the
                    // start with no terminal so `match_child_or_timer` resolves
                    // to the timeout branch instead of failing on the missing
                    // mock. The `StartTimer` in the same batch supplies progress.
                    Err(harness_err) => {
                        if batch_is_child_timeout_race {
                            history.push(WorkflowEvent::ChildWorkflowStarted {
                                child_id,
                                workflow_name,
                                input: child_input,
                            });
                            return Ok(false);
                        }
                        return Err(harness_err);
                    }
                };
                history.push(WorkflowEvent::ChildWorkflowStarted {
                    child_id,
                    workflow_name,
                    input: child_input,
                });
                match result {
                    Ok(output) => {
                        deferred_events
                            .push(WorkflowEvent::ChildWorkflowCompleted { child_id, output });
                    }
                    Err(error) => {
                        // Decode the child mock's error so a typed
                        // `WorkflowFailure` envelope surfaces the child's
                        // `error_type`/`details`/`non_retryable` to the parent
                        // (issue #767), mirroring the worker's
                        // `wake_parent_for_child_failure`. A plain string decodes
                        // to all-None typed fields (unchanged legacy behaviour).
                        let decoded = crate::failure::decode_workflow_failure(&error);
                        deferred_events.push(WorkflowEvent::child_workflow_failed_typed(
                            child_id, &decoded,
                        ));
                    }
                }
                Ok(true)
            }

            WorkflowCommand::SignalExternalWorkflow {
                signal_id,
                target,
                signal_name,
                payload,
                result_tx,
                already_requested,
                idempotency_key,
            } => {
                if !already_requested {
                    history.push(WorkflowEvent::ExternalSignalRequested {
                        signal_id,
                        target,
                        signal_name,
                        payload,
                        idempotency_key,
                    });
                }
                history.push(WorkflowEvent::ExternalSignalDelivered { signal_id });
                let _ = result_tx.send(Ok(()));
                Ok(true)
            }

            // Cancel always succeeds in the test harness (no DB, target always
            // treated as reachable and alive).
            WorkflowCommand::RequestCancelExternalWorkflow {
                cancel_id,
                target,
                result_tx,
                already_requested,
            } => {
                if !already_requested {
                    history.push(WorkflowEvent::ExternalCancelRequested { cancel_id, target });
                }
                history.push(WorkflowEvent::ExternalCancelDelivered { cancel_id });
                let _ = result_tx.send(Ok(()));
                Ok(true)
            }

            // Await resolves from the configured canned outcome for the target
            // (issue #757): a `with_external_await_failure` entry → the typed
            // `Err` branch; else a `with_external_await_result` entry (or, if
            // unconfigured, a `COMPLETED` with `Null` output) → the `Ok` branch.
            WorkflowCommand::AwaitExternalWorkflow {
                await_id,
                target,
                result_tx,
                already_requested,
            } => {
                if !already_requested {
                    history.push(WorkflowEvent::ExternalAwaitRequested { await_id, target });
                }
                if let Some((reason_code, message, error_type, details, non_retryable)) =
                    self.external_await_failures.get(&target).cloned()
                {
                    history.push(WorkflowEvent::ExternalAwaitFailed {
                        await_id,
                        reason_code,
                        message,
                        error_type,
                        details,
                        non_retryable,
                    });
                } else {
                    let output = self
                        .external_await_results
                        .get(&target)
                        .cloned()
                        .unwrap_or(Value::Null);
                    history.push(WorkflowEvent::ExternalAwaitResolved { await_id, output });
                }
                let _ = result_tx.send(Ok(()));
                Ok(true)
            }

            // Detached child spawn: record the event in history so replay can return
            // the same child_id. The simulator does not create actual child executions
            // — it just simulates the parent's history as if the child was spawned.
            WorkflowCommand::SpawnDetachedChildWorkflow {
                child_id,
                workflow_name,
                input,
                parent_close_policy,
            } => {
                history.push(WorkflowEvent::ChildWorkflowSpawnedDetached {
                    child_id,
                    workflow_name,
                    input,
                    parent_close_policy,
                });
                Ok(true)
            }

            // Deterministic side-effect capture (system_now/new_uuid/random_*/
            // side_effect) emitted before a suspending command. The real worker
            // persists these via build_suspension_events, so the harness must do
            // the same — otherwise the next replay iteration sees the following
            // event where it expects SideEffectRecorded and reports spurious drift.
            // Pushed to `history` (not deferred_events) to preserve command order
            // ahead of the suspending command's own scheduled event.
            WorkflowCommand::RecordSideEffect { kind, name, value } => {
                history.push(WorkflowEvent::SideEffectRecorded { kind, name, value });
                Ok(false)
            }

            // Markers (fan-out count guards, dag condition skips, etc.) must be
            // persisted to history so the next replay iteration finds them in the
            // same position as the real worker would.  Mirrors RecordSideEffect above.
            WorkflowCommand::RecordMarker { name, details } => {
                history.push(WorkflowEvent::MarkerRecorded { name, details });
                Ok(false)
            }

            // Durably cancel the losing branches of a resolved ctx.race()
            // (issue #600). Mirrors the real worker's `apply_race_loser_cancellations`:
            // append a synthetic terminal for each still-open loser so the next
            // replay iteration resolves it to a terminal instead of looping on
            // `ActivityInProgress`/`ChildInProgress`. Timers carry no history
            // footprint in the harness (no `harvest_timers` table to clean up).
            WorkflowCommand::CancelRaceLosers {
                activities,
                children,
                timers: _,
            } => {
                for activity_id in activities {
                    deferred_events.push(WorkflowEvent::ActivityFailed {
                        activity_id,
                        error: "lost race to a sibling branch".to_string(),
                        attempt: 1,
                        error_type: "Error".to_string(),
                        non_retryable: true,
                        details: None,
                    });
                }
                for child_id in children {
                    deferred_events.push(WorkflowEvent::child_workflow_failed(
                        child_id,
                        "lost race to a sibling branch".to_string(),
                    ));
                }
                Ok(true)
            }

            // Cancellable/renewable durable timer arm (issue #768, Codex P2
            // round 4). Two roles by `for_await`, mirroring the real worker's
            // `plan_timer_lifecycle`:
            //
            // - `for_await: false` (fresh arm from `start_timer`/`reset`): record
            //   `TimerStarted` at this command's position (dedup: skip if already
            //   active in history) and NEVER fire. A cancellable timer is only
            //   fire-eligible once awaited, so an armed-but-unawaited timer cannot
            //   fire while parked on a competing suspension — no impossible history
            //   such as `[TimerStarted, ActivityScheduled, TimerFired,
            //   ActivityCompleted]`.
            // - `for_await: true` (re-arm from `await_fire`): record NO event
            //   (the arm's `TimerStarted` was already recorded by the fresh arm).
            //   On a bookkeeping-only `await_fire` batch, fire now (defer
            //   `TimerFired`, no real sleep); on a competing suspension, stay
            //   parked and let a later bookkeeping-only cycle fire it.
            WorkflowCommand::ArmTimer {
                timer_id,
                duration_secs,
                for_await,
            } => {
                if for_await {
                    let fire_already_deferred = deferred_events.iter().any(|e| {
                        matches!(e, WorkflowEvent::TimerFired { timer_id: id } if *id == timer_id)
                    });
                    if fire_already_deferred {
                        // Another `await_fire` re-arm for this id already deferred
                        // the fire this batch — idempotent no-op.
                        return Ok(false);
                    }
                    if batch_has_competing_suspension {
                        // Awaited alongside a genuine non-timer suspension: parked,
                        // fires on a later bookkeeping-only cycle.
                        return Ok(false);
                    }
                    // Deadline order (round 15): only the minimum-deadline awaited
                    // timer(s) fire this cycle. A strictly-later awaited timer stays
                    // parked (no progress from it) and fires when a subsequent
                    // bookkeeping-only cycle re-arms it against the (now smaller) set
                    // of remaining awaits — matching production's min-`fires_at`
                    // reschedule + deadline-ordered ingest.
                    if min_await_deadline_secs.is_some_and(|min| duration_secs > min) {
                        return Ok(false);
                    }
                    deferred_events.push(WorkflowEvent::TimerFired { timer_id });
                    return Ok(true);
                }
                // Fresh arm: record TimerStarted (dedup by active-in-history), never
                // fire.
                if Self::timer_is_active_in_history(history, &timer_id) {
                    return Ok(false);
                }
                history.push(WorkflowEvent::TimerStarted {
                    timer_id,
                    duration_secs,
                });
                Ok(true)
            }

            // Cancellable/renewable durable timer cancel (issue #768). Record
            // TimerCancelled and drop the deferred TimerFired for this id so the
            // timer never fires — the cancelled branch is taken deterministically.
            WorkflowCommand::CancelTimer { timer_id } => {
                deferred_events.retain(
                    |e| !matches!(e, WorkflowEvent::TimerFired { timer_id: id } if *id == timer_id),
                );
                history.push(WorkflowEvent::TimerCancelled { timer_id });
                Ok(true)
            }

            // WaitForActivity: activity was scheduled in a previous iteration;
            // its terminal event is already in history and will be matched on replay.
            WorkflowCommand::WaitForActivity { .. }
            | WorkflowCommand::RecordUpdateResult { .. }
            | WorkflowCommand::UpsertSearchAttributes { .. }
            | WorkflowCommand::SetCurrentDetails { .. }
            // Ephemeral progress (issue #791): a bookkeeping no-op in the test
            // harness — appends no event, changes no history, drives no wait.
            | WorkflowCommand::PublishProgress { .. }
            // Durable per-execution logs (issue #790): event-less bookkeeping
            // (mirrors `SetCurrentDetails`). The worker persists it to
            // `harvest_workflow_logs` in production; the harness has no DB, so
            // nothing is recorded here and no progress is made.
            | WorkflowCommand::RecordLog { .. }
            | WorkflowCommand::ScheduleExternalActivity { .. }
            // Release is EVENT-LESS bookkeeping (mirrors `SetCurrentDetails`,
            // issue #691): the worker frees the lock in production, but the
            // harness has no lock table, so nothing is recorded. No progress.
            | WorkflowCommand::ReleaseMutex { .. }
            | WorkflowCommand::Complete { .. }
            | WorkflowCommand::Fail { .. }
            | WorkflowCommand::ContinueAsNew { .. } => Ok(false),

            // Issue #691 (ctx.mutex): acquire is a SOLO suspension. When the key
            // is marked contended (`with_mutex_contended`) the harness WITHHOLDS
            // the grant — records nothing and leaves the workflow parked, the
            // same "blocked on an unavailable event" signal an unqueued
            // `WaitForSignal` uses. Otherwise auto-grant: record a synthetic
            // `MutexGranted` with a monotonic `lock_seq` (count of prior grants
            // in history + 1) and the harness's deterministic clock. Because
            // acquire is solo, the grant is the resolving event and must land in
            // `history` so the next replay cycle's `match_mutex_granted` consumes
            // it and the workflow proceeds.
            WorkflowCommand::AcquireMutex { key, .. } => {
                if self.mutex_contended.contains(&key) {
                    return Ok(false);
                }
                let lock_seq = i64::try_from(
                    history
                        .iter()
                        .filter(|e| matches!(e, WorkflowEvent::MutexGranted { .. }))
                        .count(),
                )
                .unwrap_or(i64::MAX)
                .saturating_add(1);
                history.push(WorkflowEvent::MutexGranted {
                    key,
                    lock_seq,
                    acquired_at: self.simulated_now,
                });
                Ok(true)
            }
        }
    }

    /// Whether `timer_id` has an active (unfired, uncancelled) `TimerStarted`
    /// in `history` — used by the harness to make cancellable-timer arming
    /// idempotent (issue #768).
    fn timer_is_active_in_history(
        history: &[WorkflowEvent],
        timer_id: &crate::types::TimerId,
    ) -> bool {
        let mut active = false;
        for event in history {
            match event {
                WorkflowEvent::TimerStarted { timer_id: id, .. } if id == timer_id => active = true,
                WorkflowEvent::TimerFired { timer_id: id }
                | WorkflowEvent::TimerCancelled { timer_id: id }
                    if id == timer_id =>
                {
                    active = false;
                }
                _ => {}
            }
        }
        active
    }

    // ── Internal helpers ─────────────────────────────────────────────────

    /// Increment and return the per-name call counter (1-based).
    fn next_call_count(call_counts: &mut HashMap<String, u32>, name: &str) -> u32 {
        let count = call_counts.entry(name.to_string()).or_insert(0);
        *count += 1;
        *count
    }

    /// Resolve an activity (regular or local) using registered mocks.
    ///
    /// Per-call-count results take priority over the general fallback mock.
    ///
    /// Returns `Ok(activity_result)` when a mock is found (the inner `Result`
    /// is the mock's success/failure value), or `Err(harness_error)` when no
    /// mock is registered — a harness configuration problem that must be fixed
    /// in the test, not handled as a workflow-level failure.
    fn resolve_activity(
        &self,
        name: &str,
        input: Value,
        call_num: u32,
    ) -> Result<Result<Value, String>, String> {
        if let Some(result) = self.attempt_results.get(&(name.to_string(), call_num)) {
            return Ok(result.clone());
        }
        if let Some(mock) = self.activity_mocks.get(name) {
            return Ok(mock(input));
        }
        // Worker sessions (issue #606): the internal acquire/release
        // activities have no registered handler in production either -- the
        // real worker intercepts them by reserved name before regular
        // dispatch. Auto-resolve them here to a stable synthetic worker id
        // so a session-using workflow needs no special mock for the happy
        // path. Register an explicit mock/attempt_result above (this check
        // runs first) to exercise acquisition-timeout or broken-session
        // behavior instead.
        if name == crate::context::SESSION_ACQUIRE_ACTIVITY_NAME {
            return Ok(Ok(serde_json::json!(TEST_SESSION_HOST_WORKER_ID)));
        }
        if name == crate::context::SESSION_RELEASE_ACTIVITY_NAME {
            return Ok(Ok(Value::Null));
        }
        Err(format!(
            "WorkflowTestEnv: no mock registered for activity '{name}' \
             (call {call_num}). Register one with mock_activity() or \
             mock_activity_attempt()."
        ))
    }

    /// Resolve a child workflow using registered stubs.
    ///
    /// Returns `Ok(child_result)` when a stub is found, or `Err(harness_error)`
    /// when no stub is registered — must be fixed in the test.
    fn resolve_child(&self, name: &str, input: Value) -> Result<Result<Value, String>, String> {
        if let Some(mock) = self.child_mocks.get(name) {
            return Ok(mock(input));
        }
        Err(format!(
            "WorkflowTestEnv: no mock registered for child workflow '{name}'. \
             Register one with mock_child_workflow()."
        ))
    }

    /// Simulate a worker-level retry sequence for one activity scheduling.
    ///
    /// Mirrors the real worker: each attempt emits `ActivityStarted`; non-
    /// terminal failures call `requeue_for_retry` (no event); the last failure
    /// emits `ActivityFailed { non_retryable: true }`; success emits
    /// `ActivityCompleted`.  The replay engine skips `ActivityStarted` events,
    /// so the workflow sees only the terminal outcome — identical to production.
    fn push_activity_retry_sequence(
        history: &mut Vec<WorkflowEvent>,
        activity_id: ActivityExecId,
        attempts: Vec<Result<Value, String>>,
    ) {
        let total = u32::try_from(attempts.len()).unwrap_or(u32::MAX);
        for (i, result) in attempts.into_iter().enumerate() {
            let attempt_num = u32::try_from(i).unwrap_or(u32::MAX).saturating_add(1);
            history.push(WorkflowEvent::ActivityStarted {
                activity_id,
                worker_id: WorkerId::new("test-worker"),
            });
            match result {
                Ok(output) => {
                    history.push(WorkflowEvent::ActivityCompleted {
                        activity_id,
                        output,
                    });
                    return;
                }
                Err(error) => {
                    // Parse the payload so we can honour typed non-retryable
                    // failures mid-sequence, matching production's
                    // next_retry_delay check which stops immediately for
                    // non_retryable payloads regardless of remaining budget.
                    let parsed = crate::failure::parse_error_payload_full(&error);
                    if attempt_num == total || parsed.non_retryable {
                        // Retry budget exhausted, or payload is explicitly
                        // non-retryable → emit the terminal ActivityFailed.
                        history.push(WorkflowEvent::ActivityFailed {
                            activity_id,
                            error: parsed.message,
                            attempt: attempt_num,
                            error_type: parsed.error_type,
                            non_retryable: parsed.non_retryable,
                            details: parsed.details,
                        });
                        return;
                    }
                    // Non-terminal retryable: requeue_for_retry stores the
                    // error in the task row but writes no event.
                }
            }
        }
    }

    /// Append `ActivityCompleted` or `ActivityFailed` to history.
    ///
    /// `attempt` is always 1 because each explicit call to `execute_activity_raw`
    /// represents a new scheduling — worker-level retries within one scheduling
    /// are not modelled by the test harness.
    fn push_activity_terminal(
        history: &mut Vec<WorkflowEvent>,
        activity_id: ActivityExecId,
        result: Result<Value, String>,
    ) {
        match result {
            Ok(output) => history.push(WorkflowEvent::ActivityCompleted {
                activity_id,
                output,
            }),
            Err(error) => history.push(WorkflowEvent::ActivityFailed {
                activity_id,
                error,
                attempt: 1,
                error_type: "Error".to_string(),
                non_retryable: false,
                details: None,
            }),
        }
    }

    /// Append `LocalActivityCompleted`, or `LocalActivityFailed` +
    /// `LocalActivityExhausted` to history.
    ///
    /// Production records one `LocalActivityFailed` per attempt before the
    /// terminal `LocalActivityExhausted`; the harness models a single attempt
    /// so it emits exactly one of each on failure.
    fn push_local_activity_terminal(
        history: &mut Vec<WorkflowEvent>,
        activity_id: ActivityExecId,
        result: Result<Value, String>,
    ) {
        match result {
            Ok(output) => history.push(WorkflowEvent::LocalActivityCompleted {
                activity_id,
                output,
            }),
            Err(error) => {
                history.push(WorkflowEvent::LocalActivityFailed {
                    activity_id,
                    error: error.clone(),
                    attempt: 1,
                });
                history.push(WorkflowEvent::LocalActivityExhausted {
                    activity_id,
                    error,
                    attempt: 1,
                });
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ActivityExecId;
    use crate::types::TimerId;
    use chrono::Utc;
    use std::future::Future;
    use std::pin::Pin;

    fn ts(id: &str, secs: u64) -> WorkflowEvent {
        WorkflowEvent::TimerStarted {
            timer_id: TimerId::new(id),
            duration_secs: secs,
        }
    }
    fn tf(id: &str) -> WorkflowEvent {
        WorkflowEvent::TimerFired {
            timer_id: TimerId::new(id),
        }
    }
    fn tc(id: &str) -> WorkflowEvent {
        WorkflowEvent::TimerCancelled {
            timer_id: TimerId::new(id),
        }
    }

    #[test]
    fn fired_timer_duration_counts_only_fired_arms() {
        // Classic timer: every TimerStarted fires — sum unchanged.
        assert_eq!(
            fired_timer_duration_secs(&[ts("t1", 30), tf("t1"), ts("t2", 5), tf("t2")]),
            35
        );
        // Reset (cancellable): TS(300)[cancelled], TS(600)[fired] → only 600.
        assert_eq!(
            fired_timer_duration_secs(&[ts("idle", 300), tc("idle"), ts("idle", 600), tf("idle")]),
            600
        );
        // Reset to same duration: only the fired arm counts (300, not 600).
        assert_eq!(
            fired_timer_duration_secs(&[ts("idle", 300), tc("idle"), ts("idle", 300), tf("idle")]),
            300
        );
        // Cancelled, never fired → 0.
        assert_eq!(fired_timer_duration_secs(&[ts("idle", 300), tc("idle")]), 0);
        // An older arm fired, a newer arm cancelled → only the fired one.
        assert_eq!(
            fired_timer_duration_secs(&[ts("a", 300), tf("a"), ts("a", 600), tc("a")]),
            300
        );
        // Signal-preempted timer records no TimerStarted → 0.
        assert_eq!(fired_timer_duration_secs(&[]), 0);
        // A lone TimerFired with no matching TimerStarted contributes nothing.
        assert_eq!(fired_timer_duration_secs(&[tf("ghost")]), 0);
    }

    #[test]
    fn concurrently_armed_timers_advance_to_the_max_deadline_not_the_sum() {
        // Issue #768, Codex P2 round 16. Two cancellable timers armed in one
        // `await_fire` batch (both `TimerStarted` before either `TimerFired`) share
        // a deadline anchor of `now = 0`, so the fired result is MAX(10, 1) = 10,
        // NOT the SUM 11. Round-15 fires the smaller deadline first, so the recorded
        // order is TS(slow), TS(fast), TF(fast), TF(slow).
        assert_eq!(
            fired_timer_duration_secs(&[ts("slow", 10), ts("fast", 1), tf("fast"), tf("slow")]),
            10,
            "concurrently-armed timers must advance to the MAX deadline"
        );
        // Order of the two TimerFired events is irrelevant to the MAX.
        assert_eq!(
            fired_timer_duration_secs(&[ts("slow", 10), ts("fast", 1), tf("slow"), tf("fast")]),
            10
        );
        // Sequential timers (each armed after the previous fired) still SUM.
        assert_eq!(
            fired_timer_duration_secs(&[ts("a", 10), tf("a"), ts("b", 1), tf("b")]),
            11,
            "sequential timers must still sum"
        );
        // Three concurrent → MAX of the three deadlines.
        assert_eq!(
            fired_timer_duration_secs(&[
                ts("a", 3),
                ts("b", 7),
                ts("c", 2),
                tf("c"),
                tf("a"),
                tf("b"),
            ]),
            7
        );
    }

    fn simple_workflow<'a>(
        _ctx: &'a crate::context::WorkflowContext,
        input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move { Ok(input) })
    }

    fn activity_workflow<'a>(
        ctx: &'a crate::context::WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let out = ctx
                .execute_activity_raw("do_work", Value::Null, "default")
                .await
                .map_err(|e| e.to_string())?;
            Ok(out)
        })
    }

    fn activity_events() -> (ExecutionId, Vec<WorkflowEvent>) {
        let exec_id = ExecutionId::new();
        let aid = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: aid,
                name: "do_work".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: aid,
                output: serde_json::json!("done"),
            },
        ];
        (exec_id, events)
    }

    #[tokio::test]
    async fn simple_replay_succeeds() {
        let events = vec![WorkflowEvent::WorkflowStarted {
            input: serde_json::json!("hi"),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }];
        let replayer = WorkflowReplayer::new().register_fn("simple", simple_workflow);
        let report = replayer.replay_from_events(events).await;
        assert!(matches!(report.status, ReplayStatus::ReplaySucceeded));
    }

    #[tokio::test]
    async fn activity_replay_succeeds() {
        let (_exec_id, events) = activity_events();
        let replayer = WorkflowReplayer::new().register_fn("activity", activity_workflow);
        let report = replayer.replay_from_events(events).await;
        assert!(matches!(report.status, ReplayStatus::ReplaySucceeded));
    }

    #[tokio::test]
    async fn replay_with_reset_replays_only_history_through_boundary() {
        let activity_id = ActivityExecId::new();
        let history = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "do_work".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::json!("done"),
            },
            WorkflowEvent::MarkerRecorded {
                name: "bad-branch-only".into(),
                details: Value::Null,
            },
        ];

        let replayer = WorkflowReplayer::new().register_fn("activity", activity_workflow);
        let report = replayer.replay_with_reset(history, 2).await;

        assert!(matches!(report.status, ReplayStatus::ReplaySucceeded));
        assert_eq!(report.events_replayed, 4);
    }

    #[tokio::test]
    async fn activity_mismatch_is_detected() {
        fn wrong_activity<'a>(
            ctx: &'a crate::context::WorkflowContext,
            _input: Value,
        ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
            Box::pin(async move {
                ctx.execute_activity_raw("wrong_name", Value::Null, "default")
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(Value::Null)
            })
        }

        let (_exec_id, events) = activity_events();
        let replayer = WorkflowReplayer::new().register_fn("wrong", wrong_activity);
        let report = replayer.replay_from_events(events).await;
        assert!(matches!(
            report.status,
            ReplayStatus::NonDeterminismDetected {
                kind: NonDeterminismKind::ActivityScheduleMismatch,
                ..
            }
        ));
    }

    #[test]
    fn parse_nd_message_activity() {
        let (kind, expected, actual) = parse_nd_message(
            "activity mismatch: expected ActivityScheduled(a), got ActivityScheduled(b)",
        );
        assert_eq!(kind, NonDeterminismKind::ActivityScheduleMismatch);
        assert_eq!(expected, "ActivityScheduled(a)");
        assert_eq!(actual, "ActivityScheduled(b)");
    }

    #[test]
    fn parse_nd_message_timer() {
        let (kind, _, _) =
            parse_nd_message("timer mismatch: expected TimerStarted(t1), got ActivityScheduled");
        assert_eq!(kind, NonDeterminismKind::TimerMismatch);
    }

    #[test]
    fn parse_nd_message_timer_cancel_mismatch() {
        // The activity matcher trips over an unconsumed TimerCancelled left by a
        // removed cancel_timer / handle.cancel / handle.reset call (issue #768).
        let (kind, _, actual) = parse_nd_message(
            "activity mismatch: expected ActivityScheduled(work), got TimerCancelled",
        );
        assert_eq!(kind, NonDeterminismKind::TimerCancelMismatch);
        assert_eq!(actual, "TimerCancelled");
    }

    #[test]
    fn classify_kind_timer_cancel_prefix_and_actual() {
        // Explicit "timer-cancel" prefix maps to TimerCancelMismatch.
        assert_eq!(
            classify_kind("timer-cancel", "TimerStarted(idle)"),
            NonDeterminismKind::TimerCancelMismatch
        );
        // A TimerCancelled `actual` always classifies as TimerCancelMismatch,
        // regardless of which command noticed it.
        assert_eq!(
            classify_kind("activity", "TimerCancelled"),
            NonDeterminismKind::TimerCancelMismatch
        );
    }

    #[test]
    fn parse_nd_message_unknown_format() {
        let (kind, expected, _) = parse_nd_message("signal history contains unexpected failure");
        assert_eq!(kind, NonDeterminismKind::Unknown);
        assert!(!expected.is_empty());
    }

    #[test]
    fn parse_nd_message_version_marker_mismatch() {
        // The activity matcher sees a stale version-gate marker and produces this message.
        let (kind, expected, actual) = parse_nd_message(
            "activity mismatch: expected ActivityScheduled(step), got MarkerRecorded(version:gate_old)",
        );
        assert_eq!(kind, NonDeterminismKind::VersionMarkerMismatch);
        assert_eq!(expected, "ActivityScheduled(step)");
        assert_eq!(actual, "MarkerRecorded(version:gate_old)");
    }

    #[test]
    fn parse_nd_message_patch_marker_mismatch() {
        // The activity matcher sees a stale patch marker (the patched() call
        // was removed before all marker-bearing executions drained — issue #687).
        let (kind, expected, actual) = parse_nd_message(
            "activity mismatch: expected ActivityScheduled(step), got MarkerRecorded(patch:gate_old)",
        );
        assert_eq!(kind, NonDeterminismKind::PatchMarkerMismatch);
        assert_eq!(expected, "ActivityScheduled(step)");
        assert_eq!(actual, "MarkerRecorded(patch:gate_old)");
    }

    #[test]
    fn classify_kind_covers_all_prefixes() {
        assert_eq!(
            classify_kind("activity", "ActivityScheduled(other)"),
            NonDeterminismKind::ActivityScheduleMismatch
        );
        assert_eq!(
            classify_kind("local activity", "LocalActivityScheduled(other)"),
            NonDeterminismKind::LocalActivityScheduleMismatch
        );
        assert_eq!(
            classify_kind("timer", "ActivityScheduled"),
            NonDeterminismKind::TimerMismatch
        );
        assert_eq!(
            classify_kind("signal", "ActivityScheduled"),
            NonDeterminismKind::SignalMismatch
        );
        assert_eq!(
            classify_kind("child workflow", "ActivityScheduled"),
            NonDeterminismKind::ChildWorkflowMismatch
        );
        assert_eq!(
            classify_kind("side effect", "ActivityScheduled"),
            NonDeterminismKind::SideEffectMismatch
        );
        assert_eq!(
            classify_kind("side-effect drift", "ActivityScheduled"),
            NonDeterminismKind::SideEffectDrift
        );
        assert_eq!(
            classify_kind("external activity", "ActivityScheduled"),
            NonDeterminismKind::ExternalActivityMismatch
        );
        assert_eq!(
            classify_kind("external signal", "ActivityScheduled"),
            NonDeterminismKind::ExternalSignalMismatch
        );
        assert_eq!(
            classify_kind("external await", "ExternalAwaitRequested(target=x)"),
            NonDeterminismKind::ExternalAwaitMismatch
        );
        assert_eq!(
            classify_kind("continue-as-new", ""),
            NonDeterminismKind::ContinueAsNewMismatch
        );
        assert_eq!(
            classify_kind("something else", ""),
            NonDeterminismKind::Unknown
        );
        // Version marker in actual always wins regardless of kind_str
        assert_eq!(
            classify_kind("activity", "MarkerRecorded(version:gate_old)"),
            NonDeterminismKind::VersionMarkerMismatch
        );
        // Patch marker in actual likewise wins regardless of kind_str (issue #687)
        assert_eq!(
            classify_kind("timer", "MarkerRecorded(patch:gate_old)"),
            NonDeterminismKind::PatchMarkerMismatch
        );
    }

    #[test]
    fn report_display_includes_status() {
        let report = ReplayReport {
            execution_id: ExecutionId::new(),
            events_replayed: 5,
            status: ReplayStatus::NonDeterminismDetected {
                kind: NonDeterminismKind::ActivityScheduleMismatch,
                expected: "ActivityScheduled(a)".into(),
                actual: "ActivityScheduled(b)".into(),
                event_index: 3,
            },
            mismatched_command_summary: Some("expected X, got Y".into()),
        };
        let s = format!("{report}");
        assert!(s.contains("NonDeterminism"));
        assert!(s.contains("ActivityScheduleMismatch"));
    }
}
