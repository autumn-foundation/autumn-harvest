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
use std::collections::HashMap;
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
            Self::ContinueAsNewMismatch => write!(f, "ContinueAsNewMismatch"),
            Self::EarlyCompletion => write!(f, "EarlyCompletion"),
            Self::VersionMarkerMismatch => write!(f, "VersionMarkerMismatch"),
            Self::PatchMarkerMismatch => write!(f, "PatchMarkerMismatch"),
            Self::TimerCancelMismatch => write!(f, "TimerCancelMismatch"),
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
        }
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

    /// Replace the shared state with a pre-built `SharedState` arc.
    ///
    /// Used internally by [`TestRunOutcome::replay_check`] to forward the test
    /// environment's state to the replayer so the workflow sees the same typed
    /// state it saw during the original run.
    #[must_use]
    fn with_existing_state(mut self, state: SharedState) -> Self {
        self.state = state;
        self
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
    pub async fn replay_from_snapshot(&self, snapshot: HistorySnapshot) -> ReplayReport {
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

        let exec_id = snapshot.execution_id;
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
            )
            .await
        };
        outcome_to_report(exec_id, total_events, outcome, false)
    }

    /// Replay a snapshot in canary mode (used for deploy-time verify).
    pub async fn replay_canary_snapshot(&self, snapshot: HistorySnapshot) -> ReplayReport {
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

        let exec_id = snapshot.execution_id;
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
            let exec_id = ExecutionId::new();
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

        let (_, &handler) = self.handlers.iter().next().unwrap();
        let exec_id = ExecutionId::new();
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

        // Load workflow name and context headers from executions table.
        let (workflow_name, context_headers) =
            load_workflow_name_and_headers(conn, exec_id).await?;

        let snapshot = HistorySnapshot {
            workflow_name,
            execution_id: exec_id,
            events: history.events,
            context_headers: Some(context_headers),
        };
        Ok(self.replay_from_snapshot(snapshot).await)
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
            for (id, name, created, headers) in executions {
                all_executions.push(SampledExecution {
                    shard_id,
                    execution_id: crate::types::ExecutionId::from_uuid(id),
                    workflow_name: name,
                    created_at: created,
                    context_headers: headers,
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
                                }
                            }; // `conn` is dropped here

                            let report = replayer_ref.replay_canary_snapshot(snapshot).await;
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
    )>,
> {
    use crate::schema::harvest_workflow_executions::dsl::{
        context_headers, created_at, harvest_workflow_executions, id, queue_name, state,
        workflow_name,
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
        .select((id, workflow_name, created_at, context_headers))
        .order((created_at.desc(), id.desc()))
        .limit(i64::try_from(options.sample_size).unwrap_or(i64::MAX))
        .load::<(
            uuid::Uuid,
            String,
            chrono::DateTime<chrono::Utc>,
            Option<serde_json::Value>,
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

#[cfg(feature = "db")]
async fn load_workflow_name_and_headers(
    conn: &mut diesel_async::AsyncPgConnection,
    exec_id: ExecutionId,
) -> crate::error::HarvestResult<(String, HashMap<String, String>)> {
    use crate::error::{HarvestError, database_error};
    use crate::schema::harvest_workflow_executions::dsl::{
        context_headers as context_headers_col, harvest_workflow_executions, id as id_col,
        workflow_name,
    };
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    let exec_uuid = exec_id.as_uuid();

    let (name, raw_headers): (String, Option<serde_json::Value>) = harvest_workflow_executions
        .filter(id_col.eq(exec_uuid))
        .select((workflow_name, context_headers_col))
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

    Ok((name, headers))
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
        }
    }

    /// Register a batch of workflow handlers from a `workflows![…]` collector call.
    #[must_use]
    pub fn register(mut self, workflows: Vec<crate::info::WorkflowInfo>) -> Self {
        for wf in workflows {
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
        let handlers = Arc::new(self.handlers.clone());
        let state = self.state.clone();

        let mut tasks = Vec::with_capacity(files.len());
        for path in files {
            let sem = Arc::clone(&semaphore);
            let handlers = Arc::clone(&handlers);
            let state = state.clone();

            tasks.push(tokio::spawn(async move {
                let _permit = sem.acquire_owned().await.unwrap();
                replay_fixture_file(&handlers, state, &path, timeout, allow_unregistered).await
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
                    } else if path.extension().and_then(|s| s.to_str()) == Some("json") {
                        files.push(path);
                    }
                }
            }
        }
    }

    files.sort();
    Ok(files)
}

/// Replay a single fixture file and return a [`FixtureResult`].
async fn replay_fixture_file(
    handlers: &HashMap<String, WorkflowHandlerFn>,
    state: SharedState,
    path: &std::path::Path,
    timeout: std::time::Duration,
    allow_unregistered: bool,
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

    // Build a single-use replayer and run with timeout.
    let replayer = WorkflowReplayer {
        handlers: handlers.clone(),
        state,
        context_headers: HashMap::new(),
        payload_offloader: None,
        use_advancing_clock: false,
        metrics: std::sync::Arc::new(crate::telemetry::NoOpMetrics),
    };

    let replay_result =
        tokio::time::timeout(timeout, replayer.replay_from_snapshot(snapshot)).await;

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

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

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
    pub async fn replay_check(&self, handler: WorkflowHandlerFn) -> ReplayReport {
        let snapshot = crate::testing::HistorySnapshot {
            workflow_name: "__test__".to_string(),
            execution_id: self.exec_id,
            events: self.events.clone(),
            context_headers: None,
        };
        WorkflowReplayer::new()
            .with_existing_state(self.state.clone())
            .register_fn("__test__", handler)
            .with_advancing_timer_clock()
            .replay_from_snapshot(snapshot)
            .await
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
    /// Task queue name threaded into the `WorkflowContext` so in-context
    /// engine metrics carry a real `queue` label. Defaults to `""`.
    queue_name: String,
}

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
            cancellation_reason: None,
            state: empty_shared_state(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
            metrics: std::sync::Arc::new(crate::telemetry::NoOpMetrics),
            workflow_name: String::new(),
            queue_name: String::new(),
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

    /// Set the task queue name for the contexts this env builds, so
    /// in-context engine metrics carry a real `queue` label instead of the
    /// legacy `""` default. Pairs with [`with_metrics`](Self::with_metrics).
    #[must_use]
    pub fn with_queue_name(mut self, queue: impl Into<String>) -> Self {
        self.queue_name = queue.into();
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

        let start_time = self.simulated_now;

        // Thread the configured workflow/queue names into the context (via
        // the executor's span-meta plumbing, the same path the worker uses)
        // so engine metrics emitted from inside the workflow carry real
        // labels a test can assert on.
        let span_meta = if self.workflow_name.is_empty() && self.queue_name.is_empty() {
            None
        } else {
            Some(WorkflowExecuteSpanMeta {
                workflow_name: self.workflow_name.clone(),
                workflow_id: String::new(),
                shard_id: 0,
                queue_name: self.queue_name.clone(),
                is_replay: false,
                link_traceparent: None,
                build_id: None,
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
            )
            .await;

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
        }
    }

    fn finish_terminal_outcome(
        &self,
        outcome: WorkflowOutcome,
        pending_cmds: &[WorkflowCommand],
        mut history: Vec<WorkflowEvent>,
        exec_id: ExecutionId,
        start_time: DateTime<Utc>,
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
            WorkflowOutcome::ContinuedAsNew { input } => {
                history.push(WorkflowEvent::WorkflowContinuedAsNew {
                    new_exec_id: ExecutionId::new(),
                    input: input.clone(),
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

        let mut made_progress = false;
        let mut deferred_events = Vec::new();
        for cmd in commands {
            made_progress |= self.process_command(
                cmd,
                batch_has_competing_suspension,
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
                ..
            } => {
                let call_num = Self::next_call_count(call_counts, &name);
                let result = self.resolve_activity(&name, act_input.clone(), call_num)?;
                history.push(WorkflowEvent::LocalActivityScheduled {
                    activity_id,
                    name: name.clone(),
                    input: act_input,
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
                let result = self.resolve_child(&workflow_name, child_input.clone())?;
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
            | WorkflowCommand::ScheduleExternalActivity { .. }
            | WorkflowCommand::Complete { .. }
            | WorkflowCommand::Fail { .. }
            | WorkflowCommand::ContinueAsNew { .. } => Ok(false),
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
