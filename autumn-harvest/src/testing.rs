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

use serde_json::Value;

use crate::context::{SharedState, empty_shared_state};
use crate::event::WorkflowEvent;
use crate::executor::{WorkflowOutcome, run_workflow_strict};
use crate::info::{WorkflowHandlerFn, WorkflowInfo};
use crate::types::ExecutionId;

// ---------------------------------------------------------------------------
// NonDeterminismKind
// ---------------------------------------------------------------------------

/// The category of non-determinism detected during replay.
///
/// Each variant maps to a distinct command/event kind so callers can
/// distinguish (and report on) activity vs timer vs signal divergences.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    /// An external activity name did not match the recorded event.
    ExternalActivityMismatch,
    /// A continue-as-new input differed from history.
    ContinueAsNewMismatch,
    /// The workflow returned before consuming all recorded history events.
    EarlyCompletion,
    /// A version gate's `change_id` was renamed without migrating the history
    /// so the old `version:…` marker was left unconsumed and was encountered
    /// by the next command at that cursor position.
    VersionMarkerMismatch,
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
            Self::ExternalActivityMismatch => write!(f, "ExternalActivityMismatch"),
            Self::ContinueAsNewMismatch => write!(f, "ContinueAsNewMismatch"),
            Self::EarlyCompletion => write!(f, "EarlyCompletion"),
            Self::VersionMarkerMismatch => write!(f, "VersionMarkerMismatch"),
            Self::Unknown => write!(f, "Unknown"),
        }
    }
}

// ---------------------------------------------------------------------------
// ReplayStatus
// ---------------------------------------------------------------------------

/// The result classification of a single replay run.
#[derive(Debug, Clone)]
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
///         WorkflowEvent::WorkflowStarted { input: Value::Null, timestamp: Utc::now() },
///     ],
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
}

impl Default for WorkflowReplayer {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkflowReplayer {
    /// Create an empty replayer with no registered handlers.
    #[must_use]
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
            state: empty_shared_state(),
        }
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
        let total_events = snapshot.events.len();
        let input = extract_input(&snapshot.events);

        let outcome = run_workflow_strict(
            exec_id,
            snapshot.events.clone(),
            handler,
            input,
            self.state.clone(),
        )
        .await;
        outcome_to_report(exec_id, total_events, &snapshot.events, outcome)
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
        let total_events = events.len();
        let input = extract_input(&events);

        let outcome =
            run_workflow_strict(exec_id, events.clone(), handler, input, self.state.clone()).await;
        outcome_to_report(exec_id, total_events, &events, outcome)
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

        // Load workflow name from executions table.
        let workflow_name = load_workflow_name(conn, exec_id).await?;

        let snapshot = HistorySnapshot {
            workflow_name,
            execution_id: exec_id,
            events: history.events,
        };
        Ok(self.replay_from_snapshot(snapshot).await)
    }
}

// ---------------------------------------------------------------------------
// DB helper (db feature only)
// ---------------------------------------------------------------------------

#[cfg(feature = "db")]
async fn load_workflow_name(
    conn: &mut diesel_async::AsyncPgConnection,
    exec_id: ExecutionId,
) -> crate::error::HarvestResult<String> {
    use crate::error::{HarvestError, database_error};
    use crate::schema::harvest_workflow_executions::dsl::{
        harvest_workflow_executions, id as id_col, workflow_name,
    };
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    let exec_uuid = exec_id.as_uuid();

    let name: String = harvest_workflow_executions
        .filter(id_col.eq(exec_uuid))
        .select(workflow_name)
        .first(conn)
        .await
        .map_err(|e| match e {
            diesel::result::Error::NotFound => HarvestError::NotFound(exec_id.to_string()),
            other => database_error(other),
        })?;

    Ok(name)
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
    events: &[WorkflowEvent],
    outcome: WorkflowOutcome,
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
        WorkflowOutcome::Suspended { .. } => ReplayReport {
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
        },

        WorkflowOutcome::Failed { error } => try_parse_non_determinism(&error, exec_id, events)
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
    events: &[WorkflowEvent],
) -> Option<ReplayReport> {
    // HarvestError::NonDeterministic formats as "non-deterministic replay: {msg}"
    let msg = error.strip_prefix("non-deterministic replay: ")?;

    let (kind, expected, actual) = parse_nd_message(msg);
    let event_index = find_event_index(events, &actual);
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
/// regardless of which command kind triggered the mismatch.
fn classify_kind(kind_str: &str, actual: &str) -> NonDeterminismKind {
    // A version marker found where another event was expected means the version
    // gate's change_id was renamed — classify specifically so error messages
    // point at the version gate rather than the command that first noticed it.
    if actual.starts_with("MarkerRecorded(version:") {
        return NonDeterminismKind::VersionMarkerMismatch;
    }
    match kind_str {
        "activity" => NonDeterminismKind::ActivityScheduleMismatch,
        "local activity" => NonDeterminismKind::LocalActivityScheduleMismatch,
        "timer" => NonDeterminismKind::TimerMismatch,
        "signal" => NonDeterminismKind::SignalMismatch,
        "child workflow" => NonDeterminismKind::ChildWorkflowMismatch,
        "side effect" => NonDeterminismKind::SideEffectMismatch,
        "external activity" => NonDeterminismKind::ExternalActivityMismatch,
        s if s.contains("continue") => NonDeterminismKind::ContinueAsNewMismatch,
        "early completion" => NonDeterminismKind::EarlyCompletion,
        _ => NonDeterminismKind::Unknown,
    }
}

/// Find the index of the first event in `events` whose type matches the
/// event type embedded in `actual` (e.g. `"ActivityScheduled(step_two)"` →
/// look for `ActivityScheduled`).  Falls back to 0 if not found.
fn find_event_index(events: &[WorkflowEvent], actual: &str) -> usize {
    let target = actual.split('(').next().unwrap_or(actual);
    events
        .iter()
        .position(|e| e.type_name() == target)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ActivityExecId;
    use chrono::Utc;
    use std::future::Future;
    use std::pin::Pin;

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
            classify_kind("external activity", "ActivityScheduled"),
            NonDeterminismKind::ExternalActivityMismatch
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
