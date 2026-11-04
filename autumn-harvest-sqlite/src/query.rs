//! Read-only workflow queries (issue #2022).
//!
//! A query replays the stored history to the run's frontier, then calls a
//! handler that `ctx.register_query` registered. It reads rows and writes
//! nothing. The replay driver and the terminal-run classifier come from the
//! core. A running run answers as the in-process Postgres query does. A
//! terminal run answers as the Postgres HTTP query does.

use std::collections::HashMap;
use std::time::Duration;

use autumn_harvest::context::{SharedState, WorkflowContext};
use autumn_harvest::error::NonDeterministicDetails;
use autumn_harvest::executor::{
    QueryReplayOutcome, TerminalQueryDecision, classify_terminal_query, drive_query_replay_async,
    history_reached_terminal_seal,
};
use autumn_harvest::{
    ExecutionId, HarvestError, QueryHandlerInfo, WorkflowCommand, WorkflowEvent, WorkflowHandlerFn,
    WorkflowHistoryPolicy,
};
use rusqlite::Connection;
use serde_json::Value;

use crate::error::{SqliteError, SqliteResult};
use crate::store;

/// The default query timeout. It matches the Postgres builder default.
pub const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(5);

/// The rows that a replay of one run reads (issue #2468).
///
/// It owns them, so a replay future borrows no connection. A query replay
/// and the awaitables report both build their context from it. The context
/// then matches the drive's context.
pub struct ReplayInput {
    exec: ExecutionId,
    history: Vec<WorkflowEvent>,
    input: Value,
    workflow_name: String,
    workflow_id: String,
    terminal: bool,
    pending_cancel: Option<String>,
    parent: Option<ExecutionId>,
    /// The drive's timeout and deadline, so `ctx.deadline()` matches (issue
    /// #2029).
    deadline: (
        Option<chrono::Duration>,
        Option<chrono::DateTime<chrono::Utc>>,
    ),
}

impl ReplayInput {
    /// Read the rows of `exec`.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::ExecutionNotFound`] for an unknown id. Returns
    /// a persistence error if a row cannot be read.
    pub fn load(conn: &Connection, exec: ExecutionId) -> SqliteResult<Self> {
        // The state read goes first. It is the read that rejects an unknown id.
        let state = store::execution_state(conn, exec)?;
        let terminal = autumn_harvest::erase::is_terminal_state(&state);
        // A cancel request is pending only while the run is live (issue
        // #2021). The next drive sees it, so the replay sees it too.
        let pending_cancel = if terminal {
            None
        } else {
            store::cancel_request(conn, exec)?.map(|(reason, _)| reason)
        };
        Ok(Self {
            exec,
            history: store::load_history(conn, exec)?,
            input: store::execution_input(conn, exec)?,
            workflow_name: store::workflow_name_of(conn, exec)?,
            workflow_id: store::workflow_id_of(conn, exec)?,
            terminal,
            pending_cancel,
            // A child sees its parent here, as on a drive (issue #2026).
            parent: store::parent_link(conn, exec)?.map(|(parent, _)| parent),
            deadline: store::run_config(conn, exec)?.context_deadline(),
        })
    }

    /// The workflow name of the run.
    #[must_use]
    pub fn workflow_name(&self) -> &str {
        &self.workflow_name
    }

    /// Whether the run is in a terminal state.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        self.terminal
    }

    /// The stored history of the run.
    #[must_use]
    pub fn history(&self) -> &[WorkflowEvent] {
        &self.history
    }

    /// The stored history of the run, owned.
    #[must_use]
    pub fn into_history(self) -> Vec<WorkflowEvent> {
        self.history
    }

    /// The parent run, for a child run.
    #[must_use]
    pub const fn parent(&self) -> Option<ExecutionId> {
        self.parent
    }

    /// The declared execution timeout and the deadline of the run.
    #[must_use]
    pub const fn deadline(
        &self,
    ) -> (
        Option<chrono::Duration>,
        Option<chrono::DateTime<chrono::Utc>>,
    ) {
        self.deadline
    }

    /// A replay context and the run input.
    ///
    /// The drive builds its context with the same name, id, parent, history
    /// policy, pending cancel and deadline. A handler that reads them then
    /// replays the same path.
    #[must_use]
    pub fn into_context(
        self,
        shared_state: SharedState,
        history_policy: WorkflowHistoryPolicy,
    ) -> (WorkflowContext, Value) {
        let ctx = WorkflowContext::for_replay_with_state_and_history_policy(
            self.exec,
            self.history,
            shared_state,
            history_policy,
        )
        .with_workflow_name(self.workflow_name)
        .with_workflow_id(self.workflow_id)
        .with_parent_execution_id(self.parent)
        .with_pending_cancellation(self.pending_cancel)
        .with_execution_timeout(self.deadline.0)
        .with_deadline(self.deadline.1);
        (ctx, self.input)
    }
}

/// The rows one query replays, with the handler of the run's workflow.
pub struct QueryReplay {
    rows: ReplayInput,
    handler: WorkflowHandlerFn,
    history_policy: WorkflowHistoryPolicy,
    /// The runtime's app state, so `ctx.state::<T>()` matches the drive
    /// (issue #2467).
    shared_state: SharedState,
    /// The declarative `#[query]` handlers of the run's workflow (issue
    /// #2468).
    queries: Vec<QueryHandlerInfo>,
}

impl QueryReplay {
    /// Read the rows a query of `exec` needs.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::ExecutionNotFound`] for an unknown id, and
    /// [`SqliteError::UnknownWorkflow`] for an unregistered workflow. Returns
    /// a persistence error if a row cannot be read.
    pub fn load(
        conn: &Connection,
        workflows: &HashMap<String, WorkflowHandlerFn>,
        queries: &HashMap<String, Vec<QueryHandlerInfo>>,
        history_policy: WorkflowHistoryPolicy,
        shared_state: SharedState,
        exec: ExecutionId,
    ) -> SqliteResult<Self> {
        let rows = ReplayInput::load(conn, exec)?;
        let handler = *workflows
            .get(&rows.workflow_name)
            .ok_or_else(|| SqliteError::UnknownWorkflow(rows.workflow_name.clone()))?;
        let queries = queries
            .get(&rows.workflow_name)
            .cloned()
            .unwrap_or_default();
        Ok(Self {
            rows,
            handler,
            history_policy,
            shared_state,
            queries,
        })
    }

    /// Replay the run to its frontier, then call the query `name`.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::QueryNotFound`], [`SqliteError::QueryFailed`],
    /// [`SqliteError::QueryPanicked`], [`SqliteError::QueryTimedOut`] or
    /// [`SqliteError::QueryUnavailable`].
    pub async fn run(self, name: String, args: Value, timeout: Duration) -> SqliteResult<Value> {
        let exec = self.rows.exec;
        let terminal = self.rows.terminal;
        let sealed = terminal && history_reached_terminal_seal(&self.rows.history);
        let (ctx, input) = self
            .rows
            .into_context(self.shared_state, self.history_policy);
        // A declarative handler answers when no imperative one has the name,
        // as on Postgres (issue #2468).
        for query in &self.queries {
            ctx.register_declarative_query_handler(query);
        }

        let outcome = drive_query_replay_async(&ctx, self.handler, input, timeout).await;
        let refusal = if terminal {
            terminal_refusal(&ctx, outcome, sealed)
        } else {
            running_refusal(&ctx, outcome)
        };
        if let Some(refusal) = refusal {
            return Err(refusal.into_error(exec, name, timeout));
        }

        // The core does not catch a panic in an imperative handler. Catch it
        // here, so the panic does not reach the caller.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            ctx.execute_query_with_args(&name, args)
        }));
        handler_result(exec, name, result)
    }
}

/// The `details` of a replay panic. The core logs the panic message.
const REPLAY_PANIC: &str = "the workflow handler panicked during the query replay";

/// Why a replay cannot serve a query.
enum Refusal {
    TimedOut,
    Panicked,
    Unavailable(&'static str),
    Diverged(String),
}

impl Refusal {
    fn into_error(self, execution_id: ExecutionId, name: String, timeout: Duration) -> SqliteError {
        match self {
            Self::TimedOut => SqliteError::QueryTimedOut {
                execution_id,
                name,
                timeout_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
            },
            Self::Panicked => SqliteError::QueryPanicked {
                execution_id,
                name,
                details: REPLAY_PANIC.to_string(),
            },
            Self::Unavailable(details) => SqliteError::QueryUnavailable {
                execution_id,
                name,
                details: details.to_string(),
            },
            Self::Diverged(details) => SqliteError::NonDeterministic {
                execution_id,
                details,
            },
        }
    }
}

/// A running run serves the state at its frontier. This matches the
/// in-process Postgres query.
///
/// A drive of drifted code returns `NonDeterministic` and keeps the history.
/// A query on the same run returns the same error. It does not serve the
/// state that the new code rebuilds.
fn running_refusal(ctx: &WorkflowContext, outcome: QueryReplayOutcome) -> Option<Refusal> {
    match outcome {
        QueryReplayOutcome::TimedOut => Some(Refusal::TimedOut),
        QueryReplayOutcome::Panicked => Some(Refusal::Panicked),
        QueryReplayOutcome::ReachedTerminal | QueryReplayOutcome::Suspended => {
            if let Some(nd) = ctx.take_nd_details() {
                return Some(Refusal::Diverged(divergence(&nd)));
            }
            // The worker runs the same check at each suspension (issue
            // #1791). It counts command events only, so a pending signal or
            // result is not drift.
            ctx.first_unconsumed_command_event()
                .map(|(index, recorded)| {
                    Refusal::Diverged(format!(
                        "the query replay skipped the recorded {recorded} at event {index}"
                    ))
                })
        }
    }
}

/// Describe a divergence that the replay recorded.
fn divergence(nd: &NonDeterministicDetails) -> String {
    let at = nd
        .event_index
        .map_or_else(|| "an unknown event".to_string(), |i| format!("event {i}"));
    format!(
        "the query replay diverged from the recorded history at {at}: expected {}, got {}",
        nd.expected.as_deref().unwrap_or("?"),
        nd.actual.as_deref().unwrap_or("?"),
    )
}

/// A terminal run serves the state at its recorded end. The core classifier
/// decides, as on the Postgres HTTP path (issue #612).
///
/// One more check is specific to this backend. A seal for an unsupported
/// feature or a history cap rolls back the last cycle, side effects included.
/// The replay then mints new side-effect values past the recorded end. Each
/// query would see different values, so the query refuses.
fn terminal_refusal(
    ctx: &WorkflowContext,
    outcome: QueryReplayOutcome,
    sealed: bool,
) -> Option<Refusal> {
    match classify_terminal_query(outcome, sealed, ctx.history_has_unconsumed_events()) {
        TerminalQueryDecision::TimedOut => Some(Refusal::TimedOut),
        TerminalQueryDecision::HistoryUnavailable => Some(Refusal::Unavailable(match outcome {
            QueryReplayOutcome::Panicked => REPLAY_PANIC,
            _ => "the replay diverged from the recorded history",
        })),
        TerminalQueryDecision::Serve if ctx.take_nd_details().is_some() => Some(
            Refusal::Unavailable("the replay diverged from the recorded history"),
        ),
        TerminalQueryDecision::Serve => {
            let minted = ctx.drain_commands().iter().any(|c| {
                matches!(
                    c,
                    WorkflowCommand::RecordSideEffect { .. } | WorkflowCommand::RecordMarker { .. }
                )
            });
            minted.then_some(Refusal::Unavailable(
                "the replay ran past the recorded end and minted unrecorded side-effect values",
            ))
        }
    }
}

/// Map the handler call to the typed query errors.
fn handler_result(
    execution_id: ExecutionId,
    name: String,
    result: std::thread::Result<Result<Value, HarvestError>>,
) -> SqliteResult<Value> {
    match result {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(HarvestError::QueryHandlerNotFound(_))) => {
            Err(SqliteError::QueryNotFound { execution_id, name })
        }
        // Only a declarative handler gives this (issue #2468).
        Ok(Err(HarvestError::QueryHandlerPanicked(details))) => Err(SqliteError::QueryPanicked {
            execution_id,
            name,
            details,
        }),
        Err(payload) => Err(SqliteError::QueryPanicked {
            execution_id,
            name,
            details: autumn_harvest::error::panic_message(payload),
        }),
        Ok(Err(HarvestError::QueryHandlerFailed(details))) => Err(SqliteError::QueryFailed {
            execution_id,
            name,
            details,
        }),
        Ok(Err(other)) => Err(SqliteError::QueryFailed {
            execution_id,
            name,
            details: other.to_string(),
        }),
    }
}
