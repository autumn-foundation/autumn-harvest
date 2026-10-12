//! Read-only workflow queries (issue #2022).
//!
//! A query replays the stored history to the run's frontier, then calls a
//! handler that `ctx.register_query` registered. It reads rows and writes
//! nothing. The replay driver and the terminal-run classifier come from the
//! core. A running run answers as the in-process Postgres query does. A
//! terminal run answers as the Postgres HTTP query does.

use std::collections::HashMap;
use std::time::Duration;

use autumn_harvest::context::{WorkflowContext, empty_shared_state};
use autumn_harvest::executor::{
    QueryReplayOutcome, TerminalQueryDecision, classify_terminal_query, drive_query_replay_async,
    history_reached_terminal_seal,
};
use autumn_harvest::{
    ExecutionId, HarvestError, WorkflowCommand, WorkflowEvent, WorkflowHandlerFn,
    WorkflowHistoryPolicy,
};
use rusqlite::Connection;
use serde_json::Value;

use crate::error::{SqliteError, SqliteResult};
use crate::store;

/// The default query timeout. It matches the Postgres builder default.
pub const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(5);

/// The rows one query replays. It owns them, so the query future borrows no
/// connection.
pub struct QueryReplay {
    exec: ExecutionId,
    handler: WorkflowHandlerFn,
    history: Vec<WorkflowEvent>,
    input: Value,
    workflow_name: String,
    workflow_id: String,
    terminal: bool,
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
        exec: ExecutionId,
    ) -> SqliteResult<Self> {
        // The state read goes first. It is the read that rejects an unknown id.
        let state = store::execution_state(conn, exec)?;
        let workflow_name = store::workflow_name_of(conn, exec)?;
        let handler = *workflows
            .get(&workflow_name)
            .ok_or_else(|| SqliteError::UnknownWorkflow(workflow_name.clone()))?;
        Ok(Self {
            exec,
            handler,
            history: store::load_history(conn, exec)?,
            input: store::execution_input(conn, exec)?,
            workflow_id: store::workflow_id_of(conn, exec)?,
            workflow_name,
            terminal: autumn_harvest::erase::is_terminal_state(&state),
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
        let exec = self.exec;
        let sealed = self.terminal && history_reached_terminal_seal(&self.history);
        // The drive builds its context with the same name and id, so a
        // handler that reads `ctx.info()` replays the same path.
        let ctx = WorkflowContext::for_replay_with_state_and_history_policy(
            exec,
            self.history,
            empty_shared_state(),
            WorkflowHistoryPolicy::default(),
        )
        .with_workflow_name(self.workflow_name)
        .with_workflow_id(self.workflow_id);

        let outcome = drive_query_replay_async(&ctx, self.handler, self.input, timeout).await;
        let refusal = if self.terminal {
            terminal_refusal(&ctx, outcome, sealed)
        } else {
            running_refusal(outcome)
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
        }
    }
}

/// A running run serves the state at its frontier. This matches the
/// in-process Postgres query.
const fn running_refusal(outcome: QueryReplayOutcome) -> Option<Refusal> {
    match outcome {
        QueryReplayOutcome::TimedOut => Some(Refusal::TimedOut),
        QueryReplayOutcome::Panicked => Some(Refusal::Panicked),
        QueryReplayOutcome::ReachedTerminal | QueryReplayOutcome::Suspended => None,
    }
}

/// A terminal run serves the state at its recorded end. The core classifier
/// decides, as on the Postgres HTTP path (issue #612).
///
/// One more check is specific to this backend. A run sealed by an
/// unsupported feature rolls back its last cycle, side effects included.
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
        // Only a declarative handler gives this. This backend seeds none, so
        // the arm is a guard for a later core change.
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
