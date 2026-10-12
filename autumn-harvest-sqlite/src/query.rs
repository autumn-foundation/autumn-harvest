//! Read-only workflow queries (issue #2022).
//!
//! A query replays the stored history to the run's frontier, then calls a
//! handler that `ctx.register_query` registered. It reads rows and writes
//! nothing. The replay driver and the terminal-run classifier come from the
//! core, so a query here answers as a Postgres query does.

use std::collections::HashMap;
use std::time::Duration;

use autumn_harvest::context::{WorkflowContext, empty_shared_state};
use autumn_harvest::executor::{
    QueryReplayOutcome, TerminalQueryDecision, classify_terminal_query, drive_query_replay_async,
    history_reached_terminal_seal,
};
use autumn_harvest::{
    ExecutionId, HarvestError, WorkflowEvent, WorkflowHandlerFn, WorkflowHistoryPolicy,
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
        let decision = if self.terminal {
            classify_terminal_query(outcome, sealed, ctx.history_has_unconsumed_events())
        } else {
            // A running run serves the state at its first suspension.
            match outcome {
                QueryReplayOutcome::TimedOut => TerminalQueryDecision::TimedOut,
                QueryReplayOutcome::Panicked => TerminalQueryDecision::HistoryUnavailable,
                QueryReplayOutcome::ReachedTerminal | QueryReplayOutcome::Suspended => {
                    TerminalQueryDecision::Serve
                }
            }
        };

        match decision {
            TerminalQueryDecision::Serve => {}
            TerminalQueryDecision::TimedOut => {
                return Err(SqliteError::QueryTimedOut {
                    execution_id: exec,
                    name,
                    timeout_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
                });
            }
            TerminalQueryDecision::HistoryUnavailable
                if matches!(outcome, QueryReplayOutcome::Panicked) =>
            {
                return Err(SqliteError::QueryPanicked {
                    execution_id: exec,
                    name,
                    details: "the workflow handler panicked during the query replay".to_string(),
                });
            }
            TerminalQueryDecision::HistoryUnavailable => {
                return Err(SqliteError::QueryUnavailable {
                    execution_id: exec,
                    name,
                    details: "the replay diverged from the recorded history".to_string(),
                });
            }
        }

        // The core does not catch a panic in an imperative handler. Catch it
        // here, so the panic does not reach the caller.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            ctx.execute_query_with_args(&name, args)
        }));
        match result {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(HarvestError::QueryHandlerNotFound(_))) => Err(SqliteError::QueryNotFound {
                execution_id: exec,
                name,
            }),
            Ok(Err(HarvestError::QueryHandlerPanicked(details))) => {
                Err(SqliteError::QueryPanicked {
                    execution_id: exec,
                    name,
                    details,
                })
            }
            Ok(Err(HarvestError::QueryHandlerFailed(details))) => Err(SqliteError::QueryFailed {
                execution_id: exec,
                name,
                details,
            }),
            Ok(Err(other)) => Err(SqliteError::QueryFailed {
                execution_id: exec,
                name,
                details: other.to_string(),
            }),
            Err(payload) => Err(SqliteError::QueryPanicked {
                execution_id: exec,
                name,
                details: autumn_harvest::error::panic_message(payload),
            }),
        }
    }
}
