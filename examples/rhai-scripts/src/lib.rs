//! Durable Rhai scripts on the embedded `SQLite` runtime.
//!
//! A script is a plain `.rhai` file. Each host function (`sh`, `read_file`,
//! `write_file`, `env`) is a Harvest activity, so its result is saved. A
//! `sleep` is a durable timer, and `wait_for` is a signal. A run that dies
//! resumes from its last saved call, and the plain Rhai code between two calls
//! replays.
//!
//! The script engine has no clock, no environment and no file access of its
//! own. So the code between two host calls is deterministic by construction,
//! not by convention.

pub mod bridge;
pub mod cli;
pub mod host;
pub mod workflow;

use std::any::TypeId;
use std::path::Path;
use std::sync::Arc;

use autumn_harvest::ExecutionId;
use autumn_harvest::context::{
    SharedStateMap, WorkflowHistoryPolicy, WorkflowLogPolicy, empty_shared_state,
};
use autumn_harvest::executor::{QueryReplayOutcome, drive_query_replay_async};
use autumn_harvest_sqlite::{DEFAULT_QUERY_TIMEOUT, ReplayInput, SqliteResult, SqliteRuntime};
use rusqlite::{Connection, OpenFlags};
use serde_json::Value;

pub use workflow::{Echo, ScriptInput, rhai_script, rhai_script_info};

/// The registered name of the script workflow.
pub const WORKFLOW: &str = "rhai_script";

/// Open the run database at `path`, with the script workflow and the host
/// activities registered.
///
/// # Errors
///
/// Returns the runtime error if the file cannot be opened or is locked by
/// another process.
pub fn open(path: impl AsRef<Path>) -> SqliteResult<SqliteRuntime> {
    let mut rt = SqliteRuntime::open(path)?;
    register(&mut rt);
    Ok(rt)
}

/// Register the script workflow and the host activities on `rt`.
///
/// It also stores each `print` line in the run's durable log, which
/// `history` prints.
pub fn register(rt: &mut SqliteRuntime) {
    rt.register_workflow(&rhai_script_info());
    host::register(rt);
    rt.set_workflow_log_policy(Some(WorkflowLogPolicy::new()));
}

/// Send each live `print` line of the runs that `rt` drives to `echo`.
///
/// A replayed line does not reach the sink. The durable log keeps it once.
pub fn set_echo(rt: &mut SqliteRuntime, echo: Echo) {
    let mut state = SharedStateMap::new();
    state.insert(TypeId::of::<Echo>(), Box::new(echo));
    rt.set_shared_state(Arc::new(state));
}

/// Answer the `status` query of `exec` without the writer lock.
///
/// A live `run` holds the writer lock of the database, so a second runtime
/// cannot open it. This read replays the run over a read-only connection
/// instead, so it works beside a live `run`. The replay has no echo sink, so
/// it prints nothing. It writes no row.
///
/// # Errors
///
/// Returns an error if the file cannot be read, the run does not exist, or
/// the replay panics or passes the query timeout.
pub async fn status(path: impl AsRef<Path>, exec: ExecutionId) -> Result<Value, String> {
    let rows = {
        let conn = Connection::open_with_flags(path.as_ref(), OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|e| e.to_string())?;
        // One read transaction, so every row comes from the same commit.
        let tx = conn.unchecked_transaction().map_err(|e| e.to_string())?;
        let rows = ReplayInput::load(&tx, exec).map_err(|e| e.to_string())?;
        tx.commit().map_err(|e| e.to_string())?;
        rows
    };
    let (ctx, input) = rows.into_context(empty_shared_state(), WorkflowHistoryPolicy::default());
    let handler = rhai_script_info().handler;
    match drive_query_replay_async(&ctx, handler, input, DEFAULT_QUERY_TIMEOUT).await {
        QueryReplayOutcome::ReachedTerminal | QueryReplayOutcome::Suspended => {
            ctx.execute_query("status").map_err(|e| e.to_string())
        }
        QueryReplayOutcome::TimedOut => Err("the status replay timed out".to_string()),
        QueryReplayOutcome::Panicked => Err("the status replay panicked".to_string()),
    }
}
