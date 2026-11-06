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

use autumn_harvest::context::{SharedStateMap, WorkflowLogPolicy};
use autumn_harvest_sqlite::{SqliteResult, SqliteRuntime};

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
