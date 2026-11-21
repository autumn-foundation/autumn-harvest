//! The one workflow type. Every script run is a run of `rhai_script`.

use std::sync::{Arc, Mutex, PoisonError};

use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::bridge::{self, Op, Request, ScriptJob};

/// The queue that the host activities use. One writer serves it.
const QUEUE: &str = "default";

/// The input of one script run.
///
/// The run stores the source. A resume replays the stored source, so an edit
/// to the file on disk never changes a run that already started.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScriptInput {
    /// The path the script was read from. It is a label only.
    pub path: String,
    /// The script source.
    pub source: String,
    /// The script arguments, read as the `ARGS` array.
    #[serde(default)]
    pub args: Vec<String>,
    /// The directory that relative paths and shell commands use.
    pub cwd: String,
    /// The operation bound of one replay. See [`bridge::MAX_OPERATIONS`].
    #[serde(default = "default_max_operations")]
    pub max_operations: u64,
}

/// The serde default of [`ScriptInput::max_operations`].
const fn default_max_operations() -> u64 {
    bridge::MAX_OPERATIONS
}

/// A sink for the `print` lines of a live run.
///
/// The CLI installs one as shared state. A query replay of the same run then
/// prints nothing, because `status` opens its runtime with no sink.
#[derive(Clone)]
pub struct Echo(pub Arc<dyn Fn(&str) + Send + Sync>);

/// What the `status` query reports.
#[derive(Debug, Default)]
struct Status {
    progress: String,
    calls: u64,
}

/// Run one Rhai script durably.
///
/// The script runs on its own thread, and this body serves its requests in
/// order. See [`bridge`] for how a suspension stops the thread.
///
/// # Errors
///
/// Returns the script error, a failed host call that the script did not
/// catch, or a failure to start the script thread.
#[workflow]
pub async fn rhai_script(ctx: &WorkflowContext, input: ScriptInput) -> Result<Value, String> {
    let status = Arc::new(Mutex::new(Status::default()));
    let read = Arc::clone(&status);
    ctx.register_query("status", move || {
        let status = read.lock().unwrap_or_else(PoisonError::into_inner);
        json!({ "progress": status.progress, "calls": status.calls })
    });
    let echo = ctx.state::<Echo>().cloned();

    let (tx, rx) = std::sync::mpsc::channel();
    let job = ScriptJob {
        source: input.source,
        args: input.args,
        cwd: input.cwd,
        max_operations: input.max_operations,
    };
    bridge::spawn(job, tx).map_err(|err| format!("cannot start the script thread: {err}"))?;

    // Each sleep gets the next timer id, so a replay matches its timer.
    let mut sleeps = 0_u64;
    // A blocking receive, not an await. See `bridge` for why.
    while let Ok(request) = rx.recv() {
        match request {
            Request::Log(line) => {
                ctx.log_info(&line);
                if let Some(Echo(sink)) = &echo
                    && !ctx.is_replaying()
                {
                    sink(&line);
                }
            }
            Request::Progress(message) => {
                status
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .progress = message;
            }
            Request::Call { op, reply } => {
                status.lock().unwrap_or_else(PoisonError::into_inner).calls += 1;
                let result = match op {
                    Op::Activity { name, input } => ctx
                        .execute_activity_raw(name, input, QUEUE)
                        .await
                        .map_err(|err| err.to_string()),
                    Op::Sleep { secs } => {
                        sleeps += 1;
                        ctx.timer(&format!("sleep-{sleeps}"), secs)
                            .await
                            .map(|()| Value::Null)
                            .map_err(|err| err.to_string())
                    }
                    Op::WaitFor { signal } => ctx
                        .wait_for_signal(&signal)
                        .await
                        .map_err(|err| err.to_string()),
                };
                // The script thread can be gone after a panic. Nothing waits then.
                let _ = reply.send(result);
            }
            Request::Done(result) => return result,
        }
    }
    Err("the script thread stopped without a result".to_string())
}
