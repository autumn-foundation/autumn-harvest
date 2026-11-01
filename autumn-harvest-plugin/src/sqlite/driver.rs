//! The SQLite driver task (issue #2467).
//!
//! One task owns the [`SqliteRuntime`], which is the only write handle on the
//! Harvest file. It runs on the app's Tokio runtime, so an activity body runs
//! there too, as on the Postgres worker.
//!
//! ```text
//! HarvestSqlite (any task) ──► mpsc ──► driver task
//!                                        loop:
//!                                          take every queued command
//!                                          poll_once()  ── progress? ──► loop
//!                                          wait for a command or the tick
//! ```
//!
//! SQLite has no `LISTEN`/`NOTIFY`, so the tick stands in for it. It bounds
//! how late a due timer, a retry backoff or a schedule slot fires. A command
//! drives at once. The next pass starts as soon as the command returns.
//!
//! Exactly one command or one pass runs at a time. That is the single-writer
//! contract of `autumn-harvest-sqlite`. A long activity body therefore delays
//! the next command. Reads do not wait: they use a read-only connection.

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::time::Duration;

use autumn_harvest::context::SharedState;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::{ExecutionId, UpdateHandlerInfo};
use autumn_harvest_sqlite::{
    SignalDelivery, SignalWithStart, SignalWithStartOutcome, SqliteResult, SqliteRuntime,
    StartOptions, StartOutcome,
};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

/// How many commands may wait while the driver is busy.
///
/// A sender waits when the queue is full. The bound keeps a burst of HTTP
/// starts from growing memory while one long activity runs.
const COMMAND_BACKLOG: usize = 256;

/// A query replay, ready to run off the driver task.
pub type QueryFuture = Pin<Box<dyn Future<Output = SqliteResult<Value>> + Send>>;

/// What the driver registers before it opens the file.
pub struct Registry {
    /// The `#[workflow]` definitions.
    pub workflows: Vec<WorkflowInfo>,
    /// The `#[activity]` definitions.
    pub activities: Vec<ActivityInfo>,
    /// The declarative `#[update]` handlers.
    pub updates: Vec<UpdateHandlerInfo>,
    /// The `HarvestPlugin::state` values.
    pub state: SharedState,
}

/// An owned signal-with-start request. See [`SignalWithStart`].
#[derive(Debug, Clone)]
pub struct SignalWithStartRequest {
    /// The registered workflow type.
    pub workflow_name: String,
    /// The business id. It must not be blank.
    pub workflow_id: String,
    /// The input of a fresh run. An attach ignores it.
    pub input: Value,
    /// The reuse policy for a prior run of the key.
    pub reuse_policy: autumn_harvest::WorkflowIdReusePolicy,
    /// The signal name.
    pub signal_name: String,
    /// The signal payload.
    pub signal_payload: Value,
    /// An optional idempotency key. An empty key is no key.
    pub idempotency_key: Option<String>,
}

/// One request to the driver task.
pub enum Command {
    Start {
        workflow_name: String,
        input: Value,
        options: StartOptions,
        reply: oneshot::Sender<SqliteResult<StartOutcome>>,
    },
    SignalWithStart {
        request: SignalWithStartRequest,
        reply: oneshot::Sender<SqliteResult<SignalWithStartOutcome>>,
    },
    Signal {
        exec: ExecutionId,
        name: String,
        payload: Value,
        idempotency_key: Option<String>,
        reply: oneshot::Sender<SqliteResult<bool>>,
    },
    SignalByWorkflowId {
        workflow_name: String,
        workflow_id: String,
        name: String,
        payload: Value,
        idempotency_key: Option<String>,
        reply: oneshot::Sender<SqliteResult<SignalDelivery>>,
    },
    Cancel {
        exec: ExecutionId,
        reason: String,
        reply: oneshot::Sender<SqliteResult<()>>,
    },
    Terminate {
        exec: ExecutionId,
        reason: String,
        reply: oneshot::Sender<SqliteResult<()>>,
    },
    Query {
        exec: ExecutionId,
        name: String,
        args: Value,
        reply: oneshot::Sender<QueryFuture>,
    },
    Update {
        exec: ExecutionId,
        name: String,
        input: Value,
        reply: oneshot::Sender<SqliteResult<Value>>,
    },
    /// Stop the loop. `reply` fires once the runtime is dropped, so the
    /// writer lock is free.
    Shutdown { reply: Option<oneshot::Sender<()>> },
}

/// The running driver task.
pub struct Driver {
    pub sender: mpsc::Sender<Command>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Driver {
    /// Open the Harvest file and register every definition on it.
    ///
    /// It blocks on file IO, so the caller runs it on a blocking thread. A
    /// registration panic names the unsupported feature. It must fail the
    /// boot, so it is caught and returned.
    ///
    /// # Errors
    ///
    /// Returns the open error, such as a writer lock that another process
    /// holds, or the registration panic message.
    pub(crate) fn open(path: &Path, registry: Registry) -> Result<SqliteRuntime, String> {
        let mut runtime = SqliteRuntime::open(path).map_err(|error| {
            format!(
                "cannot open the Harvest SQLite database {}: {error}",
                path.display()
            )
        })?;
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            register(&mut runtime, registry);
        }))
        .map_err(autumn_harvest::error::panic_message)?;
        Ok(runtime)
    }

    /// Start the drive loop as a task on the app's runtime.
    ///
    /// An activity then runs where the rest of the app runs. A task that it
    /// spawns, such as a fleet open, lives on the app's runtime too.
    pub(crate) fn start(runtime: SqliteRuntime, tick: Duration) -> Self {
        let (sender, receiver) = mpsc::channel(COMMAND_BACKLOG);
        let task = tokio::spawn(async move {
            let reply = drive(runtime, receiver, tick).await;
            // `drive` owned the runtime, so the lock is released by now.
            if let Some(reply) = reply {
                let _ = reply.send(());
            }
        });
        Self {
            sender,
            task: Some(task),
        }
    }

    /// Stop the drive loop and wait for it.
    ///
    /// The loop finishes the command or pass in flight first. Dropping the
    /// runtime releases the writer lock. A task still `RUNNING` is reclaimed
    /// by the next open, so its activity runs again (at least once).
    pub(crate) async fn shutdown(mut self) {
        let _ = self.sender.send(Command::Shutdown { reply: None }).await;
        if let Some(task) = self.task.take()
            && task.await.is_err()
        {
            tracing::warn!("the harvest-sqlite driver did not stop cleanly");
        }
    }
}

fn register(runtime: &mut SqliteRuntime, registry: Registry) {
    for workflow in &registry.workflows {
        runtime.register_workflow(workflow);
    }
    for activity in &registry.activities {
        runtime.register_activity(activity);
    }
    for update in &registry.updates {
        runtime.register_update(update);
    }
    runtime.set_shared_state(registry.state);
}

/// The drive loop. It returns on [`Command::Shutdown`], with its reply, or
/// when every sender is gone. It drops the runtime as it returns.
async fn drive(
    mut runtime: SqliteRuntime,
    mut receiver: mpsc::Receiver<Command>,
    tick: Duration,
) -> Option<oneshot::Sender<()>> {
    let mut last_error: Option<String> = None;
    loop {
        loop {
            match receiver.try_recv() {
                Ok(Command::Shutdown { reply }) => return reply,
                Err(mpsc::error::TryRecvError::Disconnected) => return None,
                Ok(command) => handle(&mut runtime, command).await,
                Err(mpsc::error::TryRecvError::Empty) => break,
            }
        }
        let progressed = match runtime.poll_once().await {
            Ok(progressed) => {
                last_error = None;
                progressed
            }
            Err(error) => {
                // A broken run fails every pass. Log it once per change, and
                // wait for the tick, so it cannot spin the task.
                let text = error.to_string();
                if last_error.as_deref() != Some(text.as_str()) {
                    tracing::warn!(error = %text, "harvest-sqlite drive pass failed");
                    last_error = Some(text);
                }
                false
            }
        };
        if progressed {
            continue;
        }
        tokio::select! {
            command = receiver.recv() => match command {
                None => return None,
                Some(Command::Shutdown { reply }) => return reply,
                Some(command) => handle(&mut runtime, command).await,
            },
            () = tokio::time::sleep(tick) => {}
        }
    }
}

/// Run one command. A dropped reply means the caller left; the write stays.
async fn handle(runtime: &mut SqliteRuntime, command: Command) {
    match command {
        Command::Start {
            workflow_name,
            input,
            options,
            reply,
        } => {
            let _ = reply.send(runtime.start_workflow_with_options(&workflow_name, input, options));
        }
        Command::SignalWithStart { request, reply } => {
            let outcome = runtime.signal_with_start(SignalWithStart {
                workflow_name: &request.workflow_name,
                workflow_id: &request.workflow_id,
                input: request.input,
                reuse_policy: request.reuse_policy,
                signal_name: &request.signal_name,
                signal_payload: request.signal_payload,
                idempotency_key: request.idempotency_key.as_deref(),
            });
            let _ = reply.send(outcome);
        }
        Command::Signal {
            exec,
            name,
            payload,
            idempotency_key,
            reply,
        } => {
            let _ = reply.send(runtime.send_signal_idempotent(
                exec,
                &name,
                payload,
                idempotency_key.as_deref(),
            ));
        }
        Command::SignalByWorkflowId {
            workflow_name,
            workflow_id,
            name,
            payload,
            idempotency_key,
            reply,
        } => {
            let _ = reply.send(runtime.send_signal_by_workflow_id(
                &workflow_name,
                &workflow_id,
                &name,
                payload,
                idempotency_key.as_deref(),
            ));
        }
        Command::Cancel {
            exec,
            reason,
            reply,
        } => {
            let _ = reply.send(runtime.cancel(exec, &reason));
        }
        Command::Terminate {
            exec,
            reason,
            reply,
        } => {
            let _ = reply.send(runtime.terminate(exec, &reason));
        }
        Command::Query {
            exec,
            name,
            args,
            reply,
        } => {
            // The replay runs on the caller's task. The driver only reads the
            // rows it needs, so a slow query never holds the writer.
            let _ = reply.send(Box::pin(runtime.query(exec, &name, args)));
        }
        Command::Update {
            exec,
            name,
            input,
            reply,
        } => {
            let _ = reply.send(runtime.update(exec, &name, input).await);
        }
        // `drive` takes a shutdown before it calls `handle`.
        Command::Shutdown { .. } => {}
    }
}
