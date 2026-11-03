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
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::context::SharedState;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::{ExecutionId, UpdateHandlerInfo};
use autumn_harvest_sqlite::{
    AdmittedUpdate, SignalDelivery, SignalWithStart, SignalWithStartOutcome, SqliteError,
    SqliteResult, SqliteRuntime, StartOptions, StartOutcome,
};
use autumn_web::fleet_layout::FleetDbKey;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

/// The most commands the driver runs before each drive pass.
///
/// Without a bound, a steady stream of starts or signals keeps the queue
/// non-empty, and the loop never reaches `poll_once`. Runs, due timers and
/// retries would then stop moving while every request still succeeds.
const COMMANDS_PER_PASS: usize = 32;

/// How many commands may wait while the driver is busy.
///
/// A sender waits when the queue is full. The bound keeps a burst of HTTP
/// starts from growing memory while one long activity runs.
const COMMAND_BACKLOG: usize = 256;

/// A query replay, ready to run off the driver task.
pub type QueryFuture = Pin<Box<dyn Future<Output = SqliteResult<Value>> + Send>>;

/// What the driver registers before it opens the file.
///
/// The control driver and each tenant driver register from one registry.
pub struct Registry {
    /// The `#[workflow]` definitions.
    pub workflows: Vec<WorkflowInfo>,
    /// The `#[activity]` definitions.
    pub activities: Vec<ActivityInfo>,
    /// The declarative `#[update]` handlers.
    pub updates: Vec<UpdateHandlerInfo>,
    /// The declarative `#[signal]` handlers, for their payload schemas.
    pub signals: Vec<autumn_harvest::SignalHandlerInfo>,
    /// The `HarvestPlugin::state` values.
    pub state: SharedState,
}

/// An owned signal-with-start request. See [`SignalWithStart`].
#[derive(Debug, Clone)]
pub struct SignalWithStartParams {
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

/// The most continue-as-new hops that a `/result` walk follows, as on
/// Postgres.
pub const MAX_CHAIN_HOPS: usize = 128;

/// The most retry hops that one walk of a retry chain follows, as on
/// Postgres (issue #843).
pub const MAX_RETRY_HOPS: usize = autumn_harvest::RETRY_CHAIN_MAX_DEPTH;

/// A run that a start or a signal-with-start resolved to, with its state at
/// that moment (issue #2489).
///
/// The driver reads the state in the same command, so no drive runs between
/// the write and the read. The response then reports the state that the
/// write left, as on Postgres.
#[derive(Debug, Clone)]
pub struct Started<T> {
    /// The outcome of the write.
    pub outcome: T,
    /// The state of the run right after the write.
    pub state: String,
    /// The business id of the run. A start with no id gets one.
    pub workflow_id: Option<String>,
}

impl<T> Started<T> {
    /// Read the state and the business id of `exec` right after a write.
    ///
    /// A failed read reports `RUNNING` and no id, because a committed start
    /// must not answer an error. A client would retry it and start a second
    /// run.
    fn read(runtime: &SqliteRuntime, exec: ExecutionId, outcome: T) -> Self {
        let summary = runtime.describe(exec).ok().map(|row| row.summary);
        Self {
            outcome,
            state: summary
                .as_ref()
                .and_then(|summary| summary.state.value().cloned())
                .unwrap_or_else(|| "RUNNING".to_owned()),
            workflow_id: summary.and_then(|summary| summary.workflow_id.into_value()),
        }
    }
}

/// A signal that reached a run (issue #2489).
#[derive(Debug, Clone, Copy)]
pub struct RoutedSignal {
    /// The run that took the signal. It is a retry when the signal named a
    /// failed run (issue #843).
    pub exec: ExecutionId,
    /// `false` when the idempotency key was already delivered.
    pub delivered: bool,
}

/// The answer to a cancel or a terminate (issue #2489).
#[derive(Debug, Clone)]
pub struct StopAck {
    /// The run that the request acted on.
    pub exec: ExecutionId,
    /// The state of that run after the request.
    pub state: String,
    /// The reason that the run keeps.
    pub reason: String,
    /// `false` when an earlier request already did the same.
    pub newly: bool,
}

/// A signal by business key, with the run that the key resolved to
/// (issue #2489).
///
/// The driver resolves the key and delivers the signal in one command, so
/// no other write runs between them. The answer then names the run that
/// governed the delivery.
#[derive(Debug)]
pub struct KeyedSignal {
    /// The run that the key resolved to: the running run of the key, or
    /// else the newest, as on Postgres. `None` when the key has no run.
    pub resolved: Option<ExecutionId>,
    /// The delivery, or the refusal.
    pub delivery: SqliteResult<SignalDelivery>,
}

/// An update that the driver ran on a run (issue #2489).
#[derive(Debug, Clone)]
pub struct RoutedUpdate {
    /// The run that took the update.
    pub exec: ExecutionId,
    /// The admitted update.
    pub update: AdmittedUpdate,
}

/// One request to the driver task.
pub enum Command {
    Start {
        workflow_name: String,
        input: Value,
        options: StartOptions,
        reply: oneshot::Sender<SqliteResult<Started<StartOutcome>>>,
    },
    SignalWithStart {
        request: SignalWithStartParams,
        reply: oneshot::Sender<SqliteResult<Started<SignalWithStartOutcome>>>,
    },
    Signal {
        exec: ExecutionId,
        name: String,
        payload: Value,
        idempotency_key: Option<String>,
        reply: oneshot::Sender<SqliteResult<RoutedSignal>>,
    },
    SignalByWorkflowId {
        workflow_name: String,
        workflow_id: String,
        name: String,
        payload: Value,
        idempotency_key: Option<String>,
        reply: oneshot::Sender<KeyedSignal>,
    },
    Cancel {
        exec: ExecutionId,
        reason: String,
        reply: oneshot::Sender<SqliteResult<StopAck>>,
    },
    Terminate {
        exec: ExecutionId,
        reason: String,
        reply: oneshot::Sender<SqliteResult<StopAck>>,
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
        reply: oneshot::Sender<SqliteResult<RoutedUpdate>>,
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
    pub(crate) fn open(path: &Path, registry: &Registry) -> Result<SqliteRuntime, String> {
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
        let stop = CancellationToken::new();
        let task = tokio::spawn(async move {
            let reply = drive(runtime, receiver, tick, &stop).await;
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

    /// Start the drive loop of one fleet tenant (issue #2472).
    ///
    /// The loop runs with `key` as [`HarvestTenant::current`]. It stops when
    /// `stop` fires, after the command or pass in flight. The caller hands
    /// the task to the fleet, which waits for it before the tenant closes.
    /// `on_exit` runs when the task ends or is aborted.
    ///
    /// [`HarvestTenant::current`]: super::HarvestTenant::current
    pub(crate) fn spawn_tenant(
        runtime: SqliteRuntime,
        tick: Duration,
        key: FleetDbKey,
        stop: CancellationToken,
        on_exit: Box<dyn FnOnce() + Send>,
    ) -> (mpsc::Sender<Command>, tokio::task::JoinHandle<()>) {
        let (sender, receiver) = mpsc::channel(COMMAND_BACKLOG);
        let task = tokio::spawn(super::fleet::scoped(key, async move {
            // Declared first, so it runs last: after the runtime drops, on a
            // return and on an abort alike.
            let _exit = OnExit(Some(on_exit));
            let reply = drive(runtime, receiver, tick, &stop).await;
            if let Some(reply) = reply {
                let _ = reply.send(());
            }
        }));
        (sender, task)
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

/// Runs its closure when dropped.
struct OnExit(Option<Box<dyn FnOnce() + Send>>);

impl Drop for OnExit {
    fn drop(&mut self) {
        if let Some(on_exit) = self.0.take() {
            on_exit();
        }
    }
}

fn register(runtime: &mut SqliteRuntime, registry: &Registry) {
    for workflow in &registry.workflows {
        runtime.register_workflow(workflow);
    }
    for activity in &registry.activities {
        runtime.register_activity(activity);
    }
    for update in &registry.updates {
        runtime.register_update(update);
    }
    for signal in &registry.signals {
        runtime.register_signal_handler(signal);
    }
    runtime.set_shared_state(Arc::clone(&registry.state));
}

/// The drive loop. It returns on [`Command::Shutdown`], with its reply, when
/// every sender is gone, or when `stop` fires. It drops the runtime as it
/// returns.
async fn drive(
    mut runtime: SqliteRuntime,
    mut receiver: mpsc::Receiver<Command>,
    tick: Duration,
    stop: &CancellationToken,
) -> Option<oneshot::Sender<()>> {
    let mut last_error: Option<String> = None;
    loop {
        if stop.is_cancelled() {
            return None;
        }
        for _ in 0..COMMANDS_PER_PASS {
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
            // A pass can finish without awaiting anything: the store is
            // synchronous SQLite. A chain of such passes, such as a workflow
            // that keeps continuing as new, would then hold the executor. On
            // a current-thread runtime that starves HTTP handlers, timers and
            // the shutdown. Yield before the next pass.
            tokio::task::yield_now().await;
            continue;
        }
        tokio::select! {
            () = stop.cancelled() => return None,
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
            let started = runtime
                .start_workflow_with_options(&workflow_name, input, options)
                .map(|outcome| Started::read(runtime, outcome.exec_id, outcome));
            let _ = reply.send(started);
        }
        Command::SignalWithStart { request, reply } => {
            let _ = reply.send(signal_with_start(runtime, request));
        }
        Command::Signal {
            exec,
            name,
            payload,
            idempotency_key,
            reply,
        } => {
            let routed = live_attempt(runtime, exec).and_then(|exec| {
                runtime
                    .send_signal_idempotent(exec, &name, payload, idempotency_key.as_deref())
                    .map(|delivered| RoutedSignal { exec, delivered })
            });
            let _ = reply.send(routed);
        }
        Command::SignalByWorkflowId {
            workflow_name,
            workflow_id,
            name,
            payload,
            idempotency_key,
            reply,
        } => {
            let resolved = resolve_key(runtime, &workflow_name, &workflow_id);
            let delivery = signal_by_workflow_id(
                runtime,
                &workflow_name,
                &workflow_id,
                &name,
                payload,
                idempotency_key.as_deref(),
            );
            let _ = reply.send(KeyedSignal { resolved, delivery });
        }
        Command::Cancel {
            exec,
            reason,
            reply,
        } => {
            let _ = reply.send(cancel(runtime, exec, &reason));
        }
        Command::Terminate {
            exec,
            reason,
            reply,
        } => {
            let _ = reply.send(terminate(runtime, exec, &reason));
        }
        Command::Query {
            exec,
            name,
            args,
            reply,
        } => {
            // The replay runs on the caller's task. The driver only reads the
            // rows it needs, so a slow query never holds the writer.
            let replay: QueryFuture = match live_attempt(runtime, exec) {
                Ok(exec) => Box::pin(runtime.query(exec, &name, args)),
                Err(error) => Box::pin(std::future::ready(Err(error))),
            };
            let _ = reply.send(replay);
        }
        Command::Update {
            exec,
            name,
            input,
            reply,
        } => {
            let routed = match live_attempt(runtime, exec) {
                Ok(exec) => runtime
                    .update_admitted(exec, &name, input)
                    .await
                    .map(|update| RoutedUpdate { exec, update }),
                Err(error) => Err(error),
            };
            let _ = reply.send(routed);
        }
        // `drive` takes a shutdown before it calls `handle`.
        Command::Shutdown { .. } => {}
    }
}

/// The stored state of `exec`.
fn state_of(runtime: &SqliteRuntime, exec: ExecutionId) -> SqliteResult<String> {
    Ok(runtime
        .describe(exec)?
        .summary
        .state
        .into_value()
        .unwrap_or_default())
}

/// The live attempt of `exec` (issue #843).
///
/// A `FAILED` run that started a workflow retry hands each write to that
/// retry, as on Postgres. The walk follows the retry chain only. A
/// `CONTINUED_AS_NEW` run is not routed, as on Postgres. A retry row that is
/// gone ends the walk at the run before it.
///
/// # Errors
///
/// Returns [`SqliteError::RetryChainTooDeep`] past [`MAX_RETRY_HOPS`]. The
/// walk fails closed, so a write never lands on a failed run while its
/// retry runs on.
fn live_attempt(runtime: &SqliteRuntime, exec: ExecutionId) -> SqliteResult<ExecutionId> {
    let mut current = exec;
    for _ in 0..MAX_RETRY_HOPS {
        if state_of(runtime, current)? != "FAILED" {
            return Ok(current);
        }
        let Some(next) = runtime.successor(current)? else {
            return Ok(current);
        };
        match runtime.describe(next) {
            Ok(_) => current = next,
            Err(SqliteError::ExecutionNotFound(_)) => return Ok(current),
            Err(error) => return Err(error),
        }
    }
    if state_of(runtime, current)? == "FAILED" && runtime.successor(current)?.is_some() {
        return Err(SqliteError::RetryChainTooDeep {
            exec_id: exec,
            max_depth: MAX_RETRY_HOPS,
        });
    }
    Ok(current)
}

/// Run a signal-with-start, and read the state that it left.
fn signal_with_start(
    runtime: &mut SqliteRuntime,
    request: SignalWithStartParams,
) -> SqliteResult<Started<SignalWithStartOutcome>> {
    let outcome = runtime.signal_with_start(SignalWithStart {
        workflow_name: &request.workflow_name,
        workflow_id: &request.workflow_id,
        input: request.input,
        reuse_policy: request.reuse_policy,
        signal_name: &request.signal_name,
        signal_payload: request.signal_payload,
        idempotency_key: request.idempotency_key.as_deref(),
    })?;
    Ok(Started::read(runtime, outcome.exec_id, outcome))
}

/// The run of a `(workflow_name, workflow_id)` key: the running run, or else
/// the newest, as on Postgres. `None` when the key has no run or the read
/// fails. The answer then carries no header, and the signal reports the
/// error.
fn resolve_key(
    runtime: &SqliteRuntime,
    workflow_name: &str,
    workflow_id: &str,
) -> Option<ExecutionId> {
    let key = autumn_harvest_sqlite::ExecutionFilter::new()
        .workflow_name(workflow_name.to_owned())
        .workflow_id(workflow_id.to_owned())
        .limit(1);
    [key.clone().state("RUNNING"), key]
        .iter()
        .find_map(|filter| {
            runtime
                .list_executions(filter)
                .ok()?
                .executions
                .first()
                .and_then(autumn_harvest_sqlite::ExecutionSummary::execution_id)
        })
}

/// Signal the run of a `(workflow_name, workflow_id)` key.
///
/// A key that names a failed run hands the signal to its live retry, as on
/// Postgres (issue #843). A retry has its own workflow id, so the key lookup
/// cannot find it.
fn signal_by_workflow_id(
    runtime: &mut SqliteRuntime,
    workflow_name: &str,
    workflow_id: &str,
    name: &str,
    payload: Value,
    idempotency_key: Option<&str>,
) -> SqliteResult<SignalDelivery> {
    let refused = match runtime.send_signal_by_workflow_id(
        workflow_name,
        workflow_id,
        name,
        payload.clone(),
        idempotency_key,
    ) {
        Err(SqliteError::WorkflowNotRunning {
            execution_id,
            state,
        }) => (execution_id, state),
        other => return other,
    };
    let target = live_attempt(runtime, refused.0)?;
    if target == refused.0 {
        return Err(SqliteError::WorkflowNotRunning {
            execution_id: refused.0,
            state: refused.1,
        });
    }
    let delivered = runtime.send_signal_idempotent(target, name, payload, idempotency_key)?;
    Ok(SignalDelivery {
        exec_id: target,
        delivered,
    })
}

/// The answer to a request that finds the run terminal: Postgres answers it
/// as a no-op (issue #1456). The stored error wins over the state phrase.
fn already_stopped(runtime: &SqliteRuntime, exec: ExecutionId) -> SqliteResult<StopAck> {
    let row = runtime.describe(exec)?;
    let state = row.summary.state.into_value().unwrap_or_default();
    let reason = row.error.into_value().unwrap_or_else(|| {
        autumn_harvest::CancelledWorkflowExecution::default_idempotent_reason(&state)
    });
    Ok(StopAck {
        exec,
        state,
        reason,
        newly: false,
    })
}

/// A cooperative cancel of the live attempt of `exec`.
///
/// A cancelled run and a repeat request answer as no-ops, with the reason
/// that the run keeps. A completed, failed or terminated run is refused, as
/// on Postgres.
fn cancel(runtime: &mut SqliteRuntime, exec: ExecutionId, reason: &str) -> SqliteResult<StopAck> {
    let exec = live_attempt(runtime, exec)?;
    // A run past its deadline ends `TIMED_OUT` first, as every other write
    // does. A repeat cancel then answers `409`, not a stale `RUNNING`.
    runtime.seal_if_overdue(exec)?;
    if state_of(runtime, exec)? == "CANCELLED" {
        return already_stopped(runtime, exec);
    }
    let newly = runtime.pending_cancel_reason(exec)?.is_none();
    if newly {
        runtime.cancel(exec, reason)?;
    }
    let reason = runtime.pending_cancel_reason(exec)?.unwrap_or_default();
    Ok(StopAck {
        exec,
        state: "RUNNING".to_owned(),
        reason,
        newly,
    })
}

/// A terminate of the live attempt of `exec`. A terminal run answers as a
/// no-op, as on Postgres.
fn terminate(
    runtime: &mut SqliteRuntime,
    exec: ExecutionId,
    reason: &str,
) -> SqliteResult<StopAck> {
    let exec = live_attempt(runtime, exec)?;
    match runtime.terminate(exec, reason) {
        Ok(()) => {
            let mut ack = already_stopped(runtime, exec)?;
            ack.newly = true;
            Ok(ack)
        }
        // A run past its deadline ends `TIMED_OUT` in the same call.
        Err(SqliteError::WorkflowNotRunning { .. }) => already_stopped(runtime, exec),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    // The `#[workflow]` macro needs an `async fn`.
    #![allow(clippy::unused_async)]

    use super::*;
    use autumn_harvest::prelude::*;

    /// Continues as new forever. Each pass makes progress, and no pass
    /// awaits anything outside the store.
    #[workflow]
    async fn forever(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
        ctx.continue_as_new(serde_json::json!(n + 1))
            .await
            .map_err(|error| error.to_string())?;
        Ok(n)
    }

    /// On a current-thread runtime, a chain of passes that never awaits
    /// must not hold the executor. Without a yield between passes, the test
    /// task is never polled again, and the watchdog below fires.
    #[test]
    fn a_progress_chain_does_not_starve_a_current_thread_runtime() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("h.db");
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let tokio = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            tokio.block_on(async move {
                let registry = Registry {
                    workflows: vec![forever_info()],
                    activities: Vec::new(),
                    updates: Vec::new(),
                    signals: Vec::new(),
                    state: autumn_harvest::context::empty_shared_state(),
                };
                let runtime = Driver::open(&path, &registry).expect("open");
                let driver = Driver::start(runtime, Duration::from_millis(250));
                let (reply, started) = oneshot::channel();
                driver
                    .sender
                    .send(Command::Start {
                        workflow_name: "forever".to_owned(),
                        input: serde_json::json!(0),
                        options: StartOptions::new(),
                        reply,
                    })
                    .await
                    .expect("send");
                started.await.expect("reply").expect("start");
                // Another task on the same runtime must still run.
                tokio::time::sleep(Duration::from_millis(50)).await;
                driver.shutdown().await;
                let _ = done_tx.send(());
            });
        });
        assert!(
            done_rx.recv_timeout(Duration::from_secs(30)).is_ok(),
            "the drive loop starved the current-thread runtime"
        );
    }
}
