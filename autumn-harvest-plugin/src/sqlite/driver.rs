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
//!
//! The retention pass runs between drive passes, on its own interval (issue
//! #2471). So it is one more writer turn, never a second writer.

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use autumn_harvest::context::SharedState;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::policy::WorkflowSchedule;
use autumn_harvest::retention::{RetentionConfig, RetentionStatus, RetentionTickResult};
use autumn_harvest::{ExecutionId, UpdateHandlerInfo};
use autumn_harvest_sqlite::{
    AdmittedUpdate, PurgePolicy, SignalDelivery, SignalWithStart, SignalWithStartOutcome,
    SqliteError, SqliteResult, SqliteRuntime, StartOptions, StartOutcome,
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

tokio::task_local! {
    /// Set on a driver task. An activity and a workflow run on that task.
    static DRIVER_TASK: ();
}

/// Whether the caller runs on a driver task, as an activity or a workflow
/// does (issue #2470).
///
/// A call there that waits for the driver would wait for itself.
pub fn on_driver() -> bool {
    DRIVER_TASK.try_with(|()| ()).is_ok()
}

/// Run `future` as if on a driver task, for a test of [`on_driver`].
#[cfg(test)]
pub async fn scope_as_driver<F: Future>(future: F) -> F::Output {
    DRIVER_TASK.scope((), future).await
}

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
    /// The declarative `#[query]` handlers (issue #2468).
    pub queries: Vec<autumn_harvest::QueryHandlerInfo>,
    /// The durable log sink policy. `None` stores no line (issue #2468).
    pub log_policy: Option<autumn_harvest::context::WorkflowLogPolicy>,
    /// The `HarvestPlugin::state` values.
    pub state: SharedState,
    /// The code schedules (issue #2471). Only the control Harvest gets them.
    pub schedules: Vec<WorkflowSchedule>,
    /// `harvest.scheduler_enabled`. `false` fires no schedule.
    pub scheduler_enabled: bool,
    /// `HarvestPlugin::retention`. Each driver runs it on its own file.
    pub retention: RetentionConfig,
}

impl Registry {
    /// A registry with no definition, for a test.
    #[cfg(test)]
    pub fn empty() -> Self {
        Self {
            workflows: Vec::new(),
            activities: Vec::new(),
            updates: Vec::new(),
            signals: Vec::new(),
            queries: Vec::new(),
            log_policy: None,
            state: autumn_harvest::context::empty_shared_state(),
            schedules: Vec::new(),
            scheduler_enabled: true,
            retention: RetentionConfig::default(),
        }
    }
}

/// A write that runs on the driver task, in turn with every other write
/// (issue #2471). The admin routes send one for each schedule change.
pub type Job = Box<dyn FnOnce(&mut SqliteRuntime) + Send>;

/// The batches of one retention step. The driver yields between steps, so
/// a large backlog never holds the task for long (issue #2471).
const RETENTION_STEP_BATCHES: u32 = 8;

/// The retention of one Harvest file (issue #2471).
///
/// The driver runs a pass on its interval, one step at a time. Each step
/// removes at most [`RETENTION_STEP_BATCHES`] batches. `POST
/// /admin/retention/run-now` queues a pass, as on Postgres. `GET
/// /admin/retention` reads the status of the last finished pass.
pub struct Retention {
    config: RetentionConfig,
    /// `None` when no age is set. A pass then removes nothing.
    policy: Option<PurgePolicy>,
    every: Duration,
    status: Mutex<RetentionStatus>,
    /// The pass in progress, if any.
    pass: Mutex<Option<(Instant, RetentionTickResult)>>,
    /// Set by run-now. The driver takes it on its next turn.
    requested: AtomicBool,
}

impl Retention {
    pub fn new(config: &RetentionConfig) -> Self {
        Self {
            config: config.clone(),
            policy: purge_policy(config).map(|policy| policy.batch_limit(RETENTION_STEP_BATCHES)),
            every: Duration::from_secs(config.tick_interval_secs.max(1)),
            status: Mutex::new(RetentionStatus {
                config: config.clone(),
                // One file is one shard.
                per_shard: vec![RetentionTickResult::default()],
            }),
            pass: Mutex::new(None),
            requested: AtomicBool::new(false),
        }
    }

    /// The status of the last finished pass, in the Postgres shape.
    pub fn status(&self) -> RetentionStatus {
        self.status
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Queue a pass. The driver runs it on its next turn.
    pub fn request(&self) {
        self.requested.store(true, Ordering::Release);
    }

    fn take_request(&self) -> bool {
        self.requested.swap(false, Ordering::AcqRel)
    }

    /// The retention age of `workflow_name`, as on Postgres. An MCP task
    /// reports it as its TTL.
    #[cfg(feature = "mcp")]
    pub fn effective_max_age(&self, workflow_name: &str) -> Option<Duration> {
        self.policy
            .as_ref()
            .and_then(|_| self.config.effective_max_age(workflow_name))
    }

    /// Whether the driver runs a pass on its own.
    const fn periodic(&self) -> bool {
        self.policy.is_some()
    }

    /// Run one step of the pass. Returns `true` while more remains.
    ///
    /// The counts add up over the steps of one pass. A dry run counts the
    /// would-be deletes in `deleted_count`, as on Postgres. A dry run takes one
    /// step only, so its count is a lower bound on a large backlog.
    fn step(&self, runtime: &mut SqliteRuntime) -> bool {
        let (started, mut tick) = self
            .pass
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .unwrap_or_else(|| {
                (
                    Instant::now(),
                    RetentionTickResult {
                        ran_at: Some(chrono::Utc::now()),
                        ..RetentionTickResult::default()
                    },
                )
            });
        let more = match &self.policy {
            None => false,
            Some(policy) => match runtime.purge_with(policy) {
                Ok(report) => {
                    let removed = usize::try_from(report.removed).unwrap_or(usize::MAX);
                    tick.candidate_count = tick.candidate_count.saturating_add(removed);
                    tick.deleted_count = tick.deleted_count.saturating_add(removed);
                    for (workflow, count) in report.by_workflow {
                        *tick.deleted_by_workflow.entry(workflow).or_default() += count;
                    }
                    !report.complete && !report.dry_run
                }
                Err(error) => {
                    tracing::warn!(%error, "harvest-sqlite retention pass failed");
                    tick.last_error = Some(error.to_string());
                    false
                }
            },
        };
        tick.duration_ms = started.elapsed().as_millis();
        if more {
            *self.pass.lock().unwrap_or_else(PoisonError::into_inner) = Some((started, tick));
        } else {
            *self.status.lock().unwrap_or_else(PoisonError::into_inner) = RetentionStatus {
                config: self.config.clone(),
                per_shard: vec![tick],
            };
        }
        more
    }
}

/// The purge policy of `config`, or `None` when it sets no age.
///
/// The ages are the Postgres ones: `max_age_secs` and the per-type
/// `overrides`. The build refuses the fields that SQLite cannot honour.
pub fn purge_policy(config: &RetentionConfig) -> Option<PurgePolicy> {
    if config.max_age_secs.is_none() && config.overrides.is_empty() {
        return None;
    }
    let mut policy = PurgePolicy::new().dry_run(config.dry_run);
    if let Some(secs) = config.max_age_secs {
        policy = policy.max_age(Duration::from_secs(secs));
    }
    for (workflow, secs) in &config.overrides {
        policy = policy.override_age(workflow.clone(), Duration::from_secs(*secs));
    }
    Some(policy)
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
    /// Return any run of the key, or else start one (issue #2470).
    StartOnce {
        workflow_name: String,
        input: Value,
        options: StartOptions,
        reply: oneshot::Sender<SqliteResult<StartOutcome>>,
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
    /// Run a [`Job`] on the runtime (issue #2471).
    Run(Job),
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
        Self::open_with(path, registry, false)
    }

    /// As [`open`](Self::open). With `replicated`, the runtime never
    /// checkpoints, because a replicator ships the file.
    ///
    /// # Errors
    ///
    /// As [`open`](Self::open).
    pub(crate) fn open_with(
        path: &Path,
        registry: &Registry,
        replicated: bool,
    ) -> Result<SqliteRuntime, String> {
        let opened = if replicated {
            SqliteRuntime::open_replicated(path)
        } else {
            SqliteRuntime::open(path)
        };
        let mut runtime = opened.map_err(|error| {
            format!(
                "cannot open the Harvest SQLite database {}: {error}",
                path.display()
            )
        })?;
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            register(&mut runtime, registry);
        }))
        .map_err(autumn_harvest::error::panic_message)?;
        runtime.set_schedules_enabled(registry.scheduler_enabled);
        Ok(runtime)
    }

    /// Upsert each code schedule of `registry` (issue #2471).
    ///
    /// The control Harvest calls it once at open. A disabled scheduler
    /// upserts none, as on Postgres, where the scheduler upserts them.
    ///
    /// # Errors
    ///
    /// Returns a message that names the schedule and the refusal, such as an
    /// unknown workflow or a field that this backend cannot honour.
    pub(crate) fn upsert_code_schedules(
        runtime: &mut SqliteRuntime,
        registry: &Registry,
    ) -> Result<(), String> {
        if !registry.scheduler_enabled {
            return Ok(());
        }
        for schedule in &registry.schedules {
            runtime.upsert_schedule(schedule).map_err(|error| {
                format!(
                    "cannot register the schedule of {} on SQLite: {error}",
                    schedule.workflow_name
                )
            })?;
        }
        Ok(())
    }

    /// Start the drive loop as a task on the app's runtime.
    ///
    /// An activity then runs where the rest of the app runs. A task that it
    /// spawns, such as a fleet open, lives on the app's runtime too.
    pub(crate) fn start(runtime: SqliteRuntime, tick: Duration, retention: Arc<Retention>) -> Self {
        let (sender, receiver) = mpsc::channel(COMMAND_BACKLOG);
        let stop = CancellationToken::new();
        let task = tokio::spawn(async move {
            let reply = DRIVER_TASK
                .scope((), drive(runtime, receiver, tick, &stop, &retention))
                .await;
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
        retention: Arc<Retention>,
        key: FleetDbKey,
        stop: CancellationToken,
        on_exit: Box<dyn FnOnce() + Send>,
    ) -> (mpsc::Sender<Command>, tokio::task::JoinHandle<()>) {
        let (sender, receiver) = mpsc::channel(COMMAND_BACKLOG);
        let task = tokio::spawn(super::fleet::scoped(key, async move {
            // Declared first, so it runs last: after the runtime drops, on a
            // return and on an abort alike.
            let _exit = OnExit(Some(on_exit));
            let reply = DRIVER_TASK
                .scope((), drive(runtime, receiver, tick, &stop, &retention))
                .await;
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
        // The build warns once about an unserved `mcp` flag (issue #2609).
        // A per-registration warning would repeat for each tenant reopen.
        runtime.register_workflow_mcp_served(workflow);
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
    for query in &registry.queries {
        runtime.register_query(query);
    }
    runtime.set_workflow_log_policy(registry.log_policy);
    runtime.set_shared_state(Arc::clone(&registry.state));
}

/// The drive loop. It returns on [`Command::Shutdown`], with its reply, when
/// every sender is gone, or when `stop` fires. It drops the runtime as it
/// returns.
///
/// A periodic retention runs its first pass at once, then on its interval.
async fn drive(
    mut runtime: SqliteRuntime,
    mut receiver: mpsc::Receiver<Command>,
    tick: Duration,
    stop: &CancellationToken,
    retention: &Retention,
) -> Option<oneshot::Sender<()>> {
    let mut last_error: Option<String> = None;
    let mut next_purge = retention.periodic().then(Instant::now);
    let mut purging = false;
    loop {
        if purging || next_purge.is_some_and(|at| Instant::now() >= at) || retention.take_request()
        {
            purging = retention.step(&mut runtime);
            if !purging {
                next_purge = retention
                    .periodic()
                    .then(|| Instant::now() + retention.every);
            }
        }
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
        // A retention pass in progress takes its next step after a yield.
        if progressed || purging {
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
        Command::StartOnce {
            workflow_name,
            input,
            options,
            reply,
        } => {
            let _ = reply.send(start_once(runtime, &workflow_name, input, options));
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
            let routed = signal(runtime, exec, &name, payload, idempotency_key.as_deref());
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
        Command::Run(job) => run_job(runtime, job),
        // `drive` takes a shutdown before it calls `handle`.
        Command::Shutdown { .. } => {}
    }
}

/// Run one job. A panic in it must not end the driver. The job's reply
/// drops, so its caller gets `Stopped` for that call only.
fn run_job(runtime: &mut SqliteRuntime, job: Job) {
    if let Err(panic) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job(runtime))) {
        tracing::error!(
            panic = %autumn_harvest::error::panic_message(panic),
            "a harvest-sqlite driver job panicked"
        );
    }
}

/// Signal the live attempt of `exec`.
fn signal(
    runtime: &mut SqliteRuntime,
    exec: ExecutionId,
    name: &str,
    payload: Value,
    idempotency_key: Option<&str>,
) -> SqliteResult<RoutedSignal> {
    let exec = live_attempt(runtime, exec)?;
    runtime
        .send_signal_idempotent(exec, name, payload, idempotency_key)
        .map(|delivered| RoutedSignal { exec, delivered })
}

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

/// Return the newest run of the key in `options`, in any state, or else
/// start one (issue #2470).
///
/// The outbox relay starts each row with this. A sealed prior, such as a
/// terminated run, counts too. A redelivery after a crash therefore never
/// runs a start twice. The driver runs one command at a time, so no write
/// comes between the check and the start.
///
/// # Errors
///
/// Returns [`SqliteError::BlankWorkflowId`] when the key has no id. A start
/// with no id would run again on each redelivery.
fn start_once(
    runtime: &mut SqliteRuntime,
    workflow_name: &str,
    input: Value,
    options: StartOptions,
) -> SqliteResult<StartOutcome> {
    let Some(workflow_id) = options
        .workflow_id
        .clone()
        .filter(|id| !id.trim().is_empty())
    else {
        return Err(SqliteError::BlankWorkflowId);
    };
    if let Some(exec_id) = runtime.newest_run_of_key(workflow_name, &workflow_id)? {
        return Ok(StartOutcome {
            exec_id,
            created: false,
        });
    }
    runtime.start_workflow_with_options(workflow_name, input, options)
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

    /// Waits for a signal that never comes, so a run stays `RUNNING`.
    #[workflow]
    async fn parked(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
        ctx.wait_for_signal("never")
            .await
            .map_err(|error| error.to_string())?;
        Ok(n)
    }

    /// `StartOnce` returns any run of the key, a sealed one too. It starts
    /// a run only for a key with none (issue #2470).
    #[test]
    fn start_once_returns_any_run_of_the_key() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut runtime = SqliteRuntime::open(&dir.path().join("h.db")).expect("open");
        runtime.register_workflow(&parked_info());
        let key = |id: &str| StartOptions::new().workflow_id(id);

        let first = start_once(&mut runtime, "parked", serde_json::json!(1), key("k")).unwrap();
        assert!(first.created);
        let again = start_once(&mut runtime, "parked", serde_json::json!(2), key("k")).unwrap();
        assert_eq!(again.exec_id, first.exec_id);
        assert!(!again.created);

        // The default reuse policy starts over a terminated run. This must not.
        runtime.terminate(first.exec_id, "operator").unwrap();
        let sealed = start_once(&mut runtime, "parked", serde_json::json!(3), key("k")).unwrap();
        assert_eq!(sealed.exec_id, first.exec_id);
        assert!(!sealed.created);

        // Another key starts its own run.
        let other = start_once(&mut runtime, "parked", serde_json::json!(4), key("j")).unwrap();
        assert!(other.created);
        assert_ne!(other.exec_id, first.exec_id);

        for blank in [StartOptions::new(), key("  ")] {
            assert!(matches!(
                start_once(&mut runtime, "parked", serde_json::json!(5), blank),
                Err(SqliteError::BlankWorkflowId)
            ));
        }
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
                    ..Registry::empty()
                };
                let runtime = Driver::open(&path, &registry).expect("open");
                let retention = Arc::new(Retention::new(&registry.retention));
                let driver = Driver::start(runtime, Duration::from_millis(250), retention);
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

    #[workflow]
    async fn once(_ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
        Ok(n)
    }

    fn every_second() -> WorkflowSchedule {
        WorkflowSchedule::new(
            "once",
            autumn_harvest::policy::Schedule::Interval(Duration::from_secs(1)),
        )
    }

    /// `harvest.scheduler_enabled = false` upserts no code schedule, and a
    /// stored schedule does not fire (issue #2471).
    #[tokio::test]
    async fn a_disabled_scheduler_upserts_and_fires_nothing() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("h.db");
        let on = Registry {
            workflows: vec![once_info()],
            schedules: vec![every_second()],
            ..Registry::empty()
        };
        let mut runtime = Driver::open(&path, &on).expect("open");
        Driver::upsert_code_schedules(&mut runtime, &on).expect("upsert");
        assert_eq!(runtime.list_schedules().expect("list").len(), 1);
        drop(runtime);

        let off = Registry {
            scheduler_enabled: false,
            schedules: vec![every_second()],
            ..on
        };
        let mut runtime = Driver::open(&path, &off).expect("reopen");
        runtime.delete_schedule("once").expect("delete");
        Driver::upsert_code_schedules(&mut runtime, &off).expect("no upsert");
        assert!(runtime.list_schedules().expect("list").is_empty());

        // A schedule from the admin API stays, and does not fire.
        runtime
            .upsert_schedule(&every_second())
            .expect("admin upsert");
        let later = chrono::Utc::now() + chrono::Duration::seconds(5);
        runtime.poll_once_as_of(later).await.expect("poll");
        let runs = runtime
            .list_executions(&autumn_harvest_sqlite::ExecutionFilter::new())
            .expect("runs");
        assert!(runs.executions.is_empty(), "no slot fires");
    }

    /// An unknown workflow in a code schedule fails the open, and names it.
    #[test]
    fn a_code_schedule_of_an_unknown_workflow_is_refused() {
        let dir = tempfile::tempdir().expect("temp dir");
        let registry = Registry {
            schedules: vec![every_second()],
            ..Registry::empty()
        };
        let mut runtime = Driver::open(&dir.path().join("h.db"), &registry).expect("open");
        let error = Driver::upsert_code_schedules(&mut runtime, &registry).unwrap_err();
        assert!(error.contains("once"), "{error}");
    }
}
