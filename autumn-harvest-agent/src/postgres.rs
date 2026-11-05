//! The agent loop and durable agents on Postgres.
//!
//! This module mirrors `sqlite`. Each function has the same name and
//! does the same thing, with one difference. A `SqliteRuntime` drives its runs
//! in the caller's process. On Postgres, a [`Worker`](autumn_harvest::worker::Worker)
//! drives them. A function here records a start or a signal, and returns. A
//! worker then claims the work.
//!
//! So `sqlite::drive` has no copy here. Use [`wait_for`] to wait for
//! a durable agent, and [`wait_run`] to wait for one agent loop run.
//!
//! [`PostgresAgents`] holds the pools, the shard router, the payload codecs
//! and the queue. Build it with the same values as the worker, so a new run
//! lands on a shard that a worker claims.
//!
//! The crate sends no SQL of its own. Each function calls an engine function
//! of `autumn-harvest`, so each query has its Ledger profile there.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use autumn_harvest::builder::HarvestBuilder;
use autumn_harvest::context::{WorkflowContext, WorkflowHistoryPolicy};
use autumn_harvest::entity::{ENTITY_OP_SIGNAL, ENTITY_STATE_QUERY};
use autumn_harvest::entity_state::ProjectedSnapshot;
use autumn_harvest::error::{HarvestError, HarvestResult};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::execution::{
    SignalWithStartOutcome, SignalWithStartParams, StartWorkflowParams, latest_with_search_attrs,
    list_running_with_search_attrs, load_execution, resolve_execution_id_by_workflow_id,
    signal_with_start_workflow_execution_with_metrics_and_codecs,
    start_or_load_workflow_execution_with_codecs,
};
use autumn_harvest::executor::{QueryReplayOutcome, drive_query_replay_async};
use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::shard::{ShardRouter, ShardedDbPool};
use autumn_harvest::signal::{
    ByIdSignalOutcome, resolve_and_signal_by_workflow_id_with_codecs,
    send_signal_idempotent_with_codecs,
};
use autumn_harvest::types::StartProvenance;
use autumn_harvest::worker::DbPool;
use autumn_harvest::{ExecutionId, ShardId, WorkflowIdReusePolicy, WorkflowResultState};
use serde_json::Value;

use crate::agent::{AGENT_WORKFLOW_NAME, Agent, AgentId, AgentOp, AgentState, Control, agent_info};
use crate::agent::{INTERRUPT_SIGNAL, Interrupt};
use crate::approval::{Approval, approval_call_id};
use crate::consolidate::{CONSOLIDATE_WORKFLOW_NAME, CONSOLIDATION_ATTR, ConsolidateTask};
use crate::consolidate_all::{
    AgentDirectory, CONSOLIDATE_ALL_WORKFLOW_NAME, ConsolidateAll, LastPass,
};
use crate::error::{AgentError, ErrorKind};
use crate::harness::AgentHarness;
use crate::heartbeat::{HEARTBEAT_WORKFLOW_NAME, HeartbeatTask};
use crate::mesh_control::{MESH_ATTR, MeshPause, MeshSpend, Spend};
use crate::model::BoxFuture;
use crate::types::AgentTask;
use crate::workflow::{WORKFLOW_NAME, activities, workflows};
use autumn_harvest::chrono::{DateTime, Utc};

/// The queue of a new run when the caller names none.
pub const DEFAULT_QUEUE: &str = "default";

/// The prefix of the deadline timer of each signal wait.
///
/// The engine names the timer `__signal_timeout:<seq>:<signal name>`.
const SIGNAL_TIMEOUT_PREFIX: &str = "__signal_timeout:";

/// How often [`wait_for`] and [`wait_run`] read the database again.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// The page size of a search attribute scan, as in [`pause_mesh`].
const SCAN_PAGE: u32 = 500;

/// The Postgres target of the agent functions.
///
/// It holds one pool for each physical database, the shard router, the
/// payload codecs and the queue of a new run. Clones share the pools.
#[derive(Clone)]
pub struct PostgresAgents {
    /// One pool per physical database, with the shards that it serves.
    groups: Vec<(DbPool, Vec<ShardId>)>,
    /// The pool of a shard that no group names.
    fallback: DbPool,
    /// The sharded pool, for a target that `sharded` built. A lookup by
    /// execution id follows its forwarding chain, because a rebalance moves
    /// a run and keeps its id.
    sharded: Option<ShardedDbPool>,
    router: ShardRouter,
    codecs: PayloadCodecs,
    /// The payload offloader of the workers, if any (issue #2378).
    offloader: Option<std::sync::Arc<autumn_harvest::payload_store::PayloadOffloader>>,
    queue: String,
}

impl std::fmt::Debug for PostgresAgents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let shards: Vec<_> = self
            .groups
            .iter()
            .map(|(_pool, shards)| shards.clone())
            .collect();
        f.debug_struct("PostgresAgents")
            .field("shards", &shards)
            .field("router", &self.router)
            .field("queue", &self.queue)
            .finish_non_exhaustive()
    }
}

impl PostgresAgents {
    /// Target one database. Each run lands on shard 0.
    ///
    /// This is the shape of a deployment with no sharding.
    #[must_use]
    pub fn single(pool: DbPool) -> Self {
        let shard = ShardId::new(0);
        Self {
            groups: vec![(pool.clone(), vec![shard])],
            fallback: pool,
            sharded: None,
            router: ShardRouter::single(),
            codecs: PayloadCodecs::default(),
            offloader: None,
            queue: DEFAULT_QUEUE.to_owned(),
        }
    }

    /// Target a sharded fleet.
    ///
    /// Pass the pools and the router of the workers. A lookup by agent id
    /// reads each physical database once. A new agent lands on the shard
    /// that `router` picks for its id.
    #[must_use]
    pub fn sharded(pools: &ShardedDbPool, router: ShardRouter) -> Self {
        let groups = pools
            .pool_groups()
            .into_iter()
            .map(|(pool, shards)| (pool.clone(), shards))
            .collect();
        Self {
            groups,
            fallback: pools.pool_for(pools.default_shard()).clone(),
            sharded: Some(pools.clone()),
            router,
            codecs: PayloadCodecs::default(),
            offloader: None,
            queue: DEFAULT_QUEUE.to_owned(),
        }
    }

    /// Encode and decode payloads with `codecs`. Pass the codecs of the
    /// workers, or a worker cannot read what a send stores.
    #[must_use]
    pub fn with_codecs(mut self, codecs: PayloadCodecs) -> Self {
        self.codecs = codecs;
        self
    }

    /// Read and write offloaded payloads with `offloader` (issue #2378). Pass
    /// the offloader of the workers. A live probe then reads a source whose
    /// input the store holds, and offloads a large probe input.
    #[must_use]
    pub fn with_payload_offloader(
        mut self,
        offloader: std::sync::Arc<autumn_harvest::payload_store::PayloadOffloader>,
    ) -> Self {
        self.offloader = Some(offloader);
        self
    }

    /// Start new runs on `queue`. The default is [`DEFAULT_QUEUE`].
    #[must_use]
    pub fn with_queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = queue.into();
        self
    }

    /// The queue of a new run.
    #[must_use]
    pub fn queue(&self) -> &str {
        &self.queue
    }

    /// The pool that serves `shard`. An unknown shard gets the pool of the
    /// default shard.
    fn pool_for(&self, shard: ShardId) -> &DbPool {
        self.groups
            .iter()
            .find(|(_pool, shards)| shards.contains(&shard))
            .map_or(&self.fallback, |(pool, _shards)| pool)
    }

    /// The first shard of each physical database.
    fn group_shards(&self) -> impl Iterator<Item = ShardId> + '_ {
        self.groups
            .iter()
            .filter_map(|(_pool, shards)| shards.first().copied())
    }

    /// A connection to the database of `shard`.
    async fn conn(&self, shard: ShardId) -> HarvestResult<autumn_harvest::pool::PooledConn> {
        autumn_harvest::pool::acquire_within_pool_bound(self.pool_for(shard)).await
    }

    /// The shard that holds `exec` now.
    ///
    /// A rebalance moves a run to another database and keeps its id. The
    /// source row becomes a forwarding seal. On a sharded target, the lookup
    /// follows that chain. One database holds every run of a `single` target.
    async fn shard_of(&self, exec: ExecutionId) -> HarvestResult<ShardId> {
        match &self.sharded {
            Some(pools) => {
                autumn_harvest::shard_rebalance::resolve_execution_shard(pools, exec).await
            }
            None => Ok(self.router.shard_for_execution(exec)),
        }
    }
}

/// Register the agent workflows, their activities and `harness` on a worker
/// builder.
///
/// It is the Postgres form of `sqlite::register`. The activities read
/// the harness with `ActivityContext::state`, so a worker with no harness
/// fails each call as `AgentHarnessMissing`.
#[must_use]
pub fn register(builder: HarvestBuilder, harness: AgentHarness) -> HarvestBuilder {
    builder
        .workflows(workflows())
        .activities(activities())
        .state(harness)
}

/// Start one agent run. Returns its execution id.
///
/// The workflow id is the execution id, so each start is a new run.
///
/// # Errors
///
/// Returns an error when the engine cannot record the start.
pub async fn start(pg: &PostgresAgents, task: &AgentTask) -> HarvestResult<ExecutionId> {
    start_named(pg, WORKFLOW_NAME, serde_json::to_value(task)?).await
}

/// Start one heartbeat tick. Returns its execution id.
///
/// To let the engine start the ticks, register the schedule that
/// `heartbeat::schedule` builds instead.
///
/// # Errors
///
/// Returns an error when the engine cannot record the start.
pub async fn start_heartbeat(
    pg: &PostgresAgents,
    task: &HeartbeatTask,
) -> HarvestResult<ExecutionId> {
    start_named(pg, HEARTBEAT_WORKFLOW_NAME, serde_json::to_value(task)?).await
}

/// Start one consolidation pass of the agent `id` (issue #2454). Returns
/// its execution id. The Postgres form of `sqlite::consolidate`.
///
/// # Errors
///
/// Returns an error when the start cannot be recorded.
pub async fn consolidate(pg: &PostgresAgents, id: &AgentId) -> HarvestResult<ExecutionId> {
    let task = ConsolidateTask::new(id.clone());
    start_named(pg, CONSOLIDATE_WORKFLOW_NAME, serde_json::to_value(task)?).await
}

/// Start one firing of the dispatcher `agent_consolidate_all` (issue
/// #2540). Returns its execution id. The Postgres form of
/// `sqlite::consolidate_all`. A deployment registers
/// `consolidate::schedule` instead.
///
/// # Errors
///
/// Returns an error when the start cannot be recorded.
pub async fn consolidate_all(
    pg: &PostgresAgents,
    task: &ConsolidateAll,
) -> HarvestResult<ExecutionId> {
    start_named(
        pg,
        CONSOLIDATE_ALL_WORKFLOW_NAME,
        serde_json::to_value(task)?,
    )
    .await
}

/// Start the workflow `name` with `input`, under a fresh workflow id.
async fn start_named(pg: &PostgresAgents, name: &str, input: Value) -> HarvestResult<ExecutionId> {
    let probe = ExecutionId::new_for_shard(ShardId::new(0));
    let workflow_id = probe.to_string();
    let shard = pg.router.pick_for_new_workflow(name, &workflow_id);
    let exec = ExecutionId::new_for_shard(shard);
    let mut conn = pg.conn(shard).await?;
    let params = StartWorkflowParams::new(name, &workflow_id, exec, input, &pg.queue);
    let started =
        start_or_load_workflow_execution_with_codecs(&mut conn, params, None, &pg.codecs).await?;
    Ok(started.exec_id)
}

/// Send a reviewer decision to the wait that `signal` names.
///
/// Read `signal` from [`pending_approvals`] or [`RunState::WaitingSignal`]. A
/// decision for a wait that already timed out stays unread. It never
/// releases a later call.
///
/// # Errors
///
/// Returns an error when the run does not exist or has ended, or when the
/// engine cannot record the signal.
pub async fn decide(
    pg: &PostgresAgents,
    exec: ExecutionId,
    signal: &str,
    approval: &Approval,
) -> HarvestResult<()> {
    let mut conn = pg.conn(pg.shard_of(exec).await?).await?;
    let payload = serde_json::to_value(approval)?;
    send_signal_idempotent_with_codecs(&mut conn, exec, signal, payload, None, &pg.codecs).await?;
    Ok(())
}

/// Send `op` to the durable agent `id`. Start the agent when it has no run.
///
/// A worker then runs the op. Pass the message id as `idempotency_key`, so a
/// client retry delivers the op once.
///
/// # Errors
///
/// Returns an error when the engine cannot record the signal.
pub async fn send(
    pg: &PostgresAgents,
    id: &AgentId,
    op: &AgentOp,
    idempotency_key: Option<&str>,
) -> HarvestResult<SignalWithStartOutcome> {
    // A steer or an interrupt goes first, to the run that is in flight. With
    // no run, nothing is in flight, and the op alone is enough.
    if let Some((name, payload)) = Agent::urgency_signal(op) {
        let key = idempotency_key.map(|key| format!("{name}:{key}"));
        signal_live(pg, id, name, payload, key.as_deref()).await?;
    }
    signal(pg, id, Agent::payload(op), idempotency_key).await
}

/// Delete the durable agent `id`. Its run ends once its inbox is empty.
///
/// # Errors
///
/// Returns an error when the engine cannot record the signal.
pub async fn delete(
    pg: &PostgresAgents,
    id: &AgentId,
    idempotency_key: Option<&str>,
) -> HarvestResult<SignalWithStartOutcome> {
    signal(pg, id, Agent::delete_payload(), idempotency_key).await
}

/// Stage one entity message for the agent `id`.
///
/// The signal-with-start goes to the database of the live run when there is
/// one. Otherwise it goes to the shard that the router picks for the id. The
/// pick is a hash of the id, so two first sends race on one shard. The
/// unique index of live runs then lets only one of them start a run.
async fn signal(
    pg: &PostgresAgents,
    id: &AgentId,
    payload: Value,
    idempotency_key: Option<&str>,
) -> HarvestResult<SignalWithStartOutcome> {
    let shard = live_run(pg, id).await?.map_or_else(
        || {
            pg.router
                .pick_for_new_workflow(AGENT_WORKFLOW_NAME, id.as_str())
        },
        |(shard, _exec)| shard,
    );
    let mut conn = pg.conn(shard).await?;
    let params = SignalWithStartParams {
        workflow_name: AGENT_WORKFLOW_NAME,
        workflow_id: id.as_str(),
        exec_id: ExecutionId::new_for_shard(shard),
        // The agent starts from an empty checkpoint, as on SQLite.
        input: serde_json::json!({}),
        parent_id: None,
        queue_name: &pg.queue,
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        concurrency_key: None,
        concurrency_limit: None,
        concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
        signal_name: ENTITY_OP_SIGNAL,
        signal_payload: payload,
        idempotency_key: idempotency_key.map(str::to_owned),
        max_workflow_input_bytes: 0,
        max_signal_payload_bytes: 0,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        sla: None,
        reject_fresh_if_debounced: false,
        workflow_retry_policy: None,
        max_workflow_attempts_ceiling: None,
        workflow_info: None,
        start_source_override: None,
        start_source_ref_override: None,
        tenant: None,
    };
    signal_with_start_workflow_execution_with_metrics_and_codecs(
        &mut conn, params, None, None, &pg.codecs,
    )
    .await
}

/// The running execution of the agent `id`, if it has one.
///
/// It reads each physical database once. When two databases each hold a live
/// run, the later start wins.
///
/// # Errors
///
/// Returns an error when a read fails.
pub async fn current_run(pg: &PostgresAgents, id: &AgentId) -> HarvestResult<Option<ExecutionId>> {
    Ok(live_run(pg, id).await?.map(|(_shard, exec)| exec))
}

/// The live run of the agent `id`, with a shard of the database that holds
/// it.
///
/// A rebalance leaves a `MIGRATED` seal on the source and a `MIGRATING` copy
/// on the target until the cutover. Neither one is the live run. The engine
/// does not count them as terminal, so the read skips them by name.
async fn live_run(
    pg: &PostgresAgents,
    id: &AgentId,
) -> HarvestResult<Option<(ShardId, ExecutionId)>> {
    let mut best: Option<(DateTime<Utc>, ShardId, ExecutionId)> = None;
    for shard in pg.group_shards() {
        let mut conn = pg.conn(shard).await?;
        let found =
            resolve_execution_id_by_workflow_id(&mut conn, AGENT_WORKFLOW_NAME, id.as_str())
                .await?;
        let Some(run) = found else { continue };
        if is_migration_copy(&run.state)
            || WorkflowResultState::from_execution_state(&run.state).is_terminal()
        {
            continue;
        }
        if best.is_none_or(|(at, _shard, _exec)| run.started_at > at) {
            best = Some((run.started_at, shard, run.exec_id));
        }
    }
    Ok(best.map(|(_at, shard, exec)| (shard, exec)))
}

/// Whether `state` names a row that a rebalance left behind or staged.
fn is_migration_copy(state: &str) -> bool {
    matches!(state, "MIGRATED" | "MIGRATING")
}

/// The committed state of the agent `id`.
///
/// It reads the entity projection first. A projection row can hold no
/// readable state: the payload store holds it, or it was past the
/// side-effect cap. The read then replays the live run, as the query route
/// does. Returns `None` before the first op commits, after a delete, and
/// after an erasure.
///
/// # Errors
///
/// Returns an error when the read fails or the state does not decode.
pub async fn state(pg: &PostgresAgents, id: &AgentId) -> HarvestResult<Option<AgentState>> {
    let mut latest: Option<autumn_harvest::entity_state::EntityStateRow> = None;
    for shard in pg.group_shards() {
        let mut conn = pg.conn(shard).await?;
        let row = autumn_harvest::entity_state::read(
            &mut conn,
            &pg.codecs,
            AGENT_WORKFLOW_NAME,
            id.as_str(),
        )
        .await?;
        if let Some(row) = row
            && latest
                .as_ref()
                .is_none_or(|best| row.updated_at > best.updated_at)
        {
            latest = Some(row);
        }
    }
    let Some(row) = latest.filter(|row| !row.deleted) else {
        return Ok(None);
    };
    let answer = match &row.snapshot {
        ProjectedSnapshot::Erased => return Ok(None),
        ProjectedSnapshot::Offloaded | ProjectedSnapshot::Omitted => {
            let Some((shard, exec)) = live_run(pg, id).await? else {
                return Ok(None);
            };
            replay_state(pg, shard, exec).await?
        }
        snapshot => snapshot.query_answer(ENTITY_STATE_QUERY),
    };
    answer
        .map(serde_json::from_value)
        .transpose()
        .map_err(Into::into)
}

/// The longest replay that [`replay_state`] drives.
const REPLAY_TIMEOUT: Duration = Duration::from_secs(5);

/// Replay the run `exec` of an agent on `shard`, and answer the state query.
///
/// The context carries the facts of the run row, as the query route sets
/// them, so the replay takes the same path as the run. Returns `None` when
/// the run has ended.
async fn replay_state(
    pg: &PostgresAgents,
    shard: ShardId,
    exec: ExecutionId,
) -> HarvestResult<Option<Value>> {
    let mut conn = pg.conn(shard).await?;
    let run = load_execution(&mut conn, exec).await?;
    if WorkflowResultState::from_execution_state(&run.state).is_terminal() {
        return Ok(None);
    }
    let input = pg.codecs.decode_column(&run.input)?;
    let history =
        autumn_harvest::store::load_history_with_codecs(&mut conn, exec, &pg.codecs).await?;
    drop(conn);
    let ctx = WorkflowContext::for_replay_with_state_and_history_policy(
        exec,
        history.events,
        autumn_harvest::context::empty_shared_state(),
        WorkflowHistoryPolicy::default(),
    )
    .with_execution_timeout(run.execution_timeout)
    .with_deadline(run.deadline_at)
    .with_current_shard_id(Some(ShardId::new(run.shard_id)))
    .with_parent_execution_id(run.parent_id.map(ExecutionId::from_uuid))
    .with_start_provenance(StartProvenance::from_row(
        run.start_source.as_deref(),
        run.continued_from_exec_id,
    ))
    .with_workflow_name(run.workflow_name.clone())
    .with_workflow_id(run.workflow_id.clone());
    let outcome = drive_query_replay_async(&ctx, agent_info().handler, input, REPLAY_TIMEOUT).await;
    match outcome {
        QueryReplayOutcome::Panicked => Err(HarvestError::QueryHandlerPanicked(format!(
            "the agent workflow panicked in the replay of {exec}"
        ))),
        QueryReplayOutcome::TimedOut => Err(HarvestError::QueryTimedOut {
            query_name: ENTITY_STATE_QUERY.to_owned(),
            timeout_ms: u64::try_from(REPLAY_TIMEOUT.as_millis()).unwrap_or(u64::MAX),
        }),
        QueryReplayOutcome::ReachedTerminal | QueryReplayOutcome::Suspended => {
            ctx.execute_query(ENTITY_STATE_QUERY).map(Some)
        }
    }
}

/// Wait until the state of the agent `id` passes `until`, up to `limit`.
///
/// A worker runs the ops, so the caller waits on the result. Returns the
/// state that passed, or `None` when `limit` passed first.
///
/// # Errors
///
/// Returns an error when a read fails.
pub async fn wait_for(
    pg: &PostgresAgents,
    id: &AgentId,
    limit: Duration,
    until: impl Fn(&AgentState) -> bool,
) -> HarvestResult<Option<AgentState>> {
    let deadline = Instant::now() + limit;
    loop {
        if let Some(state) = state(pg, id).await?
            && until(&state)
        {
            return Ok(Some(state));
        }
        if Instant::now() >= deadline {
            return Ok(None);
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Where one run stands. The Postgres form of `autumn_harvest_sqlite::RunState`.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum RunState {
    /// The run completed with this output.
    Completed(Value),
    /// The run failed with this error.
    Failed(String),
    /// The run waits for the approval signal with this name.
    WaitingSignal(String),
    /// The run ended in another terminal state, such as `CANCELLED`.
    Ended {
        /// The stored state.
        state: String,
        /// The stored error, if any.
        error: Option<String>,
    },
    /// The run is still running.
    Running,
}

/// Where the run `exec` stands now.
///
/// A running run with an open approval wait reads as
/// [`RunState::WaitingSignal`], with the first open wait.
///
/// # Errors
///
/// Returns an error when the run does not exist or a read fails.
pub async fn run_state(pg: &PostgresAgents, exec: ExecutionId) -> HarvestResult<RunState> {
    let mut conn = pg.conn(pg.shard_of(exec).await?).await?;
    let run = load_execution(&mut conn, exec).await?;
    let state = match run.state.as_str() {
        "COMPLETED" => {
            let output = pg.codecs.decode_column_opt(run.output.as_ref())?;
            RunState::Completed(output.unwrap_or(Value::Null))
        }
        "FAILED" => RunState::Failed(run.error.unwrap_or_default()),
        other if WorkflowResultState::from_execution_state(other).is_terminal() => {
            RunState::Ended {
                state: run.state,
                error: run.error,
            }
        }
        _ => {
            drop(conn);
            pending_approvals(pg, exec)
                .await?
                .into_iter()
                .next()
                .map_or(RunState::Running, RunState::WaitingSignal)
        }
    };
    Ok(state)
}

/// Wait until the run `exec` ends or waits for an approval, up to `limit`.
///
/// Returns [`RunState::Running`] when `limit` passes first.
///
/// # Errors
///
/// As [`run_state`].
pub async fn wait_run(
    pg: &PostgresAgents,
    exec: ExecutionId,
    limit: Duration,
) -> HarvestResult<RunState> {
    let deadline = Instant::now() + limit;
    loop {
        let state = run_state(pg, exec).await?;
        if state != RunState::Running || Instant::now() >= deadline {
            return Ok(state);
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// The open approval signals of the run `exec`, oldest first.
///
/// Each approval wait has a deadline timer, named for its signal. A wait is
/// open while its timer has not fired, has not been cancelled, and its signal
/// has not arrived. A decision that is queued but not yet read also closes
/// the wait, because the run needs no other input. The read covers this run
/// only, not its child runs.
///
/// The queued signals are read first, and the history second. A worker
/// consumes a signal and appends its `SignalReceived` in one transaction. So
/// each decision is in one read or the other, even when a worker consumes it
/// between the reads.
///
/// # Errors
///
/// Returns an error when a read fails.
pub async fn pending_approvals(
    pg: &PostgresAgents,
    exec: ExecutionId,
) -> HarvestResult<Vec<String>> {
    let mut conn = pg.conn(pg.shard_of(exec).await?).await?;
    let queued = autumn_harvest::signal::load_pending_signals(&mut conn, exec).await?;
    let history =
        autumn_harvest::store::load_history_with_codecs(&mut conn, exec, &pg.codecs).await?;
    let mut open = open_approval_waits(&history.events);
    open.retain(|signal| !queued.iter().any(|row| &row.signal_name == signal));
    Ok(open)
}

/// The open approval waits of `events`, in the order that they started.
fn open_approval_waits(events: &[WorkflowEvent]) -> Vec<String> {
    let mut open: Vec<(String, String)> = Vec::new();
    for event in events {
        match event {
            WorkflowEvent::TimerStarted { timer_id, .. } => {
                if let Some(signal) = approval_wait(timer_id.as_str()) {
                    open.push((timer_id.as_str().to_owned(), signal.to_owned()));
                }
            }
            WorkflowEvent::TimerFired { timer_id } | WorkflowEvent::TimerCancelled { timer_id } => {
                open.retain(|(timer, _signal)| timer != timer_id.as_str());
            }
            WorkflowEvent::SignalReceived { signal_name, .. } => {
                open.retain(|(_timer, signal)| signal != signal_name);
            }
            _ => {}
        }
    }
    let mut seen = BTreeSet::new();
    open.into_iter()
        .map(|(_timer, signal)| signal)
        .filter(|signal| seen.insert(signal.clone()))
        .collect()
}

/// The approval signal that a deadline timer bounds, if it bounds one.
fn approval_wait(timer_id: &str) -> Option<&str> {
    let rest = timer_id.strip_prefix(SIGNAL_TIMEOUT_PREFIX)?;
    let (seq, signal) = rest.split_once(':')?;
    seq.parse::<u64>().ok()?;
    approval_call_id(signal).map(|_call| signal)
}

/// The spend of a mesh from the cost ledger (issue #2272).
///
/// It sums the ledger rows of every run whose search attribute [`MESH_ATTR`]
/// names the mesh, child runs included. It reads each physical database once.
/// The Postgres form of `sqlite::LedgerSpend`.
#[derive(Debug, Clone)]
pub struct LedgerSpend {
    pg: PostgresAgents,
}

impl LedgerSpend {
    /// Read the ledger of the databases of `pg`.
    #[must_use]
    pub const fn new(pg: PostgresAgents) -> Self {
        Self { pg }
    }

    async fn read(&self, mesh: &str, since: DateTime<Utc>) -> HarvestResult<Spend> {
        let attrs = serde_json::json!({ MESH_ATTR: mesh });
        let mut spend = Spend::default();
        for shard in self.pg.group_shards() {
            let mut conn = self.pg.conn(shard).await?;
            let total =
                autumn_harvest::llm_ledger::spend_for_search_attrs(&mut conn, &attrs, since)
                    .await?;
            spend = Spend {
                tokens: spend.tokens.saturating_add(total.tokens),
                cost_usd_micros: spend.cost_usd_micros.saturating_add(total.cost_usd_micros),
            };
        }
        Ok(spend)
    }
}

impl MeshSpend for LedgerSpend {
    fn spent<'a>(
        &'a self,
        mesh: &'a str,
        since: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<Spend, AgentError>> {
        Box::pin(async move {
            self.read(mesh, since)
                .await
                .map_err(|err| AgentError::new(ErrorKind::Transport, err.to_string()))
        })
    }
}

/// The agent directory of the databases of a target (issue #2540).
///
/// It lists the running agents of a kind and the running passes of an
/// agent by their search attributes. It reads each physical database once.
/// The Postgres form of `sqlite::Directory`. Install it with
/// `AgentHarness::directory`.
#[derive(Debug, Clone)]
pub struct Directory {
    pg: PostgresAgents,
}

impl Directory {
    /// Read the runs of the databases of `pg`.
    #[must_use]
    pub const fn new(pg: PostgresAgents) -> Self {
        Self { pg }
    }

    /// The workflow ids of the running runs of `workflow_name` that carry
    /// `attrs`, on each database.
    async fn running(&self, workflow_name: &str, attrs: &Value) -> HarvestResult<Vec<String>> {
        let mut ids = Vec::new();
        for shard in self.pg.group_shards() {
            let mut conn = self.pg.conn(shard).await?;
            let mut after = None;
            loop {
                let page = list_running_with_search_attrs(
                    &mut conn,
                    workflow_name,
                    attrs,
                    after,
                    SCAN_PAGE,
                )
                .await?;
                let full = page.len() >= SCAN_PAGE as usize;
                after = page.last().map(|(exec, _id)| *exec);
                ids.extend(page.into_iter().map(|(_exec, id)| id));
                if !full {
                    break;
                }
            }
        }
        Ok(ids)
    }

    /// The last run of `workflow_name` that carries `attrs`, over each
    /// database. A running run comes first. Each database returns one row.
    async fn last_run(
        &self,
        workflow_name: &str,
        attrs: &Value,
    ) -> HarvestResult<Option<LastPass>> {
        let mut last: Option<LastPass> = None;
        for shard in self.pg.group_shards() {
            let mut conn = self.pg.conn(shard).await?;
            let Some((state, started_at)) =
                latest_with_search_attrs(&mut conn, workflow_name, attrs).await?
            else {
                continue;
            };
            let found = LastPass {
                running: state == "RUNNING",
                started_ms: started_at.timestamp_millis(),
            };
            last = Some(match last {
                Some(seen)
                    if (seen.running, seen.started_ms) >= (found.running, found.started_ms) =>
                {
                    seen
                }
                _ => found,
            });
        }
        Ok(last)
    }
}

impl AgentDirectory for Directory {
    fn agents<'a>(&'a self, kind: &'a str) -> BoxFuture<'a, Result<Vec<AgentId>, AgentError>> {
        Box::pin(async move {
            let attrs = serde_json::json!({ "agent_kind": kind });
            let ids = self
                .running(AGENT_WORKFLOW_NAME, &attrs)
                .await
                .map_err(|err| AgentError::new(ErrorKind::Transport, err.to_string()))?;
            let mut agents: Vec<AgentId> = ids
                .iter()
                .filter_map(|id| AgentId::parse(id).ok())
                .collect();
            agents.sort();
            agents.dedup();
            Ok(agents)
        })
    }

    fn last_pass<'a>(
        &'a self,
        agent: &'a AgentId,
    ) -> BoxFuture<'a, Result<Option<LastPass>, AgentError>> {
        Box::pin(async move {
            let attrs = serde_json::json!({ CONSOLIDATION_ATTR: agent.as_str() });
            self.last_run(CONSOLIDATE_WORKFLOW_NAME, &attrs)
                .await
                .map_err(|err| AgentError::new(ErrorKind::Transport, err.to_string()))
        })
    }
}

/// Pause every running agent of `mesh`, and return the audit record (issue
/// #2272).
///
/// Each agent gets an interrupt, so an op in flight stops, and then a
/// `Control::Pause` with `reason`. A paused agent holds its work and skips
/// its ticks until a `Control::Resume`. The record also goes to the tracing
/// target `harvest.audit`.
///
/// # Errors
///
/// Returns a list or a send error. Agents that got the pause before it stay
/// paused.
pub async fn pause_mesh(pg: &PostgresAgents, mesh: &str, reason: &str) -> HarvestResult<MeshPause> {
    let at_ms = Utc::now().timestamp_millis();
    let attrs = serde_json::json!({ MESH_ATTR: mesh });
    let mut targets = Vec::new();
    for shard in pg.group_shards() {
        let mut conn = pg.conn(shard).await?;
        let mut after = None;
        loop {
            let page = list_running_with_search_attrs(
                &mut conn,
                AGENT_WORKFLOW_NAME,
                &attrs,
                after,
                SCAN_PAGE,
            )
            .await?;
            let full = page.len() >= SCAN_PAGE as usize;
            after = page.last().map(|(exec, _id)| *exec);
            targets.extend(
                page.into_iter()
                    .filter_map(|(_exec, id)| AgentId::parse(&id).ok()),
            );
            if !full {
                break;
            }
        }
    }
    targets.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    targets.dedup();
    let key = format!("pause-mesh:{mesh}:{at_ms}");
    let pause = AgentOp::Control(Control::Pause {
        reason: Some(reason.to_owned()),
    });
    let mut agents = Vec::with_capacity(targets.len());
    for id in targets {
        interrupt(pg, &id, &key).await?;
        send(pg, &id, &pause, Some(&key)).await?;
        agents.push(id);
    }
    let record = MeshPause {
        mesh: mesh.to_owned(),
        reason: reason.to_owned(),
        at_ms,
        agents,
    };
    tracing::info!(
        target: "harvest.audit",
        mesh = %record.mesh,
        reason = %record.reason,
        agents = record.agents.len(),
        "a mesh was paused"
    );
    Ok(record)
}

/// Send an interrupt to the live run of `id`. A run that ended takes none.
async fn interrupt(pg: &PostgresAgents, id: &AgentId, key: &str) -> HarvestResult<()> {
    let payload = serde_json::to_value(Interrupt { message_id: None })?;
    let signal_key = format!("{INTERRUPT_SIGNAL}:{key}");
    signal_live(pg, id, INTERRUPT_SIGNAL, payload, Some(&signal_key)).await
}

/// How many times [`signal_live`] resolves the live run again.
const SUCCESSOR_ATTEMPTS: u32 = 8;

/// Send the signal `name` to the live run of `id`, if it has one.
///
/// A durable agent continues as new at each checkpoint. A send can resolve
/// the run just before it seals. The engine then reports the race, and the
/// send resolves the successor and tries again. With no live run, nothing is
/// in flight, so the send does nothing.
async fn signal_live(
    pg: &PostgresAgents,
    id: &AgentId,
    name: &str,
    payload: Value,
    idempotency_key: Option<&str>,
) -> HarvestResult<()> {
    for _attempt in 0..SUCCESSOR_ATTEMPTS {
        let Some((shard, _exec)) = live_run(pg, id).await? else {
            return Ok(());
        };
        let mut conn = pg.conn(shard).await?;
        let outcome = resolve_and_signal_by_workflow_id_with_codecs(
            &mut conn,
            AGENT_WORKFLOW_NAME,
            id.as_str(),
            name,
            payload.clone(),
            idempotency_key,
            &pg.codecs,
        )
        .await?;
        match outcome {
            ByIdSignalOutcome::Delivered
            | ByIdSignalOutcome::NoRunFound
            | ByIdSignalOutcome::NotRunning => return Ok(()),
            ByIdSignalOutcome::RacedToSuccessor => {}
        }
    }
    Err(HarvestError::Config(format!(
        "the signal {name} to {id} raced {SUCCESSOR_ATTEMPTS} continue-as-new seals"
    )))
}

/// A live probe forks on Postgres with quiet live effects (issue #2378).
///
/// The read and the fork use the codecs and the offloader of the target. The fork commits in
/// one transaction and only inserts rows, so the source stays unchanged.
#[cfg(feature = "eval")]
impl crate::eval::LiveProbeHost for PostgresAgents {
    fn source_run(
        &self,
        source: ExecutionId,
    ) -> BoxFuture<'_, Result<crate::eval::SourceRun, crate::eval::EvalError>> {
        Box::pin(async move {
            let host = |err: HarvestError| crate::eval::EvalError::Fork(err.to_string());
            let mut conn = self
                .conn(self.shard_of(source).await.map_err(host)?)
                .await
                .map_err(host)?;
            let run = load_execution(&mut conn, source).await.map_err(host)?;
            let history = autumn_harvest::store::load_history_inflated(
                &mut conn,
                source,
                &self.codecs,
                self.offloader.as_deref(),
            )
            .await
            .map_err(host)?;
            Ok(crate::eval::SourceRun {
                workflow_name: run.workflow_name,
                events: history.events,
            })
        })
    }

    fn fork_live(
        &self,
        source: ExecutionId,
        input: Value,
    ) -> BoxFuture<'_, Result<crate::eval::ProbeFork, crate::eval::EvalError>> {
        use autumn_harvest::fork::{ForkEffects, WorkflowForkError, WorkflowForkRequest};
        Box::pin(async move {
            let host = |err: HarvestError| crate::eval::EvalError::Fork(err.to_string());
            let mut conn = self
                .conn(self.shard_of(source).await.map_err(host)?)
                .await
                .map_err(host)?;
            let request = WorkflowForkRequest {
                reason: crate::eval::PROBE_FORK_REASON.to_owned(),
                operator_id: crate::eval::PROBE_FORK_OPERATOR.to_owned(),
                effects: ForkEffects::LiveQuiet,
                input: Some(input),
                ..WorkflowForkRequest::default()
            };
            let fork = autumn_harvest::fork::fork_workflow_execution_with_codecs(
                &mut conn,
                source,
                request,
                &self.codecs,
                self.offloader.as_deref(),
            )
            .await
            .map_err(|err| match err {
                WorkflowForkError::ErasedSource { .. } => crate::eval::EvalError::ErasedSource,
                other => crate::eval::EvalError::Fork(other.to_string()),
            })?;
            Ok(crate::eval::ProbeFork {
                exec_id: fork.new_exec_id,
                workflow_id: fork.workflow_id,
                forked_from_exec_id: fork.forked_from_exec_id,
            })
        })
    }
}

/// How long a live re-probe of a refine run waits for its fork (issue
/// #2380). The probe activity has a 2-hour budget, so the replay probe and
/// the wait both fit in one attempt.
pub const LIVE_REPROBE_WAIT: Duration = Duration::from_mins(60);

/// How often a live re-probe reads the state of its fork.
#[cfg(feature = "eval")]
const LIVE_REPROBE_POLL: Duration = Duration::from_secs(1);

/// The source runs of a refine run, from Postgres (issue #2380).
///
/// The read uses the codecs and the offloader of the target. With the
/// `eval` feature, a live re-probe forks the source with
/// `eval::live_probe` and waits up to [`LIVE_REPROBE_WAIT`] for its end. A
/// worker of the queue must run the fork. Install it with
/// `AgentHarness::refine_sources`.
#[derive(Debug, Clone)]
pub struct Sources {
    pg: PostgresAgents,
}

impl Sources {
    /// Read the runs of `pg`.
    #[must_use]
    pub const fn new(pg: PostgresAgents) -> Self {
        Self { pg }
    }

    async fn read(&self, run: ExecutionId) -> HarvestResult<Option<crate::refine::SourceRecord>> {
        let shard = match self.pg.shard_of(run).await {
            Ok(shard) => shard,
            Err(HarvestError::NotFound(_)) => return Ok(None),
            Err(err) => return Err(err),
        };
        let mut conn = self.pg.conn(shard).await?;
        let execution = match load_execution(&mut conn, run).await {
            Ok(execution) => execution,
            Err(HarvestError::NotFound(_)) => return Ok(None),
            Err(err) => return Err(err),
        };
        let history = autumn_harvest::store::load_history_inflated(
            &mut conn,
            run,
            &self.pg.codecs,
            self.pg.offloader.as_deref(),
        )
        .await?;
        Ok(Some(crate::refine::SourceRecord {
            ended: WorkflowResultState::from_execution_state(&execution.state).is_terminal(),
            workflow_name: execution.workflow_name,
            workflow_id: execution.workflow_id,
            events: history.events,
        }))
    }
}

impl Sources {
    /// Wait until the fork `exec` ends, up to [`LIVE_REPROBE_WAIT`].
    ///
    /// An approval wait of a probe times out to a deny after
    /// `PROBE_APPROVAL_TIMEOUT_SECS`, so the wait goes on through it. It
    /// returns the last state when the deadline passes first.
    #[cfg(feature = "eval")]
    async fn wait_end(&self, exec: ExecutionId) -> HarvestResult<RunState> {
        let deadline = Instant::now() + LIVE_REPROBE_WAIT;
        loop {
            let state = run_state(&self.pg, exec).await?;
            let open = matches!(state, RunState::Running | RunState::WaitingSignal(_));
            if !open || Instant::now() >= deadline {
                return Ok(state);
            }
            tokio::time::sleep(LIVE_REPROBE_POLL).await;
        }
    }

    /// Cancel the fork `exec`. A failed cancel logs a warning.
    #[cfg(feature = "eval")]
    async fn cancel(&self, exec: ExecutionId) {
        let cancelled = async {
            let mut conn = self.pg.conn(self.pg.shard_of(exec).await?).await?;
            autumn_harvest::cancel_workflow_execution(
                &mut conn,
                exec,
                "the live re-probe did not end in time",
                &autumn_harvest::telemetry::NoOpMetrics,
            )
            .await
        };
        if let Err(err) = cancelled.await {
            tracing::warn!(error = %err, exec = %exec, "a live re-probe fork was not cancelled");
        }
    }
}

impl crate::refine::RefineSources for Sources {
    fn source(
        &self,
        run: ExecutionId,
    ) -> BoxFuture<'_, Result<Option<crate::refine::SourceRecord>, AgentError>> {
        Box::pin(async move {
            self.read(run)
                .await
                .map_err(|err| AgentError::new(ErrorKind::Transport, err.to_string()))
        })
    }

    #[cfg(feature = "eval")]
    fn live_probe<'a>(
        &'a self,
        request: crate::refine::LiveRequest<'a>,
    ) -> BoxFuture<'a, Result<crate::refine::LiveEnd, crate::refine::LiveProbeError>> {
        use crate::refine::{LiveEnd, LiveProbeError};
        Box::pin(async move {
            let effects: Vec<_> = request.effects.iter().copied().collect();
            let probe = crate::eval::live_probe_with(
                &self.pg,
                request.harness,
                request.source,
                request.overlay,
                &effects,
                Some(request.ceilings),
            )
            .await
            .map_err(|err| LiveProbeError::Failed(err.to_string()))?;
            let state = match self.wait_end(probe.exec_id).await {
                Ok(state) => state,
                Err(err) => {
                    // The fork commits before the wait. A failed wait leaves
                    // no task that watches it, so stop it now.
                    self.cancel(probe.exec_id).await;
                    return Err(LiveProbeError::Failed(err.to_string()));
                }
            };
            let exec_id = probe.exec_id.to_string();
            let end = match state {
                RunState::Completed(output) => {
                    // The report holds the spend of the fork, so the refine
                    // ledger can count it.
                    let report: Option<crate::types::AgentReport> =
                        serde_json::from_value(output).ok();
                    LiveEnd {
                        exec_id,
                        completed: true,
                        error: None,
                        usage: report.as_ref().map(|r| r.usage).unwrap_or_default(),
                        cost_usd_micros: report.map_or(0, |r| r.cost_usd_micros),
                    }
                }
                RunState::Failed(error) => LiveEnd {
                    exec_id,
                    completed: false,
                    error: Some(crate::harness::truncate(&error, 500)),
                    usage: crate::message::TokenUsage::default(),
                    cost_usd_micros: 0,
                },
                RunState::Ended { state, error } => LiveEnd {
                    exec_id,
                    completed: false,
                    error: Some(crate::harness::truncate(&error.unwrap_or(state), 500)),
                    usage: crate::message::TokenUsage::default(),
                    cost_usd_micros: 0,
                },
                RunState::WaitingSignal(_) | RunState::Running => {
                    // A fork that does not end in time stops, so it spends
                    // no more.
                    self.cancel(probe.exec_id).await;
                    LiveEnd {
                        exec_id,
                        completed: false,
                        error: Some(
                            "the fork did not end in time, and the probe cancelled it".to_owned(),
                        ),
                        usage: crate::message::TokenUsage::default(),
                        cost_usd_micros: 0,
                    }
                }
            };
            Ok(end)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_harvest::types::TimerId;

    fn started(id: &str) -> WorkflowEvent {
        WorkflowEvent::TimerStarted {
            timer_id: TimerId::new(id),
            duration_secs: 60,
        }
    }

    #[test]
    fn an_approval_timer_names_its_signal() {
        assert_eq!(
            approval_wait("__signal_timeout:3:tool_approval:1:0:call-7"),
            Some("tool_approval:1:0:call-7")
        );
    }

    #[test]
    fn a_timer_of_another_wait_is_not_an_approval() {
        assert_eq!(approval_wait("__signal_timeout:3:harvest.entity.op"), None);
        assert_eq!(approval_wait("sleep-1"), None);
        assert_eq!(
            approval_wait("__signal_timeout:x:tool_approval:1:0:c"),
            None
        );
    }

    #[test]
    fn an_open_wait_is_listed_in_start_order() {
        let events = vec![
            started("__signal_timeout:4:tool_approval:1:1:b"),
            started("__signal_timeout:2:tool_approval:1:0:a"),
        ];
        assert_eq!(
            open_approval_waits(&events),
            vec!["tool_approval:1:1:b", "tool_approval:1:0:a"]
        );
    }

    #[test]
    fn a_fired_cancelled_or_answered_wait_is_closed() {
        let events = vec![
            started("__signal_timeout:1:tool_approval:1:0:a"),
            started("__signal_timeout:2:tool_approval:1:1:b"),
            started("__signal_timeout:3:tool_approval:1:2:c"),
            started("__signal_timeout:4:tool_approval:1:3:d"),
            WorkflowEvent::TimerFired {
                timer_id: TimerId::new("__signal_timeout:1:tool_approval:1:0:a"),
            },
            WorkflowEvent::TimerCancelled {
                timer_id: TimerId::new("__signal_timeout:2:tool_approval:1:1:b"),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "tool_approval:1:2:c".to_owned(),
                payload: serde_json::json!("approve"),
            },
        ];
        assert_eq!(open_approval_waits(&events), vec!["tool_approval:1:3:d"]);
    }

    #[test]
    fn an_empty_history_has_no_wait() {
        assert_eq!(open_approval_waits(&[]), Vec::<String>::new());
    }
}
