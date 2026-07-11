//! DAG scheduler and runtime execution.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use croner::Cron;
use diesel::OptionalExtension;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel::{BoolExpressionMethods, ExpressionMethods};
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use serde::Serialize;
use serde_json::Value;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::{HarvestError, HarvestResult};
use crate::execution::{
    StartWorkflowParams, StartedWorkflowExecution, start_or_load_workflow_execution,
};
use crate::info::DagInfo;
use crate::models::{HarvestSchedule, NewHarvestSchedule};
use crate::policy::{OverlapPolicy, Schedule, WorkflowSchedule, compute_jitter_offset};
use crate::schema::{harvest_schedules, harvest_workflow_executions};
use crate::shard::{ShardRouter, ShardedDbPool};
use crate::types::{ExecutionId, Priority, ShardId, WorkflowIdReusePolicy};
use crate::worker::{DbPool, HandlerRegistry};

const DEFAULT_SCHEDULER_TICK_INTERVAL: Duration = Duration::from_secs(1);

/// Default upper bound on the number of timestamps a single backfill request may plan.
///
/// Chosen to cover a 7-day hourly window (168 slots) with comfortable headroom.
pub const DEFAULT_BACKFILL_MAX_COUNT: usize = 1_000;

/// Errors returned when backfill planning cannot complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackfillPlanError {
    /// The window contains more timestamps than the caller-supplied limit.
    LimitExceeded { limit: usize },
}

impl std::fmt::Display for BackfillPlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LimitExceeded { limit } => {
                write!(f, "backfill would exceed the {limit}-timestamp limit")
            }
        }
    }
}

/// Compute the timestamps a schedule would fire between `from` (inclusive) and `to` (inclusive).
///
/// - For `Cron` schedules the first occurrence at or after `from` is included; subsequent
///   occurrences are stepped through until they exceed `to`.
/// - For `Interval` schedules `from` is treated as the first backfill timestamp and slots are
///   spaced by the interval duration.
/// - `Manual` schedules and `None` return an empty list (no automatic firing times).
///
/// # Errors
///
/// Returns `Err(BackfillPlanError::LimitExceeded)` if the number of planned timestamps
/// would exceed `max_count` before the window is fully enumerated.  Callers should pass
/// [`DEFAULT_BACKFILL_MAX_COUNT`] unless a tighter bound is required.
pub fn plan_backfill_timestamps(
    schedule: Option<&Schedule>,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    max_count: usize,
) -> Result<Vec<DateTime<Utc>>, BackfillPlanError> {
    if to < from {
        return Ok(vec![]);
    }

    match schedule {
        None | Some(Schedule::Manual) => Ok(vec![]),
        Some(Schedule::Cron(_) | Schedule::CronInTimezone { .. }) => {
            // Find the first cron occurrence at or after `from` by searching from 1 ms before
            // it (cron fires on whole-second boundaries; 1 ms is a safe undercut).
            let reference = from - chrono::Duration::milliseconds(1);
            let Some(mut cursor) = next_run_after(schedule, reference) else {
                return Ok(vec![]);
            };
            let mut timestamps = Vec::new();
            loop {
                if cursor > to {
                    break;
                }
                if timestamps.len() >= max_count {
                    return Err(BackfillPlanError::LimitExceeded { limit: max_count });
                }
                timestamps.push(cursor);
                let Some(next) = next_run_after(schedule, cursor) else {
                    break;
                };
                cursor = next;
            }
            Ok(timestamps)
        }
        Some(Schedule::Interval(interval)) => {
            let dur = chrono::Duration::from_std(*interval).unwrap_or(chrono::Duration::MAX);
            let mut timestamps = Vec::new();
            let mut cursor = from;
            loop {
                if cursor > to {
                    break;
                }
                if timestamps.len() >= max_count {
                    return Err(BackfillPlanError::LimitExceeded { limit: max_count });
                }
                timestamps.push(cursor);
                cursor += dur;
            }
            Ok(timestamps)
        }
    }
}

/// Represents a fully registered and compiled DAG definition.
#[derive(Debug, Clone)]
pub struct RegisteredDag {
    /// The name of the DAG.
    pub name: String,
    /// The Rust module path where the DAG is defined.
    pub module: String,
    /// Optional schedule for automatic execution.
    pub schedule: Option<Schedule>,
    /// Whether to run missed executions sequentially if the scheduler was down.
    pub catchup: bool,
    /// Maximum number of concurrent executions for this DAG.
    pub max_active_runs: u32,
    /// Default queue declared on the DAG, if any.
    pub default_queue: Option<String>,
    /// True when this DAG is executed through the workflow executor.
    pub is_unified: bool,
    /// The compiled task and dependency definition.
    pub definition: crate::dag::DagDefinition,
    /// Maximum spread window for schedule fires. `Duration::ZERO` disables jitter.
    pub jitter: std::time::Duration,
    /// Overlap policy for this DAG's schedule (issue #241).
    pub overlap_policy: OverlapPolicy,
    /// Maximum buffered slots under `BufferAll` (issue #241).
    pub buffer_all_max: u32,
    /// Team owner metadata (issue #372).
    pub owner: Option<String>,
    /// Linked runbook URL metadata (issue #372).
    pub runbook_url: Option<String>,
    /// Severity level metadata (issue #372).
    pub severity: Option<String>,
}

impl RegisteredDag {
    /// Returns the number of tasks in this DAG.
    #[must_use]
    pub fn task_count(&self) -> usize {
        self.definition.tasks().len()
    }
}

/// A collection of registered DAGs mapped by name.
pub type DagCatalog = HashMap<String, RegisteredDag>;

/// A point-in-time diagnostic snapshot of the scheduler's state.
#[derive(Debug, Clone, Serialize)]
pub struct SchedulerSnapshot {
    /// True if the scheduler loop is currently running.
    pub running: bool,
    /// Number of DAGs registered with the scheduler.
    pub dag_count: usize,
    /// Interval in milliseconds between scheduler ticks.
    pub tick_interval_ms: u64,
    /// UTC timestamp of the last executed tick.
    pub last_tick_at: Option<DateTime<Utc>>,
}

/// Provides diagnostic visibility into a running scheduler.
#[derive(Debug, Clone)]
pub struct SchedulerMonitor {
    inner: Arc<Mutex<SchedulerSnapshot>>,
}

impl SchedulerMonitor {
    /// Creates a new monitor initialized for the given number of DAGs.
    #[must_use]
    pub fn new(dag_count: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SchedulerSnapshot {
                running: true,
                dag_count,
                tick_interval_ms: DEFAULT_SCHEDULER_TICK_INTERVAL
                    .as_millis()
                    .try_into()
                    .unwrap_or(u64::MAX),
                last_tick_at: None,
            })),
        }
    }

    /// Creates a dummy offline monitor for contexts where the scheduler isn't running.
    #[must_use]
    pub fn offline() -> Self {
        Self {
            inner: Arc::new(Mutex::new(SchedulerSnapshot {
                running: false,
                dag_count: 0,
                tick_interval_ms: DEFAULT_SCHEDULER_TICK_INTERVAL
                    .as_millis()
                    .try_into()
                    .unwrap_or(u64::MAX),
                last_tick_at: None,
            })),
        }
    }

    /// Snapshot the current scheduler heartbeat state.
    ///
    /// # Panics
    ///
    /// Panics if the internal scheduler monitor mutex is poisoned.
    #[must_use]
    pub fn snapshot(&self) -> SchedulerSnapshot {
        self.inner
            .lock()
            .expect("scheduler monitor lock poisoned")
            .clone()
    }

    fn mark_tick(&self, dag_count: usize) {
        let mut guard = self.inner.lock().expect("scheduler monitor lock poisoned");
        guard.running = true;
        guard.dag_count = dag_count;
        guard.last_tick_at = Some(Utc::now());
    }

    fn mark_stopped(&self, dag_count: usize) {
        let mut guard = self.inner.lock().expect("scheduler monitor lock poisoned");
        guard.running = false;
        guard.dag_count = dag_count;
    }
}

/// The background runtime that drives DAG and workflow scheduling.
pub struct SchedulerRuntime {
    shutdown: CancellationToken,
    handle: JoinHandle<()>,
    monitor: SchedulerMonitor,
}

impl SchedulerRuntime {
    /// Spawns the scheduler loop on a new Tokio task.
    ///
    /// It wakes up at a fixed interval to evaluate schedules and trigger runs.
    #[must_use]
    pub fn spawn(
        pool: DbPool,
        registry: Arc<HandlerRegistry>,
        dags: Arc<DagCatalog>,
        workflow_schedules: Arc<Vec<WorkflowSchedule>>,
    ) -> Self {
        Self::spawn_sharded(
            ShardedDbPool::single(pool),
            ShardRouter::single(),
            registry,
            dags,
            workflow_schedules,
        )
    }

    /// Spawns the scheduler loop for a sharded deployment.
    ///
    /// DAG-backed workflow schedules are registered and ticked on the shard
    /// selected by [`ShardRouter::pick_for_dag`]. Workflow-only schedules remain
    /// on the router's default shard for backward compatibility.
    ///
    /// # Panics
    ///
    /// Panics when the scheduler is built without `unified-dag-execution` and
    /// the DAG catalog contains classic DAGs, because there is no supported
    /// tick path for executing those schedule rows.
    #[must_use]
    pub fn spawn_sharded(
        pool: ShardedDbPool,
        router: ShardRouter,
        registry: Arc<HandlerRegistry>,
        dags: Arc<DagCatalog>,
        workflow_schedules: Arc<Vec<WorkflowSchedule>>,
    ) -> Self {
        #[cfg(not(feature = "unified-dag-execution"))]
        {
            if let Err(error) = reject_classic_dags_without_unified_execution(dags.as_ref()) {
                panic!("{error}");
            }
        }

        let shutdown = CancellationToken::new();
        let shutdown_for_task = shutdown.clone();
        let total = dags.len() + workflow_schedules.len();
        let monitor = SchedulerMonitor::new(total);
        let monitor_for_task = monitor.clone();
        let handle = tokio::spawn(async move {
            while !shutdown_for_task.is_cancelled() {
                if let Err(error) = tick_once_sharded(
                    pool.clone(),
                    router.clone(),
                    Arc::clone(&registry),
                    Arc::clone(&dags),
                    Arc::clone(&workflow_schedules),
                    monitor_for_task.clone(),
                )
                .await
                {
                    tracing::warn!(error = %error, "harvest scheduler tick failed");
                }

                tokio::select! {
                    () = shutdown_for_task.cancelled() => break,
                    () = tokio::time::sleep(DEFAULT_SCHEDULER_TICK_INTERVAL) => {}
                }
            }

            let total = dags.len() + workflow_schedules.len();
            monitor_for_task.mark_stopped(total);
        });

        Self {
            shutdown,
            handle,
            monitor,
        }
    }

    /// Returns a diagnostic monitor for the scheduler.
    #[must_use]
    pub fn monitor(&self) -> SchedulerMonitor {
        self.monitor.clone()
    }

    /// Requests that the background scheduler loop shut down gracefully.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// Wait for the background scheduler task to stop.
    ///
    /// # Errors
    ///
    /// Returns the Tokio join error if the scheduler task panicked.
    pub async fn join(self) -> Result<(), tokio::task::JoinError> {
        self.handle.await
    }
}

/// Compile the registered DAG metadata into a runtime catalog keyed by name.
///
/// # Errors
///
/// Returns [`HarvestError::Config`] if a DAG name is registered more than once
/// or its definition fails to compile.
pub fn compile_dag_catalog(dags: Vec<DagInfo>) -> HarvestResult<DagCatalog> {
    let mut catalog = DagCatalog::new();

    for dag in dags {
        let name = dag.name.to_string();
        if catalog.contains_key(&name) {
            return Err(HarvestError::Config(format!(
                "duplicate dag registration for '{}'",
                dag.name
            )));
        }

        let definition = dag
            .build_definition()
            .map_err(|error| HarvestError::Config(error.to_string()))?;
        catalog.insert(
            name.clone(),
            RegisteredDag {
                name,
                module: dag.module.to_string(),
                schedule: dag.schedule.clone(),
                catchup: dag.catchup,
                max_active_runs: dag.max_active_runs,
                default_queue: dag.default_queue.map(ToOwned::to_owned),
                is_unified: dag.workflow_handler.is_some(),
                definition,
                jitter: dag.jitter,
                overlap_policy: dag.overlap_policy,
                buffer_all_max: dag.buffer_all_max,
                owner: dag.owner.map(ToString::to_string),
                runbook_url: dag.runbook_url.map(ToString::to_string),
                severity: dag.severity.map(ToString::to_string),
            },
        );
    }

    Ok(catalog)
}

#[cfg(not(feature = "unified-dag-execution"))]
fn reject_classic_dags_without_unified_execution(dags: &DagCatalog) -> HarvestResult<()> {
    let classic_dag_names = dags
        .values()
        .filter(|dag| !dag.is_unified)
        .map(|dag| dag.name.as_str())
        .collect::<Vec<_>>();
    if !classic_dag_names.is_empty() {
        return Err(HarvestError::Config(format!(
            "classic DAG execution is not supported by the scheduler without \
             autumn-harvest/unified-dag-execution; rebuild with unified DAG execution \
             or remove classic DAGs: {}",
            classic_dag_names.join(", ")
        )));
    }
    Ok(())
}

/// Upsert the durable schedule rows for the provided DAG catalog.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] if the schedule rows cannot be read or
/// written.
pub async fn register_schedules(
    conn: &mut AsyncPgConnection,
    dags: &DagCatalog,
) -> HarvestResult<()> {
    #[cfg(not(feature = "unified-dag-execution"))]
    reject_classic_dags_without_unified_execution(dags)?;
    for dag in dags.values() {
        if let Some(schedule) = &dag.schedule {
            crate::policy::validate_schedule(schedule)
                .map_err(crate::error::HarvestError::Config)?;
        }
        upsert_schedule(conn, dag).await?;
    }
    Ok(())
}

async fn register_schedules_for_shard(
    conn: &mut AsyncPgConnection,
    dags: &DagCatalog,
    router: &ShardRouter,
    shard: ShardId,
) -> HarvestResult<()> {
    #[cfg(not(feature = "unified-dag-execution"))]
    reject_classic_dags_without_unified_execution(dags)?;
    for dag in dags.values() {
        if router.pick_for_dag(&dag.name) != shard {
            continue;
        }
        if let Some(schedule) = &dag.schedule {
            crate::policy::validate_schedule(schedule)
                .map_err(crate::error::HarvestError::Config)?;
        }
        upsert_schedule(conn, dag).await?;
    }
    Ok(())
}

/// Upsert the durable schedule rows for the provided workflow schedules.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] if the schedule rows cannot be read or
/// written.
pub async fn register_workflow_schedules(
    conn: &mut AsyncPgConnection,
    schedules: &[WorkflowSchedule],
) -> HarvestResult<()> {
    for ws in schedules {
        crate::policy::validate_schedule(&ws.schedule)
            .map_err(crate::error::HarvestError::Config)?;
        upsert_workflow_schedule(conn, ws).await?;
    }
    Ok(())
}

async fn register_workflow_schedules_for_shard(
    conn: &mut AsyncPgConnection,
    schedules: &[WorkflowSchedule],
    router: &ShardRouter,
    shard: ShardId,
) -> HarvestResult<()> {
    for ws in schedules {
        let owning_shard = workflow_schedule_shard(ws, router);
        if owning_shard == shard {
            crate::policy::validate_schedule(&ws.schedule)
                .map_err(crate::error::HarvestError::Config)?;
            upsert_workflow_schedule(conn, ws).await?;
        } else if ws.dag_name.is_some() {
            delete_stale_dag_workflow_schedule(conn, ws).await?;
        }
    }
    Ok(())
}

async fn delete_stale_dag_workflow_schedule(
    conn: &mut AsyncPgConnection,
    ws: &WorkflowSchedule,
) -> HarvestResult<()> {
    use crate::schema::harvest_schedules::dsl;

    let Some(dag_name) = ws.dag_name.as_deref() else {
        return Ok(());
    };

    diesel::delete(
        dsl::harvest_schedules
            .filter(
                dsl::workflow_name
                    .eq(&ws.workflow_name)
                    .or(dsl::workflow_name.is_null()),
            )
            .filter(dsl::dag_name.eq(dag_name).or(dsl::dag_name.is_null())),
    )
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;
    Ok(())
}

fn workflow_schedule_shard(schedule: &WorkflowSchedule, router: &ShardRouter) -> ShardId {
    schedule.dag_name.as_deref().map_or_else(
        || router.default_shard(),
        |dag_name| router.pick_for_dag(dag_name),
    )
}

/// Run one scheduler tick: dispatch due workflow-schedule runs.
///
/// The `_dags` parameter is retained for API compatibility; since
/// `unified-dag-execution` is the default, all DAGs are registered as workflow
/// schedules and `_dags` is always an empty catalog.
///
/// # Errors
///
/// Returns [`HarvestError`] if Postgres cannot be reached.
pub async fn tick_once(
    pool: DbPool,
    registry: Arc<HandlerRegistry>,
    dags: Arc<DagCatalog>,
    workflow_schedules: Arc<Vec<WorkflowSchedule>>,
    monitor: SchedulerMonitor,
) -> HarvestResult<()> {
    tick_once_sharded(
        ShardedDbPool::single(pool),
        ShardRouter::single(),
        registry,
        dags,
        workflow_schedules,
        monitor,
    )
    .await
}

/// Run one scheduler tick across every configured shard.
///
/// Classic DAG schedules are registered only on their owning DAG shard.
/// Unified DAG workflow schedules follow the same DAG shard so automatic runs
/// remain visible to the DAG APIs and carry an encoded shard id.
///
/// # Errors
///
/// Returns [`HarvestError`] if a shard connection or schedule registration
/// fails.
pub async fn tick_once_sharded(
    pool: ShardedDbPool,
    router: ShardRouter,
    registry: Arc<HandlerRegistry>,
    dags: Arc<DagCatalog>,
    workflow_schedules: Arc<Vec<WorkflowSchedule>>,
    monitor: SchedulerMonitor,
) -> HarvestResult<()> {
    #[cfg(not(feature = "unified-dag-execution"))]
    reject_classic_dags_without_unified_execution(dags.as_ref())?;

    let total = dags.len() + workflow_schedules.len();
    monitor.mark_tick(total);

    let metrics = Arc::clone(&registry.telemetry().metrics);

    // issue #377: load active gates ONCE from the central gate store (default
    // pool) before iterating shards. `harvest_admission_gates` is a
    // single-shard table stored on the default shard; reading it through a
    // per-shard connection would find an empty table on every shard 1+.
    // Fail-closed on error: skip the entire tick so gates are never bypassed
    // by a DB hiccup.
    #[cfg(feature = "db")]
    let active_gates: Vec<crate::admission_gate::AdmissionGate> = {
        match pool.pool_for_execution(ExecutionId::new()).get().await {
            Ok(mut gate_conn) => {
                match crate::admission_gate::db::load_active_gates(&mut gate_conn).await {
                    Ok(gates) => gates,
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "harvest: could not load admission gates; \
                             skipping scheduler tick (fail-closed)"
                        );
                        return Ok(());
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "harvest: could not acquire gate-store connection; \
                     skipping scheduler tick (fail-closed)"
                );
                return Ok(());
            }
        }
    };
    // When the `db` feature is absent the gate table does not exist; use an
    // empty slice so the downstream functions compile without gates.
    #[cfg(not(feature = "db"))]
    let active_gates: Vec<crate::admission_gate::AdmissionGate> = Vec::new();

    for (shard, shard_pool) in pool.iter_shards() {
        let mut conn = shard_pool
            .get()
            .await
            .map_err(|error| HarvestError::Database(error.to_string()))?;

        register_schedules_for_shard(&mut conn, dags.as_ref(), &router, shard).await?;
        register_workflow_schedules_for_shard(
            &mut conn,
            workflow_schedules.as_ref(),
            &router,
            shard,
        )
        .await?;

        // Drain buffered slots BEFORE evaluating newly-due firings so that
        // capacity freed by a just-completed run is consumed by the oldest
        // pending slot first, not by the freshest next_run_at firing.
        #[cfg(feature = "db")]
        if let Err(error) = drain_buffered_schedule_runs(
            &mut conn,
            shard,
            dags.as_ref(),
            registry.as_ref(),
            &metrics,
            &active_gates,
        )
        .await
        {
            tracing::warn!(
                error = %error,
                shard_id = shard.as_i32(),
                "harvest: buffered schedule drain error"
            );
        }

        if let Err(error) = tick_workflow_schedules(
            &mut conn,
            shard,
            dags.as_ref(),
            registry.as_ref(),
            &metrics,
            &active_gates,
        )
        .await
        {
            tracing::warn!(
                error = %error,
                shard_id = shard.as_i32(),
                "harvest workflow-schedule tick error"
            );
        }
    }

    Ok(())
}

/// Trigger a DAG run as a workflow execution (issue #256 Step 5).
///
/// All DAGs run on the unified workflow execution path. This starts a workflow
/// execution for the named DAG using `start_or_load_workflow_execution`.
///
/// # Errors
///
/// Returns [`HarvestError`] if the DB pool is exhausted or the workflow start
/// transaction fails.
#[allow(clippy::too_many_arguments)]
pub async fn trigger_unified_dag(
    pool: DbPool,
    dag_name: &str,
    run_conf: Option<Value>,
    shard: crate::types::ShardId,
    default_queue: &str,
    owner: Option<&str>,
    runbook_url: Option<&str>,
    severity: Option<&str>,
) -> HarvestResult<StartedWorkflowExecution> {
    let mut db = pool
        .get()
        .await
        .map_err(|error| HarvestError::Database(error.to_string()))?;

    let exec_id = ExecutionId::new_for_shard(shard);
    // Use the exec_id UUID as the deduplication key so back-to-back manual
    // triggers always produce distinct workflow IDs regardless of clock resolution.
    let workflow_id = format!("{dag_name}-{exec_id}");

    // Resolve the DAG schedule row by its DAG marker first. Some upgrade paths
    // can still have workflow-only rows, so use those as a fallback until
    // registration merges them.
    let schedule = {
        use crate::schema::harvest_schedules::dsl;
        let rows = dsl::harvest_schedules
            .filter(
                dsl::dag_name
                    .eq(dag_name)
                    .or(dsl::workflow_name.eq(dag_name)),
            )
            .select(HarvestSchedule::as_select())
            .load::<HarvestSchedule>(&mut db)
            .await
            .map_err(crate::error::database_error)?;
        rows.iter()
            .find(|row| row.dag_name.as_deref() == Some(dag_name))
            .cloned()
            .or_else(|| rows.into_iter().next())
    };

    if let Some(schedule) = schedule.as_ref() {
        if schedule.is_paused {
            return Err(HarvestError::UpdateRejected {
                reason: format!(
                    "DAG '{dag_name}' is paused; manual trigger is deferred until the schedule is resumed"
                ),
            });
        }

        let running: i64 = harvest_workflow_executions::table
            .filter(harvest_workflow_executions::workflow_name.eq(dag_name))
            .filter(harvest_workflow_executions::state.eq_any(["RUNNING", "PAUSED"]))
            .count()
            .get_result(&mut db)
            .await
            .map_err(crate::error::database_error)?;
        if running >= i64::from(schedule.max_active_runs) {
            return Err(HarvestError::UpdateRejected {
                reason: format!(
                    "DAG '{dag_name}' max_active_runs reached ({running}/{}); manual trigger is deferred",
                    schedule.max_active_runs
                ),
            });
        }
    }

    let queue_name = schedule
        .as_ref()
        .and_then(|schedule| schedule.queue_name.clone())
        .unwrap_or_else(|| default_queue.to_string());
    let input = run_conf.unwrap_or(Value::Null);

    start_or_load_workflow_execution(
        &mut db,
        StartWorkflowParams {
            workflow_name: dag_name,
            workflow_id: &workflow_id,
            exec_id,
            input,
            parent_id: None,
            queue_name: &queue_name,
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            concurrency_key: None,
            concurrency_limit: None,
            priority: Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: None,
            delay: None,
            max_workflow_start_delay: None,
            owner,
            runbook_url,
            severity,
            context_headers: None,

            sla: None,
            // Attribute the manual API trigger to the schedule so it appears in
            // GET /admin/schedules/{id}/runs with origin='manual_trigger'.
            // scheduled_for stays None so resolve_carryover (issue #488) still
            // short-circuits — NULL slot comparisons are false.
            schedule_id: schedule.as_ref().map(|s| s.id),
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: None,
            retry_of_exec_id: None,
            max_workflow_attempts_ceiling: None,
            origin: schedule
                .as_ref()
                .map(|_| crate::execution::ORIGIN_MANUAL_TRIGGER),
            completion_callbacks: None,
        },
    )
    .await
}

/// Upsert the durable schedule row for one registered DAG.
///
/// This is used by management API paths that need pause metadata even before
/// the background scheduler has run its registration tick.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] if the schedule row cannot be read or
/// written.
pub async fn ensure_dag_schedule(
    conn: &mut AsyncPgConnection,
    dag: &RegisteredDag,
) -> HarvestResult<HarvestSchedule> {
    upsert_schedule(conn, dag).await
}

async fn merge_pause_metadata_into_schedule(
    conn: &mut AsyncPgConnection,
    target: &HarvestSchedule,
    source: &HarvestSchedule,
) -> HarvestResult<HarvestSchedule> {
    use crate::schema::harvest_schedules::dsl;

    let paused_at_value = target.paused_at.or(source.paused_at);
    let paused_by_value = target
        .paused_by
        .clone()
        .or_else(|| source.paused_by.clone());
    let pause_reason_value = target
        .pause_reason
        .clone()
        .or_else(|| source.pause_reason.clone());

    diesel::update(dsl::harvest_schedules.find(target.id))
        .set((
            dsl::is_paused.eq(target.is_paused || source.is_paused),
            dsl::paused_at.eq(paused_at_value),
            dsl::paused_by.eq(paused_by_value.as_deref()),
            dsl::pause_reason.eq(pause_reason_value.as_deref()),
        ))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;

    dsl::harvest_schedules
        .find(target.id)
        .select(HarvestSchedule::as_select())
        .first(conn)
        .await
        .map_err(crate::error::database_error)
}

async fn find_reusable_dag_schedule(
    conn: &mut AsyncPgConnection,
    dag_name: &str,
) -> HarvestResult<Option<HarvestSchedule>> {
    use crate::schema::harvest_schedules::dsl;

    let dag_row = dsl::harvest_schedules
        .filter(dsl::dag_name.eq(dag_name))
        .select(HarvestSchedule::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?;

    let workflow_only_row = dsl::harvest_schedules
        .filter(dsl::workflow_name.eq(dag_name))
        .filter(dsl::dag_name.is_null())
        .select(HarvestSchedule::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?;

    match (dag_row, workflow_only_row) {
        (Some(dag_row), Some(workflow_only_row)) if dag_row.id != workflow_only_row.id => {
            let merged =
                merge_pause_metadata_into_schedule(conn, &dag_row, &workflow_only_row).await?;
            diesel::delete(dsl::harvest_schedules.find(workflow_only_row.id))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
            Ok(Some(merged))
        }
        (Some(dag_row), _) => Ok(Some(dag_row)),
        (None, Some(workflow_only_row)) => Ok(Some(workflow_only_row)),
        (None, None) => Ok(None),
    }
}

async fn upsert_schedule(
    conn: &mut AsyncPgConnection,
    dag: &RegisteredDag,
) -> HarvestResult<HarvestSchedule> {
    use crate::schema::harvest_schedules::dsl;

    let existing = find_reusable_dag_schedule(conn, &dag.name).await?;
    let now = Utc::now();
    let expr = schedule_expr(dag.schedule.as_ref());

    if let Some(existing) = existing {
        let schedule_changed = existing.schedule_expr != expr;
        let next_run_at = if schedule_changed {
            next_run_after(dag.schedule.as_ref(), now)
        } else {
            existing
                .next_run_at
                .or_else(|| next_run_after(dag.schedule.as_ref(), now))
        };
        // Clear buffered slots when the schedule cadence changes or when the
        // operator switches away from a buffering policy, so stale firings are
        // not dispatched under a configuration that never produced them.  Also
        // trim to the new cap when tightening the policy or buffer_all_max.
        let is_buffering_policy = matches!(
            dag.overlap_policy,
            OverlapPolicy::BufferOne | OverlapPolicy::BufferAll
        );
        let new_buffered_runs = if is_buffering_policy && !schedule_changed {
            let cap = if dag.overlap_policy == OverlapPolicy::BufferOne {
                1usize
            } else {
                usize::try_from(dag.buffer_all_max.max(1)).unwrap_or(usize::MAX)
            };
            let mut existing_buffered = parse_buffered_runs(&existing.buffered_runs);
            existing_buffered.truncate(cap);
            buffered_runs_to_json(&existing_buffered)
        } else {
            serde_json::json!([])
        };
        let tz = dag.schedule.as_ref().map_or("UTC", Schedule::timezone_str);
        diesel::update(dsl::harvest_schedules.find(existing.id))
            .set((
                dsl::schedule_expr.eq(expr.clone()),
                dsl::timezone.eq(tz),
                dsl::catchup.eq(dag.catchup),
                // DAGs use the legacy `catchup` bool, not the bounded-catchup
                // policy columns. When this registration reuses a row that was
                // previously a workflow schedule with a `most_recent` / `window`
                // policy, clear the stale policy columns: otherwise the
                // policy-takes-precedence resolver (`CatchupPolicy::from_db`) would
                // keep driving ticks off the old bounded policy and ignore the
                // DAG's `catchup` setting (issue #484 / Codex #1552).
                dsl::catchup_policy.eq(Option::<String>::None),
                dsl::catchup_window_secs.eq(Option::<i64>::None),
                dsl::max_active_runs.eq(i32::try_from(dag.max_active_runs).unwrap_or(i32::MAX)),
                dsl::dag_name.eq(Some(dag.name.as_str())),
                dsl::updated_at.eq(now),
                dsl::next_run_at.eq(next_run_at),
                dsl::jitter_secs.eq(i64::try_from(dag.jitter.as_secs()).unwrap_or(i64::MAX)),
                dsl::overlap_policy.eq(dag.overlap_policy.as_str()),
                dsl::buffer_all_max.eq(i32::try_from(dag.buffer_all_max).unwrap_or(i32::MAX)),
                dsl::buffered_runs.eq(new_buffered_runs),
                // calendar_name and skip_policy are not set by DAG registration;
                // they are operator-managed via the CRUD API.
            ))
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;

        dsl::harvest_schedules
            .find(existing.id)
            .select(HarvestSchedule::as_select())
            .first(conn)
            .await
            .map_err(crate::error::database_error)
    } else {
        let row = NewHarvestSchedule {
            id: uuid::Uuid::new_v4(),
            dag_name: Some(&dag.name),
            schedule_expr: expr.as_deref(),
            timezone: dag.schedule.as_ref().map_or("UTC", Schedule::timezone_str),
            catchup: dag.catchup,
            max_active_runs: i32::try_from(dag.max_active_runs).unwrap_or(i32::MAX),
            is_paused: false,
            workflow_name: None,
            workflow_input: None,
            queue_name: None,
            jitter_secs: i64::try_from(dag.jitter.as_secs()).unwrap_or(i64::MAX),
            overlap_policy: dag.overlap_policy.as_str(),
            buffered_runs: serde_json::json!([]),
            buffer_all_max: i32::try_from(dag.buffer_all_max).unwrap_or(i32::MAX),
            calendar_name: None,
            skip_policy: crate::policy::SkipPolicy::Skip.as_str(),
        };
        diesel::insert_into(harvest_schedules::table)
            .values(&row)
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;

        let inserted = dsl::harvest_schedules
            .filter(dsl::dag_name.eq(&dag.name))
            .select(HarvestSchedule::as_select())
            .first(conn)
            .await
            .map_err(crate::error::database_error)?;
        let initial_next_run = next_run_after(dag.schedule.as_ref(), now);
        diesel::update(dsl::harvest_schedules.find(inserted.id))
            .set(dsl::next_run_at.eq(initial_next_run))
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;

        dsl::harvest_schedules
            .find(inserted.id)
            .select(HarvestSchedule::as_select())
            .first(conn)
            .await
            .map_err(crate::error::database_error)
    }
}

/// Upsert a `harvest_schedules` row for a [`WorkflowSchedule`].
///
/// Unified DAG schedules first reuse any existing classic DAG row keyed by
/// `dag_name`, then write `workflow_name` onto that row. Workflow-only schedules
/// use `ON CONFLICT (workflow_name) DO NOTHING` so concurrent scheduler instances
/// cannot produce duplicate rows. A subsequent `UPDATE` refreshes all mutable
/// fields, preserving `is_paused` (managed independently via pause/resume).
async fn find_reusable_dag_workflow_schedule(
    conn: &mut AsyncPgConnection,
    dag_name: &str,
    workflow_name: &str,
) -> HarvestResult<Option<HarvestSchedule>> {
    use crate::schema::harvest_schedules::dsl;

    let dag_row = dsl::harvest_schedules
        .filter(dsl::dag_name.eq(dag_name))
        .select(HarvestSchedule::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?;

    let workflow_only_row = dsl::harvest_schedules
        .filter(dsl::workflow_name.eq(workflow_name))
        .filter(dsl::dag_name.is_null())
        .select(HarvestSchedule::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?;

    match (dag_row, workflow_only_row) {
        (Some(dag_row), Some(workflow_only_row)) if dag_row.id != workflow_only_row.id => {
            let dag_row =
                merge_pause_metadata_into_schedule(conn, &dag_row, &workflow_only_row).await?;
            diesel::delete(dsl::harvest_schedules.find(workflow_only_row.id))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
            Ok(Some(dag_row))
        }
        (Some(dag_row), _) => Ok(Some(dag_row)),
        (None, Some(workflow_only_row)) => Ok(Some(workflow_only_row)),
        (None, None) => Ok(None),
    }
}

async fn insert_dag_workflow_schedule_if_missing(
    conn: &mut AsyncPgConnection,
    ws: &WorkflowSchedule,
    dag_name: &str,
    expr: Option<&str>,
) -> HarvestResult<HarvestSchedule> {
    use crate::schema::harvest_schedules::dsl;

    let row = NewHarvestSchedule {
        id: uuid::Uuid::new_v4(),
        dag_name: Some(dag_name),
        schedule_expr: expr,
        timezone: ws.schedule.timezone_str(),
        catchup: ws.catchup,
        max_active_runs: i32::try_from(ws.max_active_runs).unwrap_or(i32::MAX),
        is_paused: ws.paused,
        workflow_name: Some(&ws.workflow_name),
        workflow_input: Some(ws.input.clone()),
        queue_name: Some(ws.queue_name.as_str()),
        jitter_secs: i64::try_from(ws.jitter.as_secs()).unwrap_or(i64::MAX),
        overlap_policy: ws.overlap_policy.as_str(),
        buffered_runs: serde_json::json!([]),
        buffer_all_max: i32::try_from(ws.buffer_all_max).unwrap_or(i32::MAX),
        calendar_name: ws.calendar.as_deref(),
        skip_policy: ws.skip_policy.as_str(),
    };
    diesel::insert_into(harvest_schedules::table)
        .values(&row)
        .on_conflict(dsl::dag_name)
        .do_nothing()
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;

    dsl::harvest_schedules
        .filter(dsl::dag_name.eq(dag_name))
        .select(HarvestSchedule::as_select())
        .first(conn)
        .await
        .map_err(crate::error::database_error)
}

async fn insert_workflow_schedule_if_missing(
    conn: &mut AsyncPgConnection,
    ws: &WorkflowSchedule,
    expr: Option<&str>,
) -> HarvestResult<HarvestSchedule> {
    use crate::schema::harvest_schedules::dsl;

    let row = NewHarvestSchedule {
        id: uuid::Uuid::new_v4(),
        dag_name: None,
        schedule_expr: expr,
        timezone: ws.schedule.timezone_str(),
        catchup: ws.catchup,
        max_active_runs: i32::try_from(ws.max_active_runs).unwrap_or(i32::MAX),
        // is_paused is set on initial insert only; subsequent upserts preserve the
        // current value so that pause/resume state is not accidentally overwritten.
        is_paused: ws.paused,
        workflow_name: Some(&ws.workflow_name),
        workflow_input: Some(ws.input.clone()),
        queue_name: Some(ws.queue_name.as_str()),
        jitter_secs: i64::try_from(ws.jitter.as_secs()).unwrap_or(i64::MAX),
        overlap_policy: ws.overlap_policy.as_str(),
        buffered_runs: serde_json::json!([]),
        buffer_all_max: i32::try_from(ws.buffer_all_max).unwrap_or(i32::MAX),
        calendar_name: ws.calendar.as_deref(),
        skip_policy: ws.skip_policy.as_str(),
    };
    diesel::insert_into(harvest_schedules::table)
        .values(&row)
        .on_conflict(dsl::workflow_name)
        .do_nothing()
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;

    dsl::harvest_schedules
        .filter(dsl::workflow_name.eq(&ws.workflow_name))
        .select(HarvestSchedule::as_select())
        .first(conn)
        .await
        .map_err(crate::error::database_error)
}

async fn find_or_insert_workflow_schedule(
    conn: &mut AsyncPgConnection,
    ws: &WorkflowSchedule,
    expr: Option<&str>,
) -> HarvestResult<HarvestSchedule> {
    if let Some(dag_name) = ws.dag_name.as_deref() {
        // Unified DAG schedules are keyed by dag_name during upgrade from
        // classic schedule rows. Also reuse the short-lived workflow-only
        // representation keyed by workflow_name so upgraded deployments do not
        // trip harvest_schedules_workflow_name_unique before dag_name conflict
        // handling can run.
        if let Some(existing) =
            find_reusable_dag_workflow_schedule(conn, dag_name, &ws.workflow_name).await?
        {
            Ok(existing)
        } else {
            insert_dag_workflow_schedule_if_missing(conn, ws, dag_name, expr).await
        }
    } else {
        // Attempt an atomic insert. The UNIQUE constraint on workflow_name means a
        // concurrent writer will hit DO NOTHING rather than inserting a duplicate.
        insert_workflow_schedule_if_missing(conn, ws, expr).await
    }
}

#[allow(clippy::too_many_lines)]
async fn upsert_workflow_schedule(
    conn: &mut AsyncPgConnection,
    ws: &WorkflowSchedule,
) -> HarvestResult<HarvestSchedule> {
    let expr = schedule_expr(Some(&ws.schedule));
    let existing = find_or_insert_workflow_schedule(conn, ws, expr.as_deref()).await?;
    match apply_workflow_schedule_update(conn, ws, &existing, false).await? {
        AppliedScheduleUpdate::Updated(row) => Ok(*row),
        // Unreachable: the unguarded (guard_live_fire_claim = false) path never
        // skips. Kept as an error rather than a panic for defensive robustness.
        AppliedScheduleUpdate::SkippedLiveClaim => Err(crate::error::database_error(
            "schedule upsert unexpectedly skipped by fire-claim guard",
        )),
    }
}

/// Result of [`apply_workflow_schedule_update`].
#[derive(Debug)]
enum AppliedScheduleUpdate {
    /// The row was updated; the re-selected row is returned.
    Updated(Box<HarvestSchedule>),
    /// `guard_live_fire_claim` was set and the row currently holds a live
    /// (unexpired) #350 fire claim — nothing was written.
    SkippedLiveClaim,
}

/// Shared in-place UPDATE body for an existing `harvest_schedules` row.
///
/// Single source of truth for the update semantics used by both the
/// startup/registration upsert ([`upsert_workflow_schedule`]) and the
/// operator PATCH path ([`update_workflow_schedule`], issue #771):
/// `next_run_at` recompute on cadence change only, buffered-runs
/// preservation/trim rules (#241), legacy `catchup` bool mirroring (#484),
/// exhaustion reconciliation in both directions (#478), and the
/// `auto_paused_at` clear rule (#360).
///
/// With `guard_live_fire_claim` the main UPDATE additionally requires
/// `fire_claim_token IS NULL OR fire_claimed_until < NOW()` (issue #350):
/// when a scheduler replica currently holds a live claim on the row (a fire
/// is in flight), nothing is written and `SkippedLiveClaim` is returned so
/// an edit can never race an in-flight fire. The upsert path passes `false`,
/// preserving its pre-existing unconditional-update behavior.
#[allow(clippy::too_many_lines)]
async fn apply_workflow_schedule_update(
    conn: &mut AsyncPgConnection,
    ws: &WorkflowSchedule,
    existing: &HarvestSchedule,
    guard_live_fire_claim: bool,
) -> HarvestResult<AppliedScheduleUpdate> {
    use crate::schema::harvest_schedules::dsl;

    let now = Utc::now();
    let expr = schedule_expr(Some(&ws.schedule));
    let dag_name = ws.dag_name.as_deref().or(existing.dag_name.as_deref());

    // Recalculate next_run_at: reset on schedule-expression change, preserve otherwise.
    let schedule_changed = existing.schedule_expr != expr;
    let next_run_at = if schedule_changed {
        next_run_after(Some(&ws.schedule), now)
    } else {
        existing
            .next_run_at
            .or_else(|| next_run_after(Some(&ws.schedule), now))
    };
    // is_paused is deliberately excluded — it is managed via pause/resume, not here.
    // buffered_runs: preserved only when the policy is still buffering AND the cadence
    // has not changed.  Also trim to the new effective cap so that tightening the
    // policy (BufferAll→BufferOne, or lowering buffer_all_max) never lets the drain
    // dispatch more slots than the updated configuration permits.
    let is_buffering_policy = matches!(
        ws.overlap_policy,
        OverlapPolicy::BufferOne | OverlapPolicy::BufferAll
    );
    let new_buffered_runs = if is_buffering_policy && !schedule_changed {
        let cap = if ws.overlap_policy == OverlapPolicy::BufferOne {
            1usize
        } else {
            usize::try_from(ws.buffer_all_max.max(1)).unwrap_or(usize::MAX)
        };
        let mut existing_buffered = parse_buffered_runs(&existing.buffered_runs);
        existing_buffered.truncate(cap);
        buffered_runs_to_json(&existing_buffered)
    } else {
        serde_json::json!([])
    };
    let changeset = (
        dsl::schedule_expr.eq(expr),
        dsl::timezone.eq(ws.schedule.timezone_str()),
        // Mirror the effective catchup policy into the legacy `catchup`
        // bool so API responses and any rollback/older reader that only
        // honors the legacy column see the same enabled/disabled decision
        // the policy columns encode (issue #484 / Codex #1220). When no
        // policy is set the caller's explicit `catchup` bool is preserved.
        dsl::catchup.eq(ws
            .catchup_policy
            .map_or(ws.catchup, crate::policy::CatchupPolicy::is_catchup_enabled)),
        dsl::max_active_runs.eq(i32::try_from(ws.max_active_runs).unwrap_or(i32::MAX)),
        dsl::dag_name.eq(dag_name),
        dsl::workflow_name.eq(Some(ws.workflow_name.as_str())),
        dsl::workflow_input.eq(Some(ws.input.clone())),
        dsl::queue_name.eq(Some(ws.queue_name.as_str())),
        dsl::updated_at.eq(now),
        dsl::next_run_at.eq(next_run_at),
        dsl::jitter_secs.eq(i64::try_from(ws.jitter.as_secs()).unwrap_or(i64::MAX)),
        dsl::overlap_policy.eq(ws.overlap_policy.as_str()),
        dsl::buffer_all_max.eq(i32::try_from(ws.buffer_all_max).unwrap_or(i32::MAX)),
        dsl::buffered_runs.eq(new_buffered_runs),
        dsl::calendar_name.eq(ws.calendar.as_deref()),
        dsl::skip_policy.eq(ws.skip_policy.as_str()),
        dsl::consecutive_failure_limit.eq(ws
            .consecutive_failure_limit
            .map(|n| i32::try_from(n).unwrap_or(i32::MAX))),
        dsl::end_at.eq(ws.end_at),
        dsl::max_runs.eq(ws.max_runs.map(|n| i32::try_from(n).unwrap_or(i32::MAX))),
        // Catchup policy columns (issue #484).
        dsl::catchup_policy.eq(ws.catchup_policy.and_then(|p| p.to_db_columns().0)),
        dsl::catchup_window_secs.eq(ws.catchup_policy.and_then(|p| p.to_db_columns().1)),
        dsl::retry_policy.eq(ws
            .retry_policy
            .as_ref()
            .and_then(|p| serde_json::to_value(p).ok())),
    );
    if guard_live_fire_claim {
        // Anti-race with the scheduler tick (issue #350 / #771 AC7): refuse to
        // rewrite a row whose fire claim is currently live. The tick's claim
        // UPDATE and its finalize both evaluate claim expiry against the DB
        // clock (`NOW()`), so this guard uses `diesel::dsl::now` rather than
        // the app clock — a skewed API host must never judge a claim expired
        // that the #350 machinery still considers live (or vice versa).
        //
        // When the guard passes because the claim has *expired* (crashed or
        // straggling replica left a stale token behind), the token is also
        // nulled out here: a straggler fire's finalize UPDATEs are guarded by
        // `fire_claim_token = $its_token`, so fencing the token guarantees a
        // late finalize matches zero rows and can never clobber the edited
        // `next_run_at`/`buffered_runs` with old-spec values — exactly the
        // fencing a peer replica's claim-steal performs.
        let rows_updated = diesel::update(
            dsl::harvest_schedules.find(existing.id).filter(
                dsl::fire_claim_token
                    .is_null()
                    .or(dsl::fire_claimed_until.lt(diesel::dsl::now)),
            ),
        )
        .set((
            changeset,
            dsl::fire_claim_token.eq(Option::<uuid::Uuid>::None),
            dsl::fire_claimed_until.eq(Option::<DateTime<Utc>>::None),
        ))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
        if rows_updated == 0 {
            return Ok(AppliedScheduleUpdate::SkippedLiveClaim);
        }
    } else {
        diesel::update(dsl::harvest_schedules.find(existing.id))
            .set(changeset)
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;
    }

    // Clear exhausted state when the operator updates limits so that future runs are
    // again possible (issue #478). A schedule exhausted by end_at is no longer
    // exhausted if end_at is extended or removed; similarly for max_runs.
    // The main UPDATE above already resets next_run_at via the or_else fallback
    // (existing.next_run_at was NULL → fresh next_run_after computation), so we only
    // need to nullify exhausted_at / exhausted_reason here.
    if existing.exhausted_at.is_some() {
        // For end_at: "limit removed" or "there is a valid next slot strictly before
        // the new end_at". Checking `end_at > now` is not sufficient — the schedule
        // can be legitimately exhausted before the cutoff wall-time when its last
        // valid slot has already been dispatched (next_run_after >= end_at). Only
        // clear exhaustion when the schedule expression will actually produce a new
        // firing inside the new window.
        // Special case: Manual schedules have no automatic next slot (next_run_after
        // returns None), so treat them as ok when the wall clock hasn't yet reached
        // the cutoff (manual triggers are still allowed until end_at).
        let end_at_ok = ws.end_at.is_none_or(|new_end| {
            if matches!(ws.schedule, crate::policy::Schedule::Manual) {
                return now < new_end;
            }
            next_run_after(Some(&ws.schedule), now).is_some_and(|next| next < new_end)
        });
        let max_runs_ok = ws.max_runs.is_none_or(|max| {
            i64::from(existing.runs_started) < i64::from(i32::try_from(max).unwrap_or(i32::MAX))
        });
        if end_at_ok && max_runs_ok {
            diesel::update(dsl::harvest_schedules.find(existing.id))
                .set((
                    dsl::exhausted_at.eq(None::<DateTime<Utc>>),
                    dsl::exhausted_reason.eq(None::<String>),
                    dsl::updated_at.eq(now),
                ))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
        }
    } else {
        // Active schedule: if the new limits are already violated, transition to
        // exhausted immediately rather than waiting for the next due tick (issue #478).
        // This prevents a schedule whose max_runs was lowered to the current
        // runs_started (or whose end_at was moved before next_run_at) from lingering
        // as active-but-never-runnable until the next tick processes it.
        let max_runs_now_violated = ws.max_runs.is_some_and(|max| {
            let max_i32 = i32::try_from(max).unwrap_or(i32::MAX);
            max_i32 > 0 && existing.runs_started >= max_i32
        });
        let end_at_now_violated = ws.end_at.is_some_and(|new_end| {
            // Manual schedules have no automatic next slot; only exhaust when the
            // wall clock has already reached the cutoff so manual triggers remain
            // possible until then.
            if matches!(ws.schedule, crate::policy::Schedule::Manual) {
                return now >= new_end;
            }
            // Preserve any overdue existing next_run_at that is still within the
            // window (e.g. downtime left next_run_at=10:00, end_at=10:30, now=11:00).
            // The scheduler must process that slot before we can declare exhaustion.
            if existing.next_run_at.is_some_and(|t| t < new_end) {
                return false;
            }
            next_run_after(Some(&ws.schedule), now).is_none_or(|next| next >= new_end)
        });
        if max_runs_now_violated || end_at_now_violated {
            let reason: &str = if max_runs_now_violated {
                "max_runs_exhausted"
            } else {
                "end_at_reached"
            };
            diesel::update(
                dsl::harvest_schedules
                    .find(existing.id)
                    .filter(dsl::exhausted_at.is_null()),
            )
            .set((
                dsl::exhausted_at.eq(Some(now)),
                dsl::exhausted_reason.eq(Some(reason)),
                dsl::next_run_at.eq(None::<DateTime<Utc>>),
                dsl::updated_at.eq(now),
            ))
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;
        }
    }

    // Clear auto_paused_at only when disabling the feature (limit = None or Some(0)).
    // When the limit is positive we deliberately leave the DB value untouched so a
    // concurrent failure-counter auto-pause (set by the worker completion path) is
    // never silently overwritten by a redeployment upsert.
    let limit_disabled = ws.consecutive_failure_limit.is_none_or(|n| n == 0);
    if limit_disabled {
        diesel::update(dsl::harvest_schedules.find(existing.id))
            .set(dsl::auto_paused_at.eq(None::<DateTime<Utc>>))
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;
    }

    let row = dsl::harvest_schedules
        .find(existing.id)
        .select(HarvestSchedule::as_select())
        .first(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(AppliedScheduleUpdate::Updated(Box::new(row)))
}

/// Partial, id-keyed update for an existing workflow schedule (issue #771).
///
/// Every field is optional: `None` leaves the stored value unchanged.
/// Nullable columns use a double-`Option` — the outer `None` means
/// "unchanged", `Some(None)` means "clear to NULL", `Some(Some(v))` sets `v`.
///
/// The schedule's workflow type is deliberately absent: it is not editable
/// (changing it is semantically a different schedule).
#[derive(Debug, Clone, Default)]
pub struct WorkflowSchedulePatch {
    /// New schedule cadence. Changing it recomputes `next_run_at` anchored
    /// at now — elapsed slots are never retroactively fired.
    pub schedule: Option<Schedule>,
    /// New JSON input passed to every scheduled run.
    pub input: Option<serde_json::Value>,
    /// New task queue name for dispatched runs.
    pub queue_name: Option<String>,
    /// New legacy catchup bool. Ignored when a `catchup_policy` is stored or
    /// provided (the policy takes precedence, issue #484).
    pub catchup: Option<bool>,
    /// New maximum number of concurrently running scheduled executions.
    pub max_active_runs: Option<u32>,
    /// New jitter window (issue #240).
    pub jitter: Option<Duration>,
    /// New overlap policy (issue #241).
    pub overlap_policy: Option<OverlapPolicy>,
    /// New maximum buffered slots under `BufferAll` (issue #241).
    pub buffer_all_max: Option<u32>,
    /// New calendar association (issue #337). `Some(None)` detaches.
    pub calendar: Option<Option<String>>,
    /// New calendar skip policy (issue #337).
    pub skip_policy: Option<crate::policy::SkipPolicy>,
    /// New auto-pause threshold (issue #360). `Some(None)` disables.
    pub consecutive_failure_limit: Option<Option<u32>>,
    /// New absolute cutoff (issue #478). `Some(None)` removes the cutoff.
    pub end_at: Option<Option<DateTime<Utc>>>,
    /// New total run budget (issue #478). `Some(None)` (or `Some(Some(0))`)
    /// removes the budget.
    pub max_runs: Option<Option<u32>>,
    /// New bounded catchup policy (issue #484). `Some(None)` clears the
    /// policy columns, falling back to the legacy `catchup` bool.
    pub catchup_policy: Option<Option<crate::policy::CatchupPolicy>>,
    /// New schedule-level retry policy (issue #523). `Some(None)` clears.
    pub retry_policy: Option<Option<crate::policy::RetryPolicy>>,
}

/// Typed outcome of [`update_workflow_schedule`].
#[derive(Debug)]
pub enum ScheduleUpdateOutcome {
    /// The row was updated in place; the re-selected row is returned
    /// (boxed: the row is much larger than the other variants).
    Updated(Box<HarvestSchedule>),
    /// No `harvest_schedules` row with the given id exists on this shard.
    NotFound,
    /// The row is a DAG schedule (`dag_name IS NOT NULL`) — DAG schedules
    /// are owned by `PATCH /dags/{dag_name}`, not this updater.
    DagSchedule,
    /// A scheduler replica currently holds a live (unexpired) #350 fire
    /// claim on the row — the fire is in flight. Nothing was written; retry
    /// shortly (the claim lease is bounded at ~30 s).
    ClaimLive,
}

/// Merge a [`WorkflowSchedulePatch`] over an existing `harvest_schedules` row
/// into the full [`WorkflowSchedule`] the shared update body consumes.
///
/// Only fields present in the patch change; everything else round-trips from
/// the stored row (`schedule_expr` is re-parsed with the same lenient rules
/// the tick loop uses, so an unparseable/NULL expression is treated as
/// `Schedule::Manual`).
///
/// # Errors
///
/// Returns [`HarvestError::Config`] when the row has no `workflow_name` (not
/// a workflow schedule) or when the patch leaves `retry_policy` untouched but
/// the stored `retry_policy` JSON does not deserialize as a `RetryPolicy` —
/// erroring loudly instead of silently dropping the stored policy to NULL on
/// an unrelated edit (repair by explicitly setting or clearing it).
fn merge_schedule_patch(
    existing: &HarvestSchedule,
    patch: &WorkflowSchedulePatch,
) -> HarvestResult<WorkflowSchedule> {
    let workflow_name = existing.workflow_name.clone().ok_or_else(|| {
        HarvestError::Config(format!(
            "schedule {} has no workflow_name; not a workflow schedule",
            existing.id
        ))
    })?;
    let existing_schedule = existing
        .schedule_expr
        .as_deref()
        .and_then(parse_schedule_from_expr)
        .unwrap_or(Schedule::Manual);
    // Reconstruct the stored catchup policy without the legacy-bool fallback:
    // a NULL policy column stays None so an unrelated patch never converts a
    // legacy-bool row into an explicit-policy row.
    let existing_catchup_policy = existing.catchup_policy.as_deref().map(|_| {
        crate::policy::CatchupPolicy::from_db(
            existing.catchup_policy.as_deref(),
            existing.catchup_window_secs,
            existing.catchup,
        )
    });
    // Resolve the retry policy the merged spec carries. When the patch does
    // not touch `retry_policy`, the stored JSON must round-trip through the
    // typed struct (the shared update body re-serializes it); a stored value
    // that fails to deserialize is surfaced as an error rather than silently
    // dropped to NULL by an unrelated patch — the operator repairs the row by
    // explicitly setting or clearing `retry_policy`.
    let merged_retry_policy: Option<crate::policy::RetryPolicy> = match patch.retry_policy.as_ref()
    {
        Some(new) => new.clone(),
        None => match existing.retry_policy.as_ref() {
            Some(stored) => Some(serde_json::from_value(stored.clone()).map_err(|e| {
                HarvestError::Config(format!(
                    "stored retry_policy for schedule {} is not a valid RetryPolicy ({e}); \
                         set or clear retry_policy explicitly in the patch to repair it",
                    existing.id
                ))
            })?),
            None => None,
        },
    };

    Ok(WorkflowSchedule {
        workflow_name,
        dag_name: None,
        schedule: patch.schedule.clone().unwrap_or(existing_schedule),
        input: patch
            .input
            .clone()
            .or_else(|| existing.workflow_input.clone())
            .unwrap_or(serde_json::Value::Null),
        catchup: patch.catchup.unwrap_or(existing.catchup),
        max_active_runs: patch
            .max_active_runs
            .unwrap_or_else(|| u32::try_from(existing.max_active_runs).unwrap_or(0)),
        // Not consulted by the shared update body (pause state is managed via
        // pause/resume) — carried through for completeness only.
        paused: existing.is_paused,
        queue_name: patch
            .queue_name
            .clone()
            .or_else(|| existing.queue_name.clone())
            .unwrap_or_else(|| "default".to_string()),
        jitter: patch.jitter.unwrap_or_else(|| {
            Duration::from_secs(u64::try_from(existing.jitter_secs).unwrap_or(0))
        }),
        overlap_policy: patch
            .overlap_policy
            .unwrap_or_else(|| OverlapPolicy::from_db(&existing.overlap_policy)),
        buffer_all_max: patch
            .buffer_all_max
            .unwrap_or_else(|| u32::try_from(existing.buffer_all_max).unwrap_or(0)),
        // Not persisted on harvest_schedules; nothing to preserve.
        execution_timeout: None,
        calendar: patch
            .calendar
            .as_ref()
            .map_or_else(|| existing.calendar_name.clone(), Clone::clone),
        skip_policy: patch
            .skip_policy
            .unwrap_or_else(|| crate::policy::SkipPolicy::from_db(&existing.skip_policy)),
        consecutive_failure_limit: patch.consecutive_failure_limit.unwrap_or_else(|| {
            existing
                .consecutive_failure_limit
                .map(|n| u32::try_from(n).unwrap_or(0))
        }),
        end_at: patch.end_at.unwrap_or(existing.end_at),
        // Normalize 0 → None on both arms: callers passing max_runs=0 intend
        // "no limit" (mirrors the create path).
        max_runs: patch.max_runs.map_or_else(
            || {
                existing
                    .max_runs
                    .and_then(|n| u32::try_from(n).ok())
                    .filter(|&n| n > 0)
            },
            |new| new.filter(|&n| n > 0),
        ),
        catchup_policy: patch.catchup_policy.unwrap_or(existing_catchup_policy),
        retry_policy: merged_retry_policy,
    })
}

/// Apply a partial in-place update to an existing workflow schedule row,
/// keyed by its `harvest_schedules.id` (issue #771).
///
/// The row's identity (`schedule_id`) is never changed — #488 carryover
/// lineage, the `sched:{schedule_id}:…` workflow-id namespace, and #534 run
/// history all keep resolving. Pause state, pause metadata, `runs_started`,
/// `last_run_at`, and `consecutive_failure_count` are preserved.
///
/// The merged spec is validated before any write and the whole operation
/// runs in a single transaction, so a rejected patch writes nothing.
/// `next_run_at` is recomputed anchored at now only when the effective
/// schedule expression actually changed (mirroring the registration upsert);
/// otherwise the pending value is preserved.
///
/// # Errors
///
/// Returns [`HarvestError::Config`] when the merged spec fails validation
/// (invalid cron expression, unknown timezone, zero interval) and
/// [`HarvestError::Database`] on connection/query failure. Row-state
/// conditions (missing row, DAG row, live fire claim) are reported through
/// [`ScheduleUpdateOutcome`], not errors.
pub async fn update_workflow_schedule(
    conn: &mut AsyncPgConnection,
    schedule_id: uuid::Uuid,
    patch: &WorkflowSchedulePatch,
) -> HarvestResult<ScheduleUpdateOutcome> {
    use crate::schema::harvest_schedules::dsl;
    use diesel_async::AsyncConnection;
    use scoped_futures::ScopedFutureExt;

    conn.transaction(|conn| {
        async move {
            // FOR UPDATE: serialize this PATCH against a concurrent scheduler
            // tick (whose claim/advance/finalize UPDATEs take the row lock)
            // and against peer PATCHes. Without the lock, a full
            // claim→advance→clear-claim cycle could commit between this
            // SELECT and the guarded UPDATE below, and the merge would write
            // back the *stale* `next_run_at` (re-arming an already-fired slot
            // → double fire), stale `buffered_runs`, and a stale `runs_started`
            // basis for the #478 exhaustion reconciliation; two concurrent
            // PATCHes would likewise silently lose one side's fields.
            let existing: Option<HarvestSchedule> = dsl::harvest_schedules
                .find(schedule_id)
                .select(HarvestSchedule::as_select())
                .for_update()
                .first(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?;
            let Some(existing) = existing else {
                return Ok(ScheduleUpdateOutcome::NotFound);
            };
            if existing.dag_name.is_some() || existing.workflow_name.is_none() {
                return Ok(ScheduleUpdateOutcome::DagSchedule);
            }

            // Validate the MERGED spec (e.g. new cron + existing timezone)
            // before any write; a Config error rolls the transaction back.
            let merged = merge_schedule_patch(&existing, patch)?;
            crate::policy::validate_schedule(&merged.schedule).map_err(HarvestError::Config)?;

            match apply_workflow_schedule_update(conn, &merged, &existing, true).await? {
                AppliedScheduleUpdate::Updated(row) => Ok(ScheduleUpdateOutcome::Updated(row)),
                AppliedScheduleUpdate::SkippedLiveClaim => {
                    // 0 rows can also mean the row vanished between our SELECT
                    // and UPDATE (concurrent delete) — distinguish it.
                    let still_exists: bool = diesel::select(diesel::dsl::exists(
                        dsl::harvest_schedules.find(schedule_id),
                    ))
                    .get_result(conn)
                    .await
                    .map_err(crate::error::database_error)?;
                    if still_exists {
                        Ok(ScheduleUpdateOutcome::ClaimLive)
                    } else {
                        Ok(ScheduleUpdateOutcome::NotFound)
                    }
                }
            }
        }
        .scope_boxed()
    })
    .await
}

/// Derive a deterministic, idempotent `workflow_id` for a scheduled run.
///
/// The id is stable across retries: if the scheduler ticks twice before
/// updating `last_run_at`, `RejectDuplicate` reports the already-created
/// execution and the scheduler treats that slot as dispatched.
fn scheduled_workflow_id(
    schedule_id: uuid::Uuid,
    workflow_name: &str,
    scheduled_for: DateTime<Utc>,
) -> String {
    let micros = scheduled_for.timestamp_subsec_micros();
    if micros == 0 {
        format!(
            "sched:{}:{}:{}",
            schedule_id,
            workflow_name,
            scheduled_for.timestamp()
        )
    } else {
        format!(
            "sched:{}:{}:{}.{:06}",
            schedule_id,
            workflow_name,
            scheduled_for.timestamp(),
            micros
        )
    }
}

/// Public re-export of `scheduled_workflow_id` for use in the backfill handler.
#[must_use]
pub fn scheduled_workflow_id_pub(
    schedule_id: uuid::Uuid,
    workflow_name: &str,
    scheduled_for: DateTime<Utc>,
) -> String {
    scheduled_workflow_id(schedule_id, workflow_name, scheduled_for)
}

/// Public re-export of `next_run_after` for use in [`crate::calendar`] preview helpers.
#[must_use]
pub fn next_run_after_pub(
    schedule: Option<&Schedule>,
    reference: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    next_run_after(schedule, reference)
}

const fn scheduled_workflow_reuse_policy() -> WorkflowIdReusePolicy {
    WorkflowIdReusePolicy::RejectDuplicate
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ScheduledStartOutcome {
    Created { exec_id: ExecutionId, state: String },
    Duplicate { exec_id: ExecutionId, state: String },
}

impl ScheduledStartOutcome {
    const fn created(&self) -> bool {
        matches!(self, Self::Created { .. })
    }

    const fn exec_id(&self) -> ExecutionId {
        match self {
            Self::Created { exec_id, .. } | Self::Duplicate { exec_id, .. } => *exec_id,
        }
    }

    fn state(&self) -> &str {
        match self {
            Self::Created { state, .. } | Self::Duplicate { state, .. } => state,
        }
    }
}

fn scheduled_start_outcome(
    result: HarvestResult<StartedWorkflowExecution>,
) -> HarvestResult<ScheduledStartOutcome> {
    match result {
        Ok(started) if started.created => Ok(ScheduledStartOutcome::Created {
            exec_id: started.exec_id,
            state: started.state,
        }),
        Ok(started) => Ok(ScheduledStartOutcome::Duplicate {
            exec_id: started.exec_id,
            state: started.state,
        }),
        Err(HarvestError::AlreadyExists {
            existing_exec_id,
            existing_state,
        }) => Ok(ScheduledStartOutcome::Duplicate {
            exec_id: existing_exec_id,
            state: existing_state,
        }),
        Err(error) => Err(error),
    }
}

/// Process due workflow-schedule rows and dispatch workflow starts.
/// Parse a stored `schedule_expr` string back into a [`Schedule`] variant.
///
/// The format written by `schedule_expr` is `"cron:<expr>"`, `"interval:<secs>"`,
/// or `"manual"`. Unrecognised strings return `None` and the row is treated as
/// `Schedule::Manual` (no automatic `next_run_at`).
#[must_use]
pub fn parse_schedule_from_expr_pub(expr: &str) -> Option<Schedule> {
    parse_schedule_from_expr(expr)
}

fn parse_schedule_from_expr(expr: &str) -> Option<Schedule> {
    if let Some(rest) = expr.strip_prefix("cron_tz:") {
        // Format: "cron_tz:<tz>:<expr>" where <tz> is an IANA name that may
        // contain one colon (e.g. nothing — IANA names use '/'). We split on
        // the first colon after the prefix to separate tz from the cron expr.
        let (tz, cron_expr) = rest.split_once(':')?;
        return Some(Schedule::CronInTimezone {
            expr: cron_expr.to_string(),
            tz: tz.to_string(),
        });
    }
    expr.strip_prefix("cron:").map_or_else(
        || {
            expr.strip_prefix("interval:")
                .and_then(|s| s.parse::<u64>().ok())
                .map(|secs| Schedule::Interval(Duration::from_secs(secs)))
        },
        |cron| Some(Schedule::Cron(cron.to_string())),
    )
}

#[allow(clippy::too_many_lines)]
async fn tick_workflow_schedules(
    conn: &mut AsyncPgConnection,
    current_shard: ShardId,
    registered_dags: &DagCatalog,
    registry: &crate::worker::HandlerRegistry,
    metrics: &Arc<dyn crate::telemetry::MetricsRecorder>,
    active_gates: &[crate::admission_gate::AdmissionGate],
) -> HarvestResult<()> {
    use crate::schema::harvest_schedules::dsl;

    let now = Utc::now();

    let due: Vec<HarvestSchedule> = dsl::harvest_schedules
        .filter(dsl::workflow_name.is_not_null())
        .filter(dsl::is_paused.eq(false))
        // Auto-paused schedules (issue #360) are excluded from the due list.
        .filter(dsl::auto_paused_at.is_null())
        // Exhausted schedules (issue #478) are permanently terminal — never re-fire.
        .filter(dsl::exhausted_at.is_null())
        .filter(dsl::next_run_at.is_not_null())
        .filter(dsl::next_run_at.le(now))
        .order(dsl::next_run_at.asc())
        .select(HarvestSchedule::as_select())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    for schedule in due {
        let Some(ref wf_name) = schedule.workflow_name else {
            continue;
        };

        if let Some(ref dag_name) = schedule.dag_name
            && !registered_dags.contains_key(dag_name)
        {
            tracing::info!(
                workflow_name = %wf_name,
                dag_name = %dag_name,
                "harvest DAG workflow schedule skipped: DAG is no longer registered"
            );
            metrics.record_schedule_skipped("dag", dag_name, "dag_not_registered");
            crate::schedule_decision::record_decision_graceful(
                conn,
                Some(&**metrics),
                Some(schedule.id),
                dag_name,
                "dag",
                "skipped",
                "dag_not_registered",
                Some(serde_json::json!({
                    "workflow_name": wf_name,
                })),
                now,
                now,
                i16::try_from(current_shard.as_i32()).unwrap_or(0),
            )
            .await;
            diesel::update(dsl::harvest_schedules.find(schedule.id))
                .set((
                    dsl::next_run_at.eq(Option::<DateTime<Utc>>::None),
                    dsl::updated_at.eq(now),
                ))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
            continue;
        }

        claim_and_fire_workflow_schedule(
            conn,
            &schedule,
            now,
            current_shard,
            registered_dags,
            registry,
            metrics,
            active_gates,
        )
        .await?;
    }

    Ok(())
}

/// Claim one due workflow-schedule slot (issue #350 HA claim) and fire it from
/// a **fresh post-claim read** of the row (issue #771 AC7).
///
/// `snapshot` is the row as loaded by the caller's due-list SELECT and may be
/// stale by the time the claim is attempted: an in-place edit
/// (`PATCH /admin/schedules/{id}`, issue #771) that does not rewrite
/// `next_run_at` — an input/queue/policy-only edit — can commit between the
/// due-list SELECT and the claim UPDATE without invalidating the claim's
/// `next_run_at` guard. Firing from the pre-claim snapshot would then dispatch
/// the slot with the pre-edit input/queue/policies. So after winning the claim
/// this function re-reads the row and re-derives everything the fire consumes
/// (input, queue, parsed schedule expression, catchup/overlap/retry policies)
/// from the fresh row. Edits arriving *after* the claim are fenced by the
/// PATCH's own live-claim guard (`ClaimLive` → 409), so the fresh row is
/// authoritative for the whole fire. Cadence edits (which rewrite
/// `next_run_at`) were already fenced by the claim's `next_run_at` guard.
///
/// This mirrors the pre-existing fresh re-read of `consecutive_failure_count`
/// / `auto_paused_at` inside `tick_one_workflow_schedule`, which exists for
/// exactly this stale-snapshot bug class (issue #360 / Codex #1928).
///
/// Exposed `#[doc(hidden)] pub` so the stale-snapshot race can be driven
/// deterministically from integration tests (a test hands in a deliberately
/// stale snapshot while the DB row already carries the edited values).
///
/// # Errors
///
/// Returns [`HarvestError::Database`] when the claim UPDATE or the post-claim
/// re-read fails; a `tick_one_workflow_schedule` failure is logged and the
/// claim released (matching the caller's pre-existing continue-to-next-schedule
/// semantics) rather than propagated.
#[doc(hidden)]
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub async fn claim_and_fire_workflow_schedule(
    conn: &mut AsyncPgConnection,
    snapshot: &HarvestSchedule,
    now: DateTime<Utc>,
    current_shard: ShardId,
    registered_dags: &DagCatalog,
    registry: &crate::worker::HandlerRegistry,
    metrics: &Arc<dyn crate::telemetry::MetricsRecorder>,
    active_gates: &[crate::admission_gate::AdmissionGate],
) -> HarvestResult<()> {
    use crate::schema::harvest_schedules::dsl;

    let Some(snapshot_wf_name) = snapshot.workflow_name.as_deref() else {
        return Ok(());
    };
    let Some(logical_date) = snapshot.next_run_at else {
        return Ok(());
    };

    // ── HA claim (issue #350) ─────────────────────────────────────────────
    // Atomically claim this due slot so concurrent replicas in a multi-
    // replica deployment never double-fire the same schedule.
    //
    // The UPDATE guards on both the claim expiry (fire_claim_token IS NULL
    // OR fire_claimed_until < NOW()) AND the logical slot (next_run_at =
    // logical_date). The next_run_at guard prevents a stale-snapshot race:
    // if a peer has already fired this slot and advanced next_run_at, our
    // claim UPDATE matches zero rows and we skip cleanly.
    //
    // We generate the token client-side so we can reference it in the error
    // cleanup path — preventing a slow late-running tick from clearing a
    // successor replica's live claim after the 30 s TTL has expired.
    //
    // Crash-recovery window: if this replica crashes after claiming but
    // before advancing next_run_at, the claim expires after 30 s and any
    // healthy peer retries the slot on its next tick.
    let my_claim_token = uuid::Uuid::new_v4();
    let claim_rows_affected: usize = diesel::sql_query(
        "UPDATE harvest_schedules \
         SET fire_claim_token = $1, \
             fire_claimed_until = NOW() + INTERVAL '30 seconds' \
         WHERE id = $2 \
           AND next_run_at = $3 \
           AND (fire_claim_token IS NULL OR fire_claimed_until < NOW())",
    )
    .bind::<diesel::sql_types::Uuid, _>(my_claim_token)
    .bind::<diesel::sql_types::Uuid, _>(snapshot.id)
    .bind::<diesel::sql_types::Timestamptz, _>(logical_date)
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;

    if claim_rows_affected == 0 {
        metrics.record_schedule_fire_attempt(snapshot_wf_name, "lost_race");
        tracing::debug!(
            schedule_id = %snapshot.id,
            workflow_name = %snapshot_wf_name,
            "harvest: schedule slot claim lost to peer replica; skipping this tick"
        );
        return Ok(());
    }
    metrics.record_schedule_fire_attempt(snapshot_wf_name, "claimed");

    // ── Post-claim refresh (issue #771 AC7) ───────────────────────────────
    // Never fire from the pre-claim snapshot: re-read the row now that the
    // claim is held so a non-cadence edit that committed between the due-list
    // SELECT and the claim is picked up by this very fire.
    let fresh: Option<HarvestSchedule> = dsl::harvest_schedules
        .find(snapshot.id)
        .select(HarvestSchedule::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?;
    let Some(schedule) = fresh else {
        // The row was deleted between the claim and the re-read; the claim
        // died with the row and there is nothing to fire or release.
        return Ok(());
    };
    let schedule = &schedule;
    let Some(ref wf_name) = schedule.workflow_name else {
        // Defensive only: workflow_name is immutable through every mutation
        // path (the PATCH route rejects it as not editable).
        return Ok(());
    };
    // Parse the schedule expression stored in the DB row. This covers both
    // in-process registered schedules and schedules created via the API
    // (which are DB-only and do not appear in the in-memory list).
    let parsed_schedule = schedule
        .schedule_expr
        .as_deref()
        .and_then(parse_schedule_from_expr);
    let catchup_policy = crate::policy::CatchupPolicy::from_db(
        schedule.catchup_policy.as_deref(),
        schedule.catchup_window_secs,
        schedule.catchup,
    );

    // issue #377: gate check runs AFTER the HA claim to prevent every
    // replica from independently recording a skip metric and advancing
    // next_run_at for the same slot. Only the replica that wins the claim
    // skips or fires the slot.
    {
        let queue_name = schedule.queue_name.as_deref().unwrap_or("default");
        // Use dag_name (not wf_name) when looking up DAG metadata so that an
        // owner-scoped gate is matched even when the workflow name differs from
        // the DAG name (e.g. unified DAG aliases).
        let dag_lookup_key = schedule.dag_name.as_deref().unwrap_or(wf_name.as_str());
        let owner = registry
            .workflows
            .get(wf_name.as_str())
            .and_then(|i| i.owner)
            .or_else(|| {
                registered_dags
                    .get(dag_lookup_key)
                    .and_then(|d| d.owner.as_deref())
            });
        if let Some(gate) = crate::admission_gate::check_admission(
            active_gates,
            wf_name,
            queue_name,
            current_shard.as_i32(),
            owner,
        ) {
            let gate_id_str = gate.id.to_string();
            tracing::info!(
                workflow_name = %wf_name,
                gate_id = %gate_id_str,
                reason = %gate.reason,
                "harvest: schedule fire skipped due to admission gate"
            );
            metrics.record_schedule_skipped("workflow", wf_name, "admission_blocked");
            // issue #618, F-round17: the scheduler is a *gated* producer, so a
            // schedule fire blocked by an active gate must ALSO appear in
            // harvest.admission.blocked (the "zero-uncounted gated producer" contract),
            // in addition to the schedule-domain schedule_skipped signal above. Pass
            // the matched gate's scope kind + reason, mirroring every other gated
            // producer's block-count call.
            metrics.record_admission_blocked(gate.scope.kind_str(), &gate.reason);
            crate::schedule_decision::record_decision_graceful(
                conn,
                Some(&**metrics),
                Some(schedule.id),
                wf_name,
                "workflow",
                "skipped",
                "admission_blocked",
                Some(serde_json::json!({
                    "gate_id": gate_id_str,
                    "reason": gate.reason,
                })),
                now,
                now,
                i16::try_from(current_shard.as_i32()).unwrap_or(0),
            )
            .await;
            // Advance next_run_at and clear the claim token so the next
            // tick can re-claim this schedule immediately. Without clearing
            // the token, the claim would block other replicas for up to the
            // full 30 s TTL before they could re-examine the slot.
            let next_run = next_run_after(parsed_schedule.as_ref(), now);
            let _ = diesel::sql_query(
                "UPDATE harvest_schedules \
                 SET next_run_at = $1, \
                     updated_at = $2, \
                     fire_claim_token = NULL, \
                     fire_claimed_until = NULL \
                 WHERE id = $3 AND fire_claim_token = $4",
            )
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>, _>(next_run)
            .bind::<diesel::sql_types::Timestamptz, _>(now)
            .bind::<diesel::sql_types::Uuid, _>(schedule.id)
            .bind::<diesel::sql_types::Uuid, _>(my_claim_token)
            .execute(conn)
            .await;
            return Ok(());
        }
    }

    if let Err(error) = tick_one_workflow_schedule(
        conn,
        wf_name,
        catchup_policy,
        parsed_schedule.as_ref(),
        schedule,
        logical_date,
        now,
        current_shard,
        my_claim_token,
        registered_dags,
        registry,
        metrics,
    )
    .await
    {
        tracing::warn!(
            error = %error, workflow_name = %wf_name,
            "harvest: workflow schedule tick failed; continuing to next schedule"
        );
        // Clear our own claim on error so a peer can retry promptly. Guard
        // on the token so a slow late-running tick doesn't clear a
        // successor's live claim if the 30 s TTL has already expired.
        let _ = diesel::sql_query(
            "UPDATE harvest_schedules \
             SET fire_claim_token = NULL, fire_claimed_until = NULL \
             WHERE id = $1 AND fire_claim_token = $2",
        )
        .bind::<diesel::sql_types::Uuid, _>(schedule.id)
        .bind::<diesel::sql_types::Uuid, _>(my_claim_token)
        .execute(conn)
        .await;
    }

    Ok(())
}

/// Cancel the oldest scheduled RUNNING executions for `workflow_name` under `schedule_id`,
/// up to `max_to_cancel`.
///
/// Filters by `schedule_id` rather than the `sched:` workflow-id prefix so that workflow-retry
/// executions (which carry a UUID `workflow_id` but still link back to the originating schedule via
/// the `schedule_id` FK) are included, while operator-triggered manual runs (which have
/// `schedule_id = NULL`) are not inadvertently cancelled.
/// Orders by `started_at ASC` so the oldest executions are cancelled first.
#[cfg(feature = "db")]
async fn cancel_in_flight_runs(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    schedule_id: uuid::Uuid,
    reason: &str,
    max_to_cancel: u32,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> HarvestResult<u32> {
    use crate::execution::cancel_workflow_execution;

    let running_ids: Vec<uuid::Uuid> =
        harvest_workflow_executions::table
            .filter(harvest_workflow_executions::workflow_name.eq(workflow_name))
            .filter(harvest_workflow_executions::schedule_id.eq(Some(schedule_id)))
            .filter(harvest_workflow_executions::state.eq_any(["RUNNING", "PAUSED"]))
            // Exclude manual-trigger runs: attributing them to the schedule (issue #534)
            // must not make them targets for automatic overlap-cleanup. Scheduled and
            // backfill runs remain eligible; NULL origin (pre-migration) is included for
            // backward compatibility.
            .filter(harvest_workflow_executions::origin.is_null().or(
                harvest_workflow_executions::origin.ne(crate::execution::ORIGIN_MANUAL_TRIGGER),
            ))
            .order(harvest_workflow_executions::started_at.asc())
            .select(harvest_workflow_executions::id)
            .load(conn)
            .await
            .map_err(crate::error::database_error)?;

    let mut count: u32 = 0;
    for raw_id in running_ids
        .into_iter()
        .take(usize::try_from(max_to_cancel).unwrap_or(usize::MAX))
    {
        let exec_id = ExecutionId::from_uuid(raw_id);
        match cancel_workflow_execution(conn, exec_id, reason, metrics).await {
            Ok(_) => count += 1,
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    exec_id = %exec_id,
                    "harvest: CancelOther could not cancel in-flight execution; skipping"
                );
            }
        }
    }
    Ok(count)
}

/// Terminate the oldest scheduled RUNNING executions for `workflow_name` under `schedule_id`,
/// up to `max_to_terminate`.
///
/// Filters by `schedule_id` (same rationale as `cancel_in_flight_runs`) so workflow-retry
/// executions are included and manual-trigger runs (`schedule_id` = NULL) are excluded.
/// Orders by `started_at ASC` so the oldest executions are terminated first.
#[cfg(feature = "db")]
async fn terminate_in_flight_runs(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    schedule_id: uuid::Uuid,
    reason: &str,
    max_to_terminate: u32,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> HarvestResult<u32> {
    use crate::execution::terminate_workflow_execution;

    let active_ids: Vec<uuid::Uuid> =
        harvest_workflow_executions::table
            .filter(harvest_workflow_executions::workflow_name.eq(workflow_name))
            .filter(harvest_workflow_executions::schedule_id.eq(Some(schedule_id)))
            .filter(harvest_workflow_executions::state.eq_any(["RUNNING", "PAUSED"]))
            .filter(harvest_workflow_executions::origin.is_null().or(
                harvest_workflow_executions::origin.ne(crate::execution::ORIGIN_MANUAL_TRIGGER),
            ))
            .order(harvest_workflow_executions::started_at.asc())
            .select(harvest_workflow_executions::id)
            .load(conn)
            .await
            .map_err(crate::error::database_error)?;

    let mut count: u32 = 0;
    for raw_id in active_ids
        .into_iter()
        .take(usize::try_from(max_to_terminate).unwrap_or(usize::MAX))
    {
        let exec_id = ExecutionId::from_uuid(raw_id);
        match terminate_workflow_execution(conn, exec_id, reason, metrics).await {
            Ok(_) => count += 1,
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    exec_id = %exec_id,
                    "harvest: TerminateOther could not terminate in-flight execution; skipping"
                );
            }
        }
    }
    Ok(count)
}

/// Emit the bounded-catchup drop audit (skip metrics + one aggregated decision
/// row + the `last_catchup_dropped`/`last_catchup_at` recovery columns) for a
/// recovery tick that dropped `dropped` missed slots.
///
/// Call this **only** on a path that commits an advanced `next_run_at` past the
/// dropped slots (overlap Drop/Buffer early-returns and the dispatch finalize),
/// never before a fallible dispatch — otherwise a transient failure that leaves
/// `next_run_at` unchanged would record drops that are then re-recorded when the
/// same slots are retried (issue #484 / Codex #2174). No-op when `dropped == 0`,
/// so ordinary (non-recovery) ticks never touch the recovery audit columns.
#[allow(clippy::too_many_arguments)]
async fn record_catchup_drops(
    conn: &mut AsyncPgConnection,
    metrics: &Arc<dyn crate::telemetry::MetricsRecorder>,
    schedule_id: uuid::Uuid,
    wf_name: &str,
    catchup_policy: crate::policy::CatchupPolicy,
    catchup_window_secs: Option<i64>,
    dropped: u64,
    now: DateTime<Utc>,
    current_shard: ShardId,
    claim_token: uuid::Uuid,
) {
    use crate::schema::harvest_schedules::dsl;

    if dropped == 0 {
        return;
    }
    // Count every dropped slot exactly with a single batched increment — no
    // O(dropped) loop, so a large recovery never stalls the tick and the counter
    // does not under-report the outage (issue #484 / Codex #1837).
    metrics.record_schedule_skipped_n("workflow", wf_name, "catchup_window_exceeded", dropped);
    crate::schedule_decision::record_decision_graceful(
        conn,
        Some(&**metrics),
        Some(schedule_id),
        wf_name,
        "workflow",
        "skipped",
        "catchup_window_exceeded",
        Some(serde_json::json!({
            "catchup_policy": catchup_policy.as_str(),
            "catchup_window_secs": catchup_window_secs,
            "dropped": dropped,
        })),
        now,
        now,
        i16::try_from(current_shard.as_i32()).unwrap_or(0),
    )
    .await;

    // Persist the catchup-drop summary as a separate, conditional update — NOT
    // part of the main finalize update — so ordinary (zero-drop) ticks never
    // reset this recovery audit trail back to 0 / NULL. Guarded by the HA claim
    // token (like the final update) so a stale tick that lost its claim cannot
    // stamp obsolete recovery audit fields over a successor replica's row
    // (issue #484 / Codex #2297).
    let _ = diesel::update(
        dsl::harvest_schedules
            .find(schedule_id)
            .filter(dsl::fire_claim_token.eq(Some(claim_token))),
    )
    .set((
        dsl::last_catchup_dropped.eq(i32::try_from(dropped).unwrap_or(i32::MAX)),
        dsl::last_catchup_at.eq(Some(now)),
    ))
    .execute(conn)
    .await;
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn tick_one_workflow_schedule(
    conn: &mut AsyncPgConnection,
    wf_name: &str,
    catchup_policy: crate::policy::CatchupPolicy,
    parsed_schedule: Option<&Schedule>,
    schedule: &HarvestSchedule,
    logical_date: DateTime<Utc>,
    now: DateTime<Utc>,
    current_shard: ShardId,
    claim_token: uuid::Uuid,
    registered_dags: &DagCatalog,
    registry: &crate::worker::HandlerRegistry,
    metrics: &Arc<dyn crate::telemetry::MetricsRecorder>,
) -> HarvestResult<()> {
    use crate::execution::StartWorkflowParams;
    use crate::schema::harvest_schedules::dsl;

    let catchup = catchup_policy.is_catchup_enabled();

    // Re-read the auto-pause inputs (consecutive_failure_count, auto_paused_at)
    // under the HA claim up front. A concurrent worker completion can reset the
    // failure counter or set auto_paused_at after this tick loaded the schedule
    // row, so the pre-plan short-circuit below and the auto-pause guard further
    // down MUST consult the same *fresh* values — otherwise the short-circuit
    // could empty the plan off a stale "at limit" snapshot while the guard (using
    // the reset count) declines to auto-pause, silently stopping the schedule
    // with next_run_at = NULL (issue #360 / Codex #1928).
    let (db_failure_count, db_auto_paused_at): (i32, Option<DateTime<Utc>>) =
        dsl::harvest_schedules
            .find(schedule.id)
            .select((dsl::consecutive_failure_count, dsl::auto_paused_at))
            .first(conn)
            .await
            .map_err(crate::error::database_error)?;

    // Compute the bounded catchup plan up front from the *original* oldest
    // overdue slot, then rebind `logical_date` to the earliest slot the policy
    // actually intends to fire. This makes the calendar suppression, overlap
    // buffering, jitter window, and dispatch logic below all operate on a
    // policy-approved slot rather than the oldest missed one (issue #484:
    // bounded policies must be applied *before* those branches, not after).
    //
    // For SkipAll / Unbounded / legacy `catchup` schedules, `run_dates.first()`
    // is exactly the original `logical_date`, so their behavior is completely
    // unchanged — only the new MostRecent / Window policies shift the anchor.
    // Skip materializing the full overdue backlog for an Unbounded / legacy
    // `catchup = true` schedule that one of the guards below will immediately
    // exhaust, auto-pause, or otherwise return on without firing: a high-
    // frequency schedule left down past its cutoff / run budget (end_at /
    // max_runs, issue #478) or past its consecutive-failure limit (auto-pause,
    // issue #360) would otherwise allocate one slot per missed interval only for
    // every slot to be discarded (Codex #1829 / #1938). The authoritative
    // exhaustion / auto-pause decision is still made (and re-validated against a
    // fresh DB read) by those guards below — this only avoids the wasted
    // allocation. The bounded MostRecent / Window policies compute in O(1)
    // (closed-form for interval schedules), so this short-circuit is scoped to
    // the unbounded path.
    let will_exhaust_before_firing =
        matches!(catchup_policy, crate::policy::CatchupPolicy::Unbounded)
            && (schedule.end_at.is_some_and(|end_at| logical_date >= end_at)
                || schedule
                    .max_runs
                    .is_some_and(|max_runs| max_runs > 0 && schedule.runs_started >= max_runs)
                || db_auto_paused_at.is_some()
                || schedule
                    .consecutive_failure_limit
                    .is_some_and(|limit| limit > 0 && db_failure_count >= limit));
    let catchup_plan = if will_exhaust_before_firing {
        // Empty plan: `logical_date` rebinding below keeps the original slot
        // (the `[]` arm), so the calendar / jitter / exhaustion branches see the
        // exact same `logical_date` they would have with the full plan.
        CatchupPlan {
            run_dates: Vec::new(),
            next_run_at: None,
            dropped: 0,
        }
    } else {
        catchup_run_plan(
            parsed_schedule,
            logical_date,
            now,
            catchup_policy,
            schedule.end_at,
        )
    };
    // Slice pattern rather than `.first()` to avoid colliding with diesel's
    // `RunQueryDsl::first` brought into scope by the schema `dsl` import.
    let logical_date = match catchup_plan.run_dates.as_slice() {
        [earliest, ..] => *earliest,
        [] => logical_date,
    };

    // Compute jitter window once so it can be reused in the dispatch loop below.
    let jitter_window =
        std::time::Duration::from_secs(u64::try_from(schedule.jitter_secs.max(0)).unwrap_or(0));

    // If jitter for this slot has not yet elapsed, skip the overlap check and any
    // dispatch; the scheduler will revisit on the next tick (next_run_at unchanged).
    {
        let jitter_offset = compute_jitter_offset(schedule.id, logical_date, jitter_window);
        let effective_fire_time =
            logical_date + chrono::Duration::from_std(jitter_offset).unwrap_or_default();
        if now < effective_fire_time {
            // Release the HA claim so peers are not blocked for the full 30 s
            // TTL while the jitter window elapses. The next tick re-claims
            // once the effective fire time has passed. Guard by token so a
            // slow late-running tick cannot clear a successor's live claim.
            diesel::update(
                dsl::harvest_schedules
                    .find(schedule.id)
                    .filter(dsl::fire_claim_token.eq(Some(claim_token))),
            )
            .set((
                dsl::fire_claim_token.eq(Option::<uuid::Uuid>::None),
                dsl::fire_claimed_until.eq(Option::<DateTime<Utc>>::None),
            ))
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;
            return Ok(());
        }
    }

    // ── Calendar check ────────────────────────────────────────────────────────
    // If the schedule has a named calendar, load its exclusion dates and apply
    // the skip policy to the logical fire date. Suppressed firings (SkipPolicy::Skip)
    // are recorded as skipped with `reason = "calendar"` and the scheduler advances
    // past this slot. Deferred firings proceed with the adjusted date.
    //
    // `excluded` and `exclude_weekends` are kept for re-use in the dispatch loop
    // below so that individual catchup slots are also calendar-filtered.
    let (calendar_excluded, calendar_exclude_weekends, calendar_skip_policy) = if let Some(
        ref cal_name,
    ) =
        schedule.calendar_name
    {
        let excluded = crate::calendar::load_exclusions_for_calendar(conn, cal_name)
                .await
                .map_err(|e| {
                    tracing::error!(
                        workflow_name = %wf_name,
                        calendar = %cal_name,
                        error = %e,
                        "harvest: failed to load calendar exclusions; aborting tick to preserve calendar guarantees"
                    );
                    e
                })?;
        let exclude_weekends = crate::calendar::calendar_excludes_weekends(cal_name);
        let skip_policy = crate::policy::SkipPolicy::from_db(&schedule.skip_policy);

        let fire_date = logical_date.date_naive();
        match crate::calendar::apply_skip_policy(
            fire_date,
            skip_policy,
            &excluded,
            exclude_weekends,
        ) {
            None if catchup_plan.run_dates.len() <= 1 => {
                // The earliest (and only) planned slot is calendar-excluded, so
                // there is nothing else to fire this tick: suppress and advance.
                // For catchup schedules advance to the next slot after the
                // excluded date so overdue non-excluded slots are not dropped; for
                // non-catchup schedules advance from now.
                //
                // A *multi-slot* catchup plan (Window, or Unbounded) is handled by
                // the `None` fall-through arm below instead: aborting the whole
                // tick here would strand the later in-window allowed slots, so the
                // per-slot calendar filter in the dispatch loop skips the excluded
                // slot(s) and fires the allowed ones (issue #484 / Codex #1867).
                tracing::info!(
                    workflow_name = %wf_name,
                    calendar = %cal_name,
                    fire_date = %fire_date,
                    "harvest: workflow schedule firing suppressed by calendar"
                );
                metrics.record_schedule_skipped("workflow", wf_name, "calendar");
                // This branch advances next_run_at past the single policy-approved
                // slot, so any older slots the bounded policy already dropped are
                // now durably discarded: record their catchup-drop audit here
                // (committing path) so a MostRecent / single-slot-Window recovery
                // whose kept slot lands on an excluded calendar date still emits the
                // `catchup_window_exceeded` decision + `last_catchup_*` summary
                // (issue #484 / Codex #2012).
                record_catchup_drops(
                    conn,
                    metrics,
                    schedule.id,
                    wf_name,
                    catchup_policy,
                    schedule.catchup_window_secs,
                    catchup_plan.dropped,
                    now,
                    current_shard,
                    claim_token,
                )
                .await;
                let next = if catchup {
                    next_run_after(parsed_schedule, logical_date)
                } else {
                    next_run_after(parsed_schedule, now)
                };
                crate::schedule_decision::record_decision_graceful(
                    conn,
                    Some(&**metrics),
                    Some(schedule.id),
                    wf_name,
                    "workflow",
                    "skipped",
                    "calendar",
                    Some(serde_json::json!({
                        "calendar": cal_name,
                        "fire_date": fire_date,
                    })),
                    now,
                    next.unwrap_or(now),
                    i16::try_from(current_shard.as_i32()).unwrap_or(0),
                )
                .await;
                diesel::update(
                    dsl::harvest_schedules
                        .find(schedule.id)
                        .filter(dsl::fire_claim_token.eq(Some(claim_token))),
                )
                .set((
                    dsl::next_run_at.eq(next),
                    dsl::fire_claim_token.eq(Option::<uuid::Uuid>::None),
                    dsl::fire_claimed_until.eq(Option::<DateTime<Utc>>::None),
                    dsl::updated_at.eq(now),
                ))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
                return Ok(());
            }
            None => {
                // Multi-slot catchup plan whose earliest slot is calendar-excluded:
                // do not abort the tick. Fall through so the dispatch loop applies
                // per-slot calendar filtering (skipping excluded slots and firing
                // the later allowed ones in the same bounded window).
            }
            Some(_adjusted) => {
                // Firing proceeds on `_adjusted` day. Do NOT rebase `logical_date`
                // here — `due_run_plan` must start from the original slot so that
                // catchup planning covers the full overdue window. Per-slot
                // adjustment happens in the dispatch loop below for every slot
                // (including this first one) via `effective_scheduled_for`.
            }
        }
        (excluded, exclude_weekends, skip_policy)
    } else {
        (vec![], false, crate::policy::SkipPolicy::Skip)
    };

    // ── Auto-pause check (issue #360) ─────────────────────────────────────────
    // Uses `db_auto_paused_at` / `db_failure_count` read under the HA claim at the
    // top of the tick (the same fresh values the pre-plan short-circuit consulted),
    // so a concurrent worker completion that set auto_paused_at or reset the
    // failure counter cannot make the two disagree (issue #360 / Codex #1928).
    if db_auto_paused_at.is_some() {
        // Already auto-paused by a concurrent worker; release the claim and stop.
        diesel::update(
            dsl::harvest_schedules
                .find(schedule.id)
                .filter(dsl::fire_claim_token.eq(Some(claim_token))),
        )
        .set((
            dsl::fire_claim_token.eq(Option::<uuid::Uuid>::None),
            dsl::fire_claimed_until.eq(Option::<DateTime<Utc>>::None),
            dsl::updated_at.eq(now),
        ))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
        return Ok(());
    }

    // If the schedule has a non-zero consecutive_failure_limit and the fresh
    // counter has reached (or exceeded) that limit, auto-pause now.
    // A limit of 0 is treated as disabled (same as NULL).
    if let Some(limit) = schedule.consecutive_failure_limit
        && limit > 0
        && db_failure_count >= limit
    {
        tracing::info!(
            workflow_name = %wf_name,
            consecutive_failure_count = db_failure_count,
            consecutive_failure_limit = limit,
            "harvest: auto-pausing schedule after consecutive failures"
        );
        diesel::update(
            dsl::harvest_schedules
                .find(schedule.id)
                .filter(dsl::fire_claim_token.eq(Some(claim_token))),
        )
        .set((
            dsl::auto_paused_at.eq(Some(now)),
            dsl::fire_claim_token.eq(Option::<uuid::Uuid>::None),
            dsl::fire_claimed_until.eq(Option::<DateTime<Utc>>::None),
            dsl::updated_at.eq(now),
        ))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
        metrics.record_schedule_auto_paused(wf_name);
        return Ok(());
    }

    // ── Bounded-run checks (issue #478) ───────────────────────────────────────
    // Both checks run *after* the HA claim so only one replica records the
    // exhaustion decision and advances next_run_at.

    // Check 1 — absolute end-time cutoff.
    if let Some(end_at) = schedule.end_at
        && logical_date >= end_at
    {
        tracing::info!(
            workflow_name = %wf_name,
            logical_date = %logical_date,
            end_at = %end_at,
            "harvest: schedule end_at reached; transitioning to exhausted"
        );
        crate::schedule_decision::record_decision_graceful(
            conn,
            Some(&**metrics),
            Some(schedule.id),
            wf_name,
            "workflow",
            "skipped",
            "end_at_reached",
            Some(serde_json::json!({
                "end_at": end_at,
                "logical_date": logical_date,
            })),
            now,
            now,
            i16::try_from(current_shard.as_i32()).unwrap_or(0),
        )
        .await;
        diesel::update(
            dsl::harvest_schedules
                .find(schedule.id)
                .filter(dsl::fire_claim_token.eq(Some(claim_token))),
        )
        .set((
            dsl::exhausted_at.eq(Some(now)),
            dsl::exhausted_reason.eq(Some("end_at_reached")),
            dsl::next_run_at.eq(Option::<DateTime<Utc>>::None),
            dsl::fire_claim_token.eq(Option::<uuid::Uuid>::None),
            dsl::fire_claimed_until.eq(Option::<DateTime<Utc>>::None),
            dsl::updated_at.eq(now),
        ))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
        return Ok(());
    }

    // Check 2 — max_runs budget exhausted.
    if let Some(max_runs) = schedule.max_runs
        && max_runs > 0
        && schedule.runs_started >= max_runs
    {
        tracing::info!(
            workflow_name = %wf_name,
            runs_started = schedule.runs_started,
            max_runs,
            "harvest: schedule max_runs budget exhausted; transitioning to exhausted"
        );
        crate::schedule_decision::record_decision_graceful(
            conn,
            Some(&**metrics),
            Some(schedule.id),
            wf_name,
            "workflow",
            "skipped",
            "max_runs_exhausted",
            Some(serde_json::json!({
                "runs_started": schedule.runs_started,
                "max_runs": max_runs,
            })),
            now,
            now,
            i16::try_from(current_shard.as_i32()).unwrap_or(0),
        )
        .await;
        diesel::update(
            dsl::harvest_schedules
                .find(schedule.id)
                .filter(dsl::fire_claim_token.eq(Some(claim_token))),
        )
        .set((
            dsl::exhausted_at.eq(Some(now)),
            dsl::exhausted_reason.eq(Some("max_runs_exhausted")),
            dsl::next_run_at.eq(Option::<DateTime<Utc>>::None),
            dsl::fire_claim_token.eq(Option::<uuid::Uuid>::None),
            dsl::fire_claimed_until.eq(Option::<DateTime<Utc>>::None),
            dsl::updated_at.eq(now),
        ))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
        return Ok(());
    }

    // Reuse the plan computed at the top of the tick (which also rebound
    // `logical_date`); recomputing here from the rebound anchor would zero the
    // dropped count.
    //
    // The catchup-drop audit (skip metrics + decision row + `last_catchup_*`
    // columns) is NOT emitted here. It is emitted via `record_catchup_drops`
    // only on the paths that actually *commit* an advanced `next_run_at` past
    // the dropped slots — the overlap Drop/Buffer early-returns (issue #484 /
    // Codex #2297) and the dispatch finalize below. A transient
    // `start_or_load_workflow_execution` failure returns `Err` before the
    // finalize and leaves `next_run_at` unchanged, so those slots are retried on
    // the next tick; emitting the audit here (before the fallible dispatch) would
    // produce false/duplicated recovery data for slots that were not durably
    // dropped (issue #484 / Codex #2174).
    let CatchupPlan {
        run_dates,
        next_run_at: next_run_after_plan,
        dropped: catchup_slots_dropped,
    } = catchup_plan;

    // A bounded plan with nothing to fire (e.g. a Window where every eligible
    // slot fell outside the window, so they were all dropped) must still advance
    // next_run_at past the dropped backlog and record the drops exactly once.
    // Otherwise the overlap branch below would treat the stale rebound
    // `logical_date` (still pinned to the oldest overdue slot by the `[]` rebind
    // arm) as a runnable slot and — under Skip + catchup — retain
    // `next_run_at = logical_date`, re-auditing the same backlog on every tick
    // until capacity opens (issue #484 / Codex #1952).
    if run_dates.is_empty() {
        record_catchup_drops(
            conn,
            metrics,
            schedule.id,
            wf_name,
            catchup_policy,
            schedule.catchup_window_secs,
            catchup_slots_dropped,
            now,
            current_shard,
            claim_token,
        )
        .await;
        // Mirror the dispatch finalize's end_at exhaustion: if the next planned
        // slot is at/after end_at (or there is none), no future fire can ever be
        // legal, so mark the schedule terminal here instead of leaving it active
        // until a future due tick that can never fire — or indefinitely when
        // next_run_after_plan is None (issue #478 / Codex #2323).
        let exhausted = schedule
            .end_at
            .is_some_and(|end| next_run_after_plan.is_none_or(|next| next >= end));
        if exhausted {
            crate::schedule_decision::record_decision_graceful(
                conn,
                Some(&**metrics),
                Some(schedule.id),
                wf_name,
                "workflow",
                "skipped",
                "end_at_reached",
                Some(serde_json::json!({
                    "end_at": schedule.end_at,
                    "next_run_after_plan": next_run_after_plan,
                    "empty_catchup_plan": true,
                })),
                now,
                now,
                i16::try_from(current_shard.as_i32()).unwrap_or(0),
            )
            .await;
        }
        let (final_next, exhausted_at, exhausted_reason) = if exhausted {
            (None, Some(now), Some("end_at_reached"))
        } else {
            (next_run_after_plan, None, None)
        };
        diesel::update(
            dsl::harvest_schedules
                .find(schedule.id)
                .filter(dsl::fire_claim_token.eq(Some(claim_token))),
        )
        .set((
            dsl::next_run_at.eq(final_next),
            dsl::exhausted_at.eq(exhausted_at),
            dsl::exhausted_reason.eq(exhausted_reason),
            dsl::fire_claim_token.eq(Option::<uuid::Uuid>::None),
            dsl::fire_claimed_until.eq(Option::<DateTime<Utc>>::None),
            dsl::updated_at.eq(now),
        ))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
        return Ok(());
    }

    let mut running: i64 = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(wf_name))
        .filter(harvest_workflow_executions::state.eq_any(["RUNNING", "PAUSED"]))
        .count()
        .get_result(conn)
        .await
        .map_err(crate::error::database_error)?;
    // A throttled fire (issue #607) durably defers before any execution row
    // exists -- count it toward max_active_runs/overlap so a schedule can't
    // dispatch past its own concurrency limit while an earlier fire is still
    // sitting in the throttle queue (code review, issue #607).
    running += crate::throttle::pending_throttle_count_for_workflow(conn, wf_name).await?;

    if running >= i64::from(schedule.max_active_runs) {
        let overlap_policy = OverlapPolicy::from_db(&schedule.overlap_policy);
        let mut buffered = parse_buffered_runs(&schedule.buffered_runs);
        let buffer_all_max = usize::try_from(schedule.buffer_all_max.max(1)).unwrap_or(usize::MAX);

        let action = apply_overlap_policy(overlap_policy, logical_date, &buffered, buffer_all_max);

        match action {
            OverlapAction::Drop { reason } => {
                // Committing path (next_run_at advances past — or is retained at
                // the policy-approved slot beyond — the dropped slots), so emit
                // the catchup-drop audit here (issue #484 / Codex #2297).
                record_catchup_drops(
                    conn,
                    metrics,
                    schedule.id,
                    wf_name,
                    catchup_policy,
                    schedule.catchup_window_secs,
                    catchup_slots_dropped,
                    now,
                    current_shard,
                    claim_token,
                )
                .await;
                tracing::info!(
                    workflow_name = %wf_name,
                    running,
                    max_active_runs = schedule.max_active_runs,
                    overlap_policy = %overlap_policy.as_str(),
                    reason,
                    "harvest workflow schedule firing skipped due to overlap policy"
                );
                metrics.record_schedule_skipped("workflow", wf_name, reason);
                // For Skip drops (reason = "max_active_runs_reached"), retain
                // logical_date under catchup so the overdue slot fires once
                // capacity opens. For buffer-full drops the slot is permanently
                // discarded; advance past it so newer catchup slots are not
                // blocked behind the same overdue timestamp on every tick.
                let retain_for_retry = catchup && reason == "max_active_runs_reached";
                let next = if retain_for_retry {
                    Some(logical_date)
                } else {
                    next_run_after(parsed_schedule, now)
                };
                crate::schedule_decision::record_decision_graceful(
                    conn,
                    Some(&**metrics),
                    Some(schedule.id),
                    wf_name,
                    "workflow",
                    "skipped",
                    reason,
                    Some(serde_json::json!({
                        "overlap_policy": overlap_policy.as_str(),
                        "running_runs": running,
                        "max_active_runs": schedule.max_active_runs,
                    })),
                    now,
                    next.unwrap_or(now),
                    i16::try_from(current_shard.as_i32()).unwrap_or(0),
                )
                .await;
                diesel::update(
                    dsl::harvest_schedules
                        .find(schedule.id)
                        .filter(dsl::fire_claim_token.eq(Some(claim_token))),
                )
                .set((
                    dsl::next_run_at.eq(next),
                    dsl::fire_claim_token.eq(Option::<uuid::Uuid>::None),
                    dsl::fire_claimed_until.eq(Option::<DateTime<Utc>>::None),
                    dsl::updated_at.eq(now),
                ))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
                return Ok(());
            }
            OverlapAction::Buffer { fire_time } => {
                // Committing path (next_run_at advances past the dropped slots),
                // so emit the catchup-drop audit here (issue #484 / Codex #2297).
                record_catchup_drops(
                    conn,
                    metrics,
                    schedule.id,
                    wf_name,
                    catchup_policy,
                    schedule.catchup_window_secs,
                    catchup_slots_dropped,
                    now,
                    current_shard,
                    claim_token,
                )
                .await;
                buffered.push(fire_time);
                tracing::info!(
                    workflow_name = %wf_name,
                    buffered_count = buffered.len(),
                    overlap_policy = %overlap_policy.as_str(),
                    "harvest: buffering schedule firing for later dispatch"
                );
                // Advance next_run_at past the buffered slot so new firings
                // can be evaluated normally on subsequent ticks.
                let next = if catchup {
                    next_run_after(parsed_schedule, logical_date)
                } else {
                    next_run_after(parsed_schedule, now)
                };
                crate::schedule_decision::record_decision_graceful(
                    conn,
                    Some(&**metrics),
                    Some(schedule.id),
                    wf_name,
                    "workflow",
                    "skipped",
                    "overlap_buffered",
                    Some(serde_json::json!({
                        "overlap_policy": overlap_policy.as_str(),
                        "buffered_runs": buffered.len(),
                        "running_runs": running,
                        "max_active_runs": schedule.max_active_runs,
                    })),
                    now,
                    next.unwrap_or(now),
                    i16::try_from(current_shard.as_i32()).unwrap_or(0),
                )
                .await;
                diesel::update(
                    dsl::harvest_schedules
                        .find(schedule.id)
                        .filter(dsl::fire_claim_token.eq(Some(claim_token))),
                )
                .set((
                    dsl::next_run_at.eq(next),
                    dsl::buffered_runs.eq(buffered_runs_to_json(&buffered)),
                    dsl::fire_claim_token.eq(Option::<uuid::Uuid>::None),
                    dsl::fire_claimed_until.eq(Option::<DateTime<Utc>>::None),
                    dsl::updated_at.eq(now),
                ))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
                return Ok(());
            }
            OverlapAction::CancelAndProceed => {
                // Cancel only the oldest scheduled runs needed to free one slot,
                // then fall through to dispatch. Subtract the cancelled count so
                // the dispatch loop sees the correct remaining capacity.
                let needed =
                    u32::try_from(running.saturating_sub(i64::from(schedule.max_active_runs)) + 1)
                        .unwrap_or(1);
                let cancelled = cancel_in_flight_runs(
                    conn,
                    wf_name,
                    schedule.id,
                    "overlap policy CancelOther: new firing",
                    needed,
                    metrics.as_ref(),
                )
                .await?;
                running -= i64::from(cancelled);
            }
            OverlapAction::TerminateAndProceed => {
                // Terminate only the minimum needed to free one slot.
                let needed =
                    u32::try_from(running.saturating_sub(i64::from(schedule.max_active_runs)) + 1)
                        .unwrap_or(1);
                let terminated = terminate_in_flight_runs(
                    conn,
                    wf_name,
                    schedule.id,
                    "overlap policy TerminateOther: new firing",
                    needed,
                    metrics.as_ref(),
                )
                .await?;
                running -= i64::from(terminated);
            }
        }
    }

    let dispatch_queue = schedule.queue_name.as_deref().unwrap_or("default");
    // jitter_window already computed at function entry; reused here.

    // Re-read runs_started from the DB now that we hold the HA claim and have
    // finished the overlap handling. This captures any concurrent manual trigger
    // pre-increments that happened between the outer tick query and this point,
    // giving the dispatch loop a fresh budget baseline so it does not over-dispatch
    // against a budget that was already partially consumed by a manual trigger.
    let live_runs_started: i32 = dsl::harvest_schedules
        .find(schedule.id)
        .filter(dsl::fire_claim_token.eq(Some(claim_token)))
        .select(dsl::runs_started)
        .first(conn)
        .await
        .map_err(crate::error::database_error)?;

    let mut dispatched: u32 = 0;
    let mut last_dispatched_at: Option<DateTime<Utc>> = None;
    // Tracks the pre-rebase original slot of the last dispatched run. Used to
    // anchor next_run_at when a calendar rebases the fire time forward, so the
    // natural next cron slot after the original due slot is not skipped.
    let mut last_original_slot_dispatched: Option<DateTime<Utc>> = None;
    // Set to the first slot we could not dispatch due to max_active_runs or jitter; if Some,
    // it becomes next_run_at so catchup slots are not silently dropped.
    let mut deferred_next_run_at: Option<DateTime<Utc>> = None;
    for original_slot in &run_dates {
        // Apply calendar filtering for every slot (including the first one, since
        // the pre-loop check no longer rebases `logical_date`).
        let effective_scheduled_for = if schedule.calendar_name.is_some() {
            let slot_date = original_slot.date_naive();
            let Some(adjusted) = crate::calendar::apply_skip_policy(
                slot_date,
                calendar_skip_policy,
                &calendar_excluded,
                calendar_exclude_weekends,
            ) else {
                metrics.record_schedule_skipped("workflow", wf_name, "calendar");
                continue;
            };
            if adjusted == slot_date {
                *original_slot
            } else {
                rebase_logical_date(*original_slot, adjusted, parsed_schedule)
            }
        } else {
            *original_slot
        };
        let scheduled_for = &effective_scheduled_for;

        if running + i64::from(dispatched) >= i64::from(schedule.max_active_runs) {
            deferred_next_run_at = Some(*original_slot);
            tracing::info!(
                workflow_name = %wf_name,
                max_active_runs = schedule.max_active_runs,
                "harvest workflow schedule: max_active_runs reached during catchup; deferring remaining"
            );
            crate::schedule_decision::record_decision_graceful(
                conn,
                Some(&**metrics),
                Some(schedule.id),
                wf_name,
                "workflow",
                "skipped",
                "max_active_runs_reached",
                Some(serde_json::json!({
                    "running_runs": running,
                    "dispatched_runs": dispatched,
                    "max_active_runs": schedule.max_active_runs,
                    "deferred_slot": original_slot,
                })),
                now,
                *original_slot,
                i16::try_from(current_shard.as_i32()).unwrap_or(0),
            )
            .await;
            break;
        }
        // Per-slot end_at guard (issue #478): compare against the calendar-rebased
        // effective value (effective_scheduled_for), not original_slot. A slot that
        // was originally before end_at but was rebased to a business day at or past
        // end_at must also stop. Only defer back to original_slot when it too is
        // >= end_at (the post-loop end_at_now_exhausted check sees it and exhausts
        // correctly); when original_slot < end_at, deferring to it creates the same
        // stuck retry loop as the jitter case — just break.
        if let Some(end_at) = schedule.end_at
            && effective_scheduled_for >= end_at
        {
            if *original_slot >= end_at {
                deferred_next_run_at = Some(*original_slot);
            }
            break;
        }
        // Budget cap (issue #478): use the DB-fresh live_runs_started (re-read after
        // claim, above) rather than the stale schedule.runs_started so that concurrent
        // manual trigger pre-increments are visible here and prevent over-dispatch.
        if let Some(max_runs) = schedule.max_runs {
            let already =
                live_runs_started.saturating_add(i32::try_from(dispatched).unwrap_or(i32::MAX));
            if max_runs > 0 && already >= max_runs {
                deferred_next_run_at = Some(*original_slot);
                break;
            }
        }
        // Jitter: stall dispatch until the effective fire time has elapsed.
        // effective_fire_time = scheduled_for + hash(schedule_id, scheduled_for) % jitter_window
        let jitter_offset = compute_jitter_offset(schedule.id, *scheduled_for, jitter_window);
        let chrono_jitter = chrono::Duration::from_std(jitter_offset).unwrap_or_else(|_| {
            tracing::warn!(
                schedule_id = %schedule.id,
                jitter_secs = schedule.jitter_secs,
                "harvest: jitter_secs overflows chrono::Duration; falling back to zero jitter"
            );
            chrono::Duration::zero()
        });
        let effective_fire_time = *scheduled_for + chrono_jitter;
        if now < effective_fire_time {
            deferred_next_run_at = Some(*original_slot);
            tracing::debug!(
                workflow_name = %wf_name,
                logical_date = %scheduled_for,
                effective_fire_time = %effective_fire_time,
                "harvest: schedule jitter pending; deferring dispatch"
            );
            break;
        }
        // Secondary end_at guard: jitter may push the effective dispatch time past
        // the cutoff even when the original slot was before it.
        // Do NOT set deferred_next_run_at here — original_slot is still < end_at,
        // so deferring to it would cause every tick to retry the same slot forever
        // without ever firing or exhausting. Falling through to the post-loop
        // end_at_now_exhausted check with deferred_next_run_at = None lets it use
        // next_run_after_plan (which is >= end_at) and correctly mark the schedule
        // exhausted.
        if let Some(end_at) = schedule.end_at
            && effective_fire_time >= end_at
        {
            break;
        }
        let workflow_id = scheduled_workflow_id(schedule.id, wf_name, *original_slot);
        let exec_id = if schedule.dag_name.is_some() {
            ExecutionId::new_for_shard(current_shard)
        } else {
            ExecutionId::new()
        };
        let input = schedule
            .workflow_input
            .clone()
            .unwrap_or(serde_json::Value::Null);
        let wf_info = registry.workflows.get(wf_name);
        let (concurrency_key, concurrency_limit) = wf_info
            .and_then(|info| info.concurrency.as_ref())
            .map_or((None, None), |policy| {
                let key = crate::concurrency::resolve_concurrency_key(policy.key_expr, &input);
                (key, Some(policy.limit))
            });
        let (owner, runbook_url, severity) = {
            let wf_meta = wf_info.map(|info| (info.owner, info.runbook_url, info.severity));
            let dag_meta = registered_dags.get(wf_name).map(|dag| {
                (
                    dag.owner.as_deref(),
                    dag.runbook_url.as_deref(),
                    dag.severity.as_deref(),
                )
            });
            match (wf_meta, dag_meta) {
                (Some((o, r, s)), Some((dag_owner, dag_runbook, dag_severity))) => {
                    (o.or(dag_owner), r.or(dag_runbook), s.or(dag_severity))
                }
                (Some((o, r, s)), None) => (o, r, s),
                (None, Some((dag_owner, dag_runbook, dag_severity))) => {
                    (dag_owner, dag_runbook, dag_severity)
                }
                (None, None) => (None, None, None),
            }
        };
        // Only workflows carry an SLA default; DAGs have no SLA concept.
        let sla = wf_info
            .and_then(|info| info.sla)
            .and_then(|d| chrono::Duration::from_std(d).ok());
        tracing::info!(
            workflow_name = %wf_name, workflow_id = %workflow_id,
            scheduled_for = %scheduled_for, "harvest: dispatching scheduled workflow run"
        );

        // Start-throttle admission (issue #607): pace scheduled fires, defer the
        // excess. A deferred fire counts as dispatched (the slot is consumed so
        // it is not re-fired) and is admitted later by the throttle scanner; its
        // schedule_id/scheduled_for/origin are persisted so carryover (#488) and
        // run-history (#534) lineage survive the deferral. On reserve the token
        // is refunded below if the start short-circuits under the reuse policy.
        let mut scheduled_throttle_bucket: Option<String> = None;
        if let Some(throttle_policy) = wf_info.and_then(|info| info.throttle) {
            let throttle_key = throttle_policy.key_expr.map_or_else(
                || Some(String::new()),
                |k| crate::throttle::resolve_throttle_key(k, &input),
            );
            if let Some(resolved_throttle_key) = throttle_key {
                let effective_cap = wf_info
                    .and_then(|info| info.max_input_bytes)
                    .map_or(registry.max_workflow_input_bytes, |per| {
                        per.max(registry.max_workflow_input_bytes)
                    });
                // Fail fast on an oversized input rather than persisting a
                // pending row that would fail at fire time on every scanner
                // tick (code-review fix, issue #607). Returning the error here
                // -- rather than falling through to defer -- mirrors the
                // non-throttled path's `return Err(error)` a few lines below:
                // `dispatched`/`last_dispatched_at`/`last_original_slot_dispatched`
                // are never touched, so `last_run_at` is not advanced and the
                // next tick retries the same firing. Skipped when
                // `reserve_or_defer` would resolve via `Bypassed` or an
                // idempotent attach to an already-pending row.
                let skip_cap_check = crate::throttle::skip_size_check(
                    conn,
                    wf_name,
                    &workflow_id,
                    Some("reject_duplicate"),
                )
                .await?;
                if !skip_cap_check && effective_cap > 0 {
                    let observed = serde_json::to_string(&input).map_or(0u64, |s| s.len() as u64);
                    if observed > effective_cap {
                        return Err(crate::error::HarvestError::PayloadTooLarge {
                            kind: crate::error::PayloadKind::WorkflowInput,
                            observed_bytes: observed,
                            cap_bytes: effective_cap,
                            workflow_type: wf_name.to_string(),
                            activity_name: None,
                        });
                    }
                }
                let effective_retry = schedule
                    .retry_policy
                    .as_ref()
                    .and_then(|v| {
                        serde_json::from_value::<crate::policy::RetryPolicy>(v.clone()).ok()
                    })
                    .or_else(|| wf_info.and_then(|info| info.retry_policy.clone()))
                    .and_then(|p| serde_json::to_value(&p).ok());
                let start_options = crate::debounce::DebounceStartOptions {
                    reuse_policy: Some("reject_duplicate".to_string()),
                    execution_timeout_secs: wf_info
                        .and_then(|info| info.execution_timeout)
                        .and_then(|d| chrono::Duration::from_std(d).ok())
                        .map(|d| d.num_seconds()),
                    memo: None,
                    search_attrs: None,
                    sla_secs: sla.map(|d| d.num_seconds()),
                    context_headers: None,
                    priority: None,
                    concurrency_key: concurrency_key.clone(),
                    concurrency_limit,
                    owner: owner.map(str::to_string),
                    runbook_url: runbook_url.map(str::to_string),
                    severity: severity.map(str::to_string),
                    max_execution_timeout_ceiling_secs: None,
                    max_workflow_input_bytes: Some(effective_cap),
                    trace_context: None,
                    workflow_retry_policy: effective_retry,
                    max_workflow_attempts_ceiling: registry.max_workflow_attempts_ceiling,
                    completion_callbacks: None,
                    schedule_id: Some(schedule.id),
                    scheduled_for: Some(*original_slot),
                    origin: Some(crate::execution::ORIGIN_SCHEDULED.to_string()),
                };
                match crate::throttle::reserve_or_defer(
                    conn,
                    crate::throttle::AdmitThrottleParams {
                        workflow_name: wf_name,
                        throttle_key: &resolved_throttle_key,
                        workflow_id: &workflow_id,
                        queue_name: dispatch_queue,
                        input: input.clone(),
                        start_options,
                        refill_per_sec: throttle_policy.refill_per_sec,
                        burst: throttle_policy.burst,
                        schedule_to_start: throttle_policy.schedule_to_start,
                        shard_id: current_shard.as_i32(),
                    },
                )
                .await
                {
                    Ok(crate::throttle::ThrottleAdmission::Deferred(_)) => {
                        metrics.record_start_throttled(wf_name);
                        dispatched += 1;
                        last_dispatched_at = Some(*scheduled_for);
                        last_original_slot_dispatched = Some(*original_slot);
                        continue;
                    }
                    Ok(crate::throttle::ThrottleAdmission::Reserved { bucket_key }) => {
                        scheduled_throttle_bucket = Some(bucket_key);
                    }
                    Ok(crate::throttle::ThrottleAdmission::Bypassed) => {
                        // Active execution already resolves this reuse policy as a
                        // no-op/immediate reject; no token reserved, fall through to
                        // the normal start below.
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        let start_result = crate::execution::start_or_load_workflow_execution(
            conn,
            StartWorkflowParams {
                workflow_name: wf_name,
                workflow_id: &workflow_id,
                exec_id,
                input,
                parent_id: None,
                queue_name: dispatch_queue,
                execution_timeout: wf_info
                    .and_then(|info| info.execution_timeout)
                    .and_then(|d| chrono::Duration::from_std(d).ok()),
                memo: None,
                search_attrs: None,
                reuse_policy: scheduled_workflow_reuse_policy(),
                trace_context: None,
                max_execution_timeout_ceiling: None,
                concurrency_key,
                concurrency_limit,
                priority: Priority::default(),
                max_workflow_input_bytes: wf_info
                    .and_then(|info| info.max_input_bytes)
                    .map_or(registry.max_workflow_input_bytes, |per| {
                        per.max(registry.max_workflow_input_bytes)
                    }),
                start_at: None,
                delay: None,
                max_workflow_start_delay: None,
                owner,
                runbook_url,
                severity,
                context_headers: None,
                sla,
                schedule_id: Some(schedule.id),
                // Logical slot = the slot encoded in workflow_id (original_slot), so
                // carryover ordering and the migration backfill agree (issue #488).
                scheduled_for: Some(*original_slot),
                workflow_attempt: 1,
                workflow_retry_policy: schedule
                    .retry_policy
                    .as_ref()
                    .and_then(|v| serde_json::from_value(v.clone()).ok())
                    .or_else(|| wf_info.and_then(|info| info.retry_policy.clone())),
                retry_of_exec_id: None,
                max_workflow_attempts_ceiling: registry.max_workflow_attempts_ceiling,
                // Normal scheduler-tick fire — attributed as the schedule's cadence (issue #534).
                origin: Some(crate::execution::ORIGIN_SCHEDULED),
                completion_callbacks: None,
            },
        )
        .await;
        match scheduled_start_outcome(start_result) {
            Ok(outcome) => {
                dispatched += 1;
                last_dispatched_at = Some(*scheduled_for);
                last_original_slot_dispatched = Some(*original_slot);
                if outcome.created() {
                    metrics.record_schedule_run("workflow", wf_name);
                } else if let Some(ref bucket) = scheduled_throttle_bucket {
                    // AC-a: RejectDuplicate returned an existing run — no admission,
                    // refund the reserved throttle token.
                    let _ = crate::queue::refund_rate_limit_token(conn, bucket).await;
                }
                tracing::info!(
                    workflow_name = %wf_name,
                    execution_id = %outcome.exec_id(),
                    state = %outcome.state(),
                    created = outcome.created(),
                    "harvest: scheduled workflow run dispatched"
                );
                let next_slot = next_run_after(parsed_schedule, *original_slot).unwrap_or(now);
                crate::schedule_decision::record_decision_graceful(
                    conn,
                    Some(&**metrics),
                    Some(schedule.id),
                    wf_name,
                    "workflow",
                    "fired",
                    "fired_ok",
                    Some(serde_json::json!({
                        "execution_id": outcome.exec_id(),
                        "state": outcome.state().to_string(),
                        "created": outcome.created(),
                    })),
                    now,
                    next_slot,
                    i16::try_from(current_shard.as_i32()).unwrap_or(0),
                )
                .await;
            }
            Err(error) => {
                // No run admitted — refund the reserved throttle token before
                // propagating.
                if let Some(ref bucket) = scheduled_throttle_bucket {
                    let _ = crate::queue::refund_rate_limit_token(conn, bucket).await;
                }
                // Propagate the error so last_run_at is not advanced — the next
                // tick will retry the same firing rather than silently dropping it.
                tracing::warn!(
                    error = %error, workflow_name = %wf_name, workflow_id = %workflow_id,
                    "harvest: failed to start scheduled workflow run"
                );
                return Err(error);
            }
        }
    }

    // Deferred catchup slots become next_run_at so the next tick retries them.
    // last_run_at only advances to the last slot actually started.
    let effective_last_run_at = last_dispatched_at.or(schedule.last_run_at);
    // When calendar rebasing is active, next_run_after_plan was anchored to the
    // original due slot and its `filter(t > now)` guard can spuriously fail when
    // `now` has advanced past the rebased fire time, causing the natural next
    // cron slot to be skipped. Re-anchor from the last original (pre-rebase)
    // slot instead, but preserve the non-catchup skip-overdue semantics by
    // filtering out past successors and falling back to next_run_after_plan
    // (which already contains the `or_else(next_run_after(now))` fallback).
    let effective_next_run_at = deferred_next_run_at.or_else(|| {
        if schedule.calendar_name.is_some() {
            last_original_slot_dispatched
                .and_then(|slot| next_run_after(parsed_schedule, slot))
                .filter(|&t| t > now)
                .or(next_run_after_plan)
        } else {
            next_run_after_plan
        }
    });

    // ── Budget accounting and exhaustion (issue #478) ────────────────────────
    // Increment runs_started using the DB-fresh live_runs_started baseline (re-read
    // after claim). Manual trigger pre-increments are NOT serialized under the HA
    // claim, so the final UPDATE must use a DB-side expression (runs_started +
    // dispatched) rather than the in-memory computed value, ensuring the tick and
    // any concurrent manual trigger each add their own delta without overwriting
    // the other's contribution.
    let new_runs_started =
        live_runs_started.saturating_add(i32::try_from(dispatched).unwrap_or(i32::MAX));
    // Treat the schedule as budget-exhausted when the live count reaches max_runs
    // regardless of whether *this* tick dispatched anything: a concurrent manual
    // trigger or backfill may have consumed the last slot after the stale
    // `schedule.runs_started` snapshot but before we read `live_runs_started`.
    let now_budget_exhausted = schedule
        .max_runs
        .is_some_and(|max| max > 0 && new_runs_started >= max);
    // Eagerly exhaust on end_at: if the next valid slot is at or past end_at, mark
    // the schedule exhausted immediately so operators see it as done after the last
    // valid firing rather than waiting for the next tick to discover it.
    let end_at_now_exhausted = schedule
        .end_at
        .is_some_and(|end| effective_next_run_at.is_none_or(|next| next >= end));
    if now_budget_exhausted {
        let max = schedule.max_runs.unwrap_or(0);
        tracing::info!(
            workflow_name = %wf_name,
            runs_started = new_runs_started,
            max_runs = max,
            "harvest: schedule max_runs budget exhausted after dispatch; transitioning to exhausted"
        );
        crate::schedule_decision::record_decision_graceful(
            conn,
            Some(&**metrics),
            Some(schedule.id),
            wf_name,
            "workflow",
            "skipped",
            "max_runs_exhausted",
            Some(serde_json::json!({
                "runs_started": new_runs_started,
                "max_runs": max,
                "exhausted_after_dispatch": true,
            })),
            now,
            now,
            i16::try_from(current_shard.as_i32()).unwrap_or(0),
        )
        .await;
    } else if end_at_now_exhausted {
        let end = schedule.end_at.unwrap_or(now);
        tracing::info!(
            workflow_name = %wf_name,
            end_at = %end,
            "harvest: schedule end_at boundary reached after dispatch; transitioning to exhausted"
        );
        crate::schedule_decision::record_decision_graceful(
            conn,
            Some(&**metrics),
            Some(schedule.id),
            wf_name,
            "workflow",
            "skipped",
            "end_at_reached",
            Some(serde_json::json!({
                "end_at": end,
                "effective_next_run_at": effective_next_run_at,
            })),
            now,
            now,
            i16::try_from(current_shard.as_i32()).unwrap_or(0),
        )
        .await;
    }
    let any_exhausted = now_budget_exhausted || end_at_now_exhausted;
    let budget_exhausted_at: Option<DateTime<Utc>> = any_exhausted.then_some(now);
    let budget_exhausted_reason: Option<&str> = if now_budget_exhausted {
        Some("max_runs_exhausted")
    } else if end_at_now_exhausted {
        Some("end_at_reached")
    } else {
        None
    };

    // Resolve effective_next_run_at: NULL when the schedule is now exhausted so
    // it never re-appears in the due-list query.
    let final_next_run_at = if any_exhausted {
        None
    } else {
        effective_next_run_at
    };

    // Dispatch reached the commit point without a `start_or_load` error: the
    // dropped slots are now durably past `next_run_at`, so emit the catchup-drop
    // audit here (and only here on the dispatch path) — never before the fallible
    // dispatch loop above (issue #484 / Codex #2174).
    record_catchup_drops(
        conn,
        metrics,
        schedule.id,
        wf_name,
        catchup_policy,
        schedule.catchup_window_secs,
        catchup_slots_dropped,
        now,
        current_shard,
        claim_token,
    )
    .await;

    // Use a DB-side runs_started + dispatched expression so that concurrent manual
    // trigger pre-increments (which are not blocked by the HA claim) are preserved
    // rather than overwritten by this stale in-memory value (issue #478).
    let dispatched_i32 = i32::try_from(dispatched).unwrap_or(i32::MAX);
    diesel::update(
        dsl::harvest_schedules
            .find(schedule.id)
            .filter(dsl::fire_claim_token.eq(Some(claim_token))),
    )
    .set((
        dsl::last_run_at.eq(effective_last_run_at),
        dsl::next_run_at.eq(final_next_run_at),
        dsl::runs_started.eq(dsl::runs_started + dispatched_i32),
        dsl::exhausted_at.eq(budget_exhausted_at),
        dsl::exhausted_reason.eq(budget_exhausted_reason),
        // Clear the HA claim so the column stays clean after a successful
        // fire. Guarded by token so a slow late tick cannot overwrite a
        // successor replica's live claim if the 30 s TTL expired.
        dsl::fire_claim_token.eq(Option::<uuid::Uuid>::None),
        dsl::fire_claimed_until.eq(Option::<DateTime<Utc>>::None),
        // NOTE: last_catchup_dropped / last_catchup_at are intentionally NOT set
        // here. They are persisted by a separate conditional update in the
        // `catchup_slots_dropped > 0` block above so that ordinary zero-drop
        // ticks do not wipe the most-recent recovery audit trail (issue #484).
        dsl::updated_at.eq(now),
    ))
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;

    Ok(())
}

/// Validate a [`Schedule`] at creation time.
///
/// Returns `Err` if the schedule is a `Cron` variant whose expression cannot be
/// parsed by `croner`. `Interval` and `Manual` schedules are always valid.
///
// Re-exported so callers can reach it via the `scheduler` module path, which
// is where it lived before being moved to `policy` for feature-gate reasons.
pub use crate::policy::validate_schedule;

fn schedule_expr(schedule: Option<&Schedule>) -> Option<String> {
    match schedule {
        Some(Schedule::Cron(expr)) => Some(format!("cron:{expr}")),
        Some(Schedule::CronInTimezone { expr, tz }) => Some(format!("cron_tz:{tz}:{expr}")),
        Some(Schedule::Interval(interval)) => Some(format!("interval:{}", interval.as_secs())),
        Some(Schedule::Manual) => Some("manual".to_string()),
        None => None,
    }
}

/// Rebase a `DateTime<Utc>` to a different date while preserving the time-of-day.
/// Used by the calendar check to shift `logical_date` when a skip policy defers
/// the fire to a different day.
fn rebase_logical_date(
    ts: DateTime<Utc>,
    date: chrono::NaiveDate,
    schedule: Option<&Schedule>,
) -> DateTime<Utc> {
    // For timezone-aware schedules preserve the wall-clock time in the schedule's
    // timezone so DST transitions don't shift the effective dispatch hour.
    if let Some(Schedule::CronInTimezone { tz, .. }) = schedule
        && let Ok(tz) = tz.parse::<chrono_tz::Tz>()
    {
        let local_ts = ts.with_timezone(&tz);
        if let Some(local_dt) = date
            .and_time(local_ts.time())
            .and_local_timezone(tz)
            .earliest()
        {
            return local_dt.with_timezone(&Utc);
        }
    }
    let naive = date.and_time(ts.time());
    chrono::Utc.from_utc_datetime(&naive)
}

fn next_run_after(schedule: Option<&Schedule>, reference: DateTime<Utc>) -> Option<DateTime<Utc>> {
    match schedule {
        Some(Schedule::Cron(expr)) => Cron::new(expr)
            .with_seconds_optional()
            .parse()
            .ok()
            .and_then(|cron| cron.find_next_occurrence(&reference, false).ok()),
        Some(Schedule::CronInTimezone { expr, tz }) => {
            // Parse the IANA timezone; if it's invalid we return None so the
            // schedule simply doesn't fire (validation should have caught this).
            let tz: chrono_tz::Tz = tz.parse().ok()?;
            let cron = Cron::new(expr).with_seconds_optional().parse().ok()?;
            // Convert the reference instant into the schedule's local timezone.
            let local_ref = reference.with_timezone(&tz);
            // First pass: find the candidate next occurrence strictly after the
            // reference time.
            let candidate = cron.find_next_occurrence(&local_ref, false).ok()?;
            // DST spring-forward correction: when `croner` computes a next
            // occurrence that falls inside a gap (e.g. "02:30 AM" on a day
            // where 02:00–03:00 is skipped), `chrono_tz` silently advances the
            // non-existent local time to the first valid second after the gap
            // (e.g. 03:00:00 PDT).  That instant does NOT match the cron
            // pattern, so calling `find_next_occurrence` again with
            // `include_current = true` from the candidate advances to the
            // actual next matching wall-clock time (e.g. 02:30 on the next day)
            // without double-advancing on ordinary (non-gap) results.
            //
            // Fall-back is handled correctly by the strict `false` on the first
            // call: `croner` advances past the first occurrence of the repeated
            // hour when called from it, producing the next-day result.  Calling
            // `find_next_occurrence(&candidate, true)` on a genuine match
            // returns the same instant, preserving that behavior.
            let local_next = cron.find_next_occurrence(&candidate, true).ok()?;
            Some(local_next.with_timezone(&Utc))
        }
        Some(Schedule::Interval(interval)) => chrono::Duration::from_std(*interval)
            .ok()
            // A zero (or non-positive) interval would return `reference` unchanged,
            // which spins any catchup walk forever and makes a due tick loop
            // indefinitely (issue #484 / Codex #3223). Treat a non-advancing
            // interval as "no next occurrence" so every caller terminates; such a
            // schedule simply never fires. (`validate_schedule` also rejects it at
            // registration time.)
            .filter(|duration| *duration > chrono::Duration::zero())
            .map(|duration| reference + duration),
        Some(Schedule::Manual) | None => None,
    }
}

fn due_run_plan(
    schedule: Option<&Schedule>,
    first_due: DateTime<Utc>,
    now: DateTime<Utc>,
    catchup: bool,
) -> (Vec<DateTime<Utc>>, Option<DateTime<Utc>>) {
    if !catchup {
        // Anchor the next slot to first_due so that jitter-induced latency
        // does not drift the schedule (interval schedules: next = first_due +
        // period, not now + period). Fall back to next_run_after(now) when the
        // slot-anchored next is already in the past to preserve the non-catchup
        // skip-overdue-slots semantics for very late dispatchers.
        let next = next_run_after(schedule, first_due)
            .filter(|&t| t > now)
            .or_else(|| next_run_after(schedule, now));
        return (vec![first_due], next);
    }

    let mut created = Vec::with_capacity(1);
    let mut cursor = first_due;

    loop {
        if cursor > now {
            return (created, Some(cursor));
        }
        created.push(cursor);
        let Some(next) = next_run_after(schedule, cursor) else {
            return (created, None);
        };
        cursor = next;
    }
}

// ── Bounded catchup planning (issue #484) ────────────────────────────────────

/// Output of [`catchup_run_plan`].
pub(crate) struct CatchupPlan {
    /// Slots to actually dispatch, in chronological order.
    pub run_dates: Vec<DateTime<Utc>>,
    /// First slot strictly after `now` (the next scheduled `next_run_at`).
    pub next_run_at: Option<DateTime<Utc>>,
    /// Number of missed slots that were dropped by the catchup policy.
    ///
    /// Each dropped slot must be counted towards `harvest.schedule.skipped`
    /// with reason `"catchup_window_exceeded"`.  `SkipAll` and `Unbounded`
    /// never produce drops.
    pub dropped: u64,
}

/// Compute the set of fire slots and the dropped count given a [`CatchupPolicy`].
///
/// This is the policy-aware replacement for the internal `due_run_plan` helper.
/// The old function is preserved as a thin wrapper for backward-compat so that
/// its unit tests continue to serve as regression evidence.
///
/// `end_at` is the schedule's hard cut-off (issue #478). For the bounded
/// policies (`MostRecent`/`Window`) a slot is only eligible if it is strictly
/// before `end_at`, so the planner never selects a slot at/after `end_at` and
/// thereby exhausts the schedule before firing the newest *valid* missed slot
/// (issue #484 / Codex #2861). `SkipAll`/`Unbounded` leave `end_at` enforcement
/// to the dispatch loop, unchanged.
///
/// The bounded policies compute their result **without materializing every
/// missed slot**: interval schedules use closed-form arithmetic (so a
/// high-frequency schedule down for a long period costs O(1), not O(n)), and
/// cron schedules walk but retain only the slots the policy actually keeps
/// (issue #484 / Codex #2859).
pub(crate) fn catchup_run_plan(
    schedule: Option<&Schedule>,
    first_due: DateTime<Utc>,
    now: DateTime<Utc>,
    policy: crate::policy::CatchupPolicy,
    end_at: Option<DateTime<Utc>>,
) -> CatchupPlan {
    use crate::policy::CatchupPolicy;

    match policy {
        CatchupPolicy::SkipAll => {
            // Identical to due_run_plan(false): fire the first overdue slot,
            // anchor next to it (no drift), no drops recorded.
            let next = next_run_after(schedule, first_due)
                .filter(|&t| t > now)
                .or_else(|| next_run_after(schedule, now));
            CatchupPlan {
                run_dates: vec![first_due],
                next_run_at: next,
                dropped: 0,
            }
        }
        CatchupPolicy::Unbounded => {
            // Identical to due_run_plan(true): enumerate every missed slot.
            let (run_dates, next_run_at) = due_run_plan(schedule, first_due, now, true);
            CatchupPlan {
                run_dates,
                next_run_at,
                dropped: 0,
            }
        }
        CatchupPolicy::MostRecent => {
            let summary = eligible_slot_summary(schedule, first_due, now, end_at);
            let run_dates = summary.last_eligible.into_iter().collect::<Vec<_>>();
            let fired = run_dates.len() as u64;
            CatchupPlan {
                run_dates,
                next_run_at: summary.next_run_at,
                dropped: summary.eligible_count.saturating_sub(fired),
            }
        }
        CatchupPolicy::Window(window) => {
            let cutoff = now
                - chrono::Duration::from_std(window).unwrap_or_else(|_| chrono::Duration::zero());
            let summary = eligible_slot_summary(schedule, first_due, now, end_at);
            let run_dates = window_eligible_slots(schedule, first_due, now, end_at, cutoff);
            let fired = run_dates.len() as u64;
            // `summary.next_run_at` is the first slot strictly after `now` for both
            // interval (closed-form) and cron (forward walk to the first slot past
            // `now`) schedules, so the whole bounded in-window set fires this tick
            // and `next_run_at` advances past `now`.
            CatchupPlan {
                run_dates,
                next_run_at: summary.next_run_at,
                dropped: summary.eligible_count.saturating_sub(fired),
            }
        }
    }
}

/// Summary of the eligible missed slots for a bounded catchup policy.
///
/// "Eligible" = a slot `s` with `first_due <= s <= now` and, when `end_at` is
/// set, `s < end_at`.
struct EligibleSummary {
    /// Total number of eligible missed slots.
    eligible_count: u64,
    /// The most recent eligible slot (`None` when there are none).
    last_eligible: Option<DateTime<Utc>>,
    /// The first scheduled slot strictly after `now` (the natural next run).
    /// Independent of `end_at`; the dispatch/budget logic handles exhaustion.
    next_run_at: Option<DateTime<Utc>>,
}

/// Extract a strictly-positive interval period as a `chrono::Duration`.
fn interval_period(schedule: Option<&Schedule>) -> Option<chrono::Duration> {
    match schedule {
        Some(Schedule::Interval(d)) => {
            let cd = chrono::Duration::from_std(*d).ok()?;
            (cd > chrono::Duration::zero()).then_some(cd)
        }
        _ => None,
    }
}

/// Largest step index `k >= 0` such that `first_due + k*period` is eligible, i.e.
/// `<= now` and (when set) `< end_at`. Returns `None` when no slot is eligible.
fn last_eligible_step(span_now: i64, period: i64, end_span: Option<i64>) -> Option<i64> {
    if span_now < 0 {
        return None;
    }
    let k_now = span_now / period; // floor; largest k with k*period <= span_now
    let k = match end_span {
        // largest k with k*period < end_span  <=>  k*period <= end_span - 1
        Some(es) if es >= 1 => k_now.min((es - 1) / period),
        Some(_) => return None, // end_at <= first_due: nothing eligible
        None => k_now,
    };
    (k >= 0).then_some(k)
}

/// Compute the eligible-slot summary, using closed-form arithmetic for interval
/// schedules and a bounded walk for cron schedules.
fn eligible_slot_summary(
    schedule: Option<&Schedule>,
    first_due: DateTime<Utc>,
    now: DateTime<Utc>,
    end_at: Option<DateTime<Utc>>,
) -> EligibleSummary {
    if let Some(period) = interval_period(schedule)
        && let (Some(p), Some(span_now)) = (
            period.num_nanoseconds(),
            (now - first_due).num_nanoseconds(),
        )
    {
        let end_span = end_at.and_then(|e| (e - first_due).num_nanoseconds());
        // Proceed with arithmetic only when end_at is unset or its offset fits in
        // nanoseconds; otherwise fall through to the walk path (avoids a wrong cap).
        if end_at.is_none() || end_span.is_some() {
            let next_run_at = (span_now >= 0)
                .then(|| first_due + chrono::Duration::nanoseconds((span_now / p + 1) * p));
            return last_eligible_step(span_now, p, end_span).map_or(
                EligibleSummary {
                    eligible_count: 0,
                    last_eligible: None,
                    next_run_at,
                },
                |k| EligibleSummary {
                    eligible_count: u64::try_from(k + 1).unwrap_or(0),
                    last_eligible: Some(first_due + chrono::Duration::nanoseconds(k * p)),
                    next_run_at,
                },
            );
        }
    }

    // Cron (or sub-second / overflowing interval): walk, retaining only the
    // count and the last eligible slot — never the full slot vector.
    //
    // The walk visits every missed occurrence up to `now`, so its cost is
    // O(missed occurrences) — the same cost as the `Unbounded` policy. This is
    // unavoidable for cron `MostRecent`: `croner` exposes only a forward
    // `find_next_occurrence` (no reverse iterator), so the most-recent slot at or
    // before `now` and the exact dropped count can only be found by walking
    // forward from `first_due`. A previous, capped version of this walk fired a
    // *stale* mid-backlog slot instead of the newest one (issue #484 /
    // Codex #3210), which violated the policy contract; correctness wins.
    // High-frequency schedules that need O(1) recovery planning should use an
    // `Interval` schedule (handled by the closed-form arithmetic above) rather
    // than a sub-minute cron expression.
    let mut cursor = first_due;
    let mut count: u64 = 0;
    let mut last_eligible = None;
    loop {
        if cursor > now {
            return EligibleSummary {
                eligible_count: count,
                last_eligible,
                next_run_at: Some(cursor),
            };
        }
        if end_at.is_none_or(|e| cursor < e) {
            count += 1;
            last_eligible = Some(cursor);
        }
        let Some(next) = next_run_after(schedule, cursor) else {
            return EligibleSummary {
                eligible_count: count,
                last_eligible,
                next_run_at: None,
            };
        };
        cursor = next;
    }
}

/// Collect the eligible slots within the catchup window `[cutoff, now]`
/// (and `< end_at`). The fired set is bounded by the operator-chosen window, so
/// materializing it is intentional; the dropped count is derived separately by
/// the caller from [`eligible_slot_summary`].
fn window_eligible_slots(
    schedule: Option<&Schedule>,
    first_due: DateTime<Utc>,
    now: DateTime<Utc>,
    end_at: Option<DateTime<Utc>>,
    cutoff: DateTime<Utc>,
) -> Vec<DateTime<Utc>> {
    if let Some(period) = interval_period(schedule)
        && let (Some(p), Some(span_now)) = (
            period.num_nanoseconds(),
            (now - first_due).num_nanoseconds(),
        )
    {
        let end_span = end_at.and_then(|e| (e - first_due).num_nanoseconds());
        if end_at.is_none() || end_span.is_some() {
            let Some(k_hi) = last_eligible_step(span_now, p, end_span) else {
                return vec![];
            };
            // Smallest k with first_due + k*period >= cutoff.
            let k_lo = match (cutoff - first_due).num_nanoseconds() {
                Some(cs) if cs > 0 => (cs + p - 1) / p, // ceil
                _ => 0,
            };
            if k_lo > k_hi {
                return vec![];
            }
            return (k_lo..=k_hi)
                .map(|k| first_due + chrono::Duration::nanoseconds(k * p))
                .collect();
        }
    }

    // Cron fallback: walk and keep only the in-window, eligible slots. Jump
    // straight to the first occurrence at/after the window cutoff so a long
    // pre-window backlog is not walked one slot at a time (issue #484 /
    // Codex #3069); the in-window set itself is bounded by the operator-chosen
    // window, so collecting it is intentional. Using `cutoff - 1ns` makes the
    // jump inclusive of an occurrence landing exactly on the cutoff.
    let mut cursor = if cutoff > first_due {
        next_run_after(schedule, cutoff - chrono::Duration::nanoseconds(1)).unwrap_or(first_due)
    } else {
        first_due
    };
    let mut slots = Vec::new();
    loop {
        if cursor > now {
            return slots;
        }
        if cursor >= cutoff && end_at.is_none_or(|e| cursor < e) {
            slots.push(cursor);
        }
        let Some(next) = next_run_after(schedule, cursor) else {
            return slots;
        };
        cursor = next;
    }
}

// ── Overlap policy helpers ────────────────────────────────────────────────────

/// The action the scheduler takes when a new firing can't start immediately
/// because `max_active_runs` is already reached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OverlapAction {
    /// Store this fire time in the schedule's buffer for later dispatch.
    Buffer { fire_time: DateTime<Utc> },
    /// Drop this firing, recording a skip metric with the given reason string.
    Drop { reason: &'static str },
    /// Cancel all in-flight runs for this workflow, then start the new firing.
    CancelAndProceed,
    /// Terminate all in-flight runs for this workflow, then start the new firing.
    TerminateAndProceed,
}

/// Decide what to do with a new firing that can't run immediately.
///
/// `buffered` is the current set of already-buffered fire times.
/// `buffer_all_max` is the cap for [`OverlapPolicy::BufferAll`].
#[allow(clippy::missing_const_for_fn)]
pub(crate) fn apply_overlap_policy(
    policy: OverlapPolicy,
    fire_time: DateTime<Utc>,
    buffered: &[DateTime<Utc>],
    buffer_all_max: usize,
) -> OverlapAction {
    match policy {
        OverlapPolicy::Skip => OverlapAction::Drop {
            reason: "max_active_runs_reached",
        },
        OverlapPolicy::BufferOne => {
            if buffered.is_empty() {
                OverlapAction::Buffer { fire_time }
            } else {
                OverlapAction::Drop {
                    reason: "buffered_slot_full",
                }
            }
        }
        OverlapPolicy::BufferAll => {
            if buffered.len() < buffer_all_max {
                OverlapAction::Buffer { fire_time }
            } else {
                OverlapAction::Drop {
                    reason: "buffer_full",
                }
            }
        }
        OverlapPolicy::CancelOther => OverlapAction::CancelAndProceed,
        OverlapPolicy::TerminateOther => OverlapAction::TerminateAndProceed,
    }
}

/// Deserialize the `buffered_runs` JSONB column into a sorted list of fire times.
///
/// Malformed entries are silently skipped so a partial JSON corruption can't
/// permanently wedge a schedule.
pub(crate) fn parse_buffered_runs(value: &serde_json::Value) -> Vec<DateTime<Utc>> {
    let Some(arr) = value.as_array() else {
        return vec![];
    };
    let mut times: Vec<DateTime<Utc>> = arr
        .iter()
        .filter_map(|v| v.as_str())
        .filter_map(|s| s.parse::<DateTime<Utc>>().ok())
        .collect();
    times.sort();
    times
}

/// Public re-export of `parse_buffered_runs` for the plugin API layer.
#[must_use]
pub fn parse_buffered_runs_pub(value: &serde_json::Value) -> Vec<DateTime<Utc>> {
    parse_buffered_runs(value)
}

/// Serialize a list of fire times back into the `buffered_runs` JSONB column.
pub(crate) fn buffered_runs_to_json(runs: &[DateTime<Utc>]) -> serde_json::Value {
    serde_json::Value::Array(
        runs.iter()
            .map(|t| serde_json::Value::String(t.to_rfc3339()))
            .collect(),
    )
}

/// Drain buffered runs for all schedules that have pending buffer entries.
///
/// Called on every scheduler tick. For each schedule with a non-empty
/// `buffered_runs` column, dispatches buffered fire times in order until
/// `max_active_runs` is reached, then updates the `buffered_runs` column.
#[cfg(feature = "db")]
#[allow(clippy::too_many_lines)]
async fn drain_buffered_schedule_runs(
    conn: &mut AsyncPgConnection,
    current_shard: ShardId,
    registered_dags: &DagCatalog,
    registry: &crate::worker::HandlerRegistry,
    metrics: &Arc<dyn crate::telemetry::MetricsRecorder>,
    active_gates: &[crate::admission_gate::AdmissionGate],
) -> HarvestResult<()> {
    use crate::schema::harvest_schedules::dsl;
    use diesel_async::RunQueryDsl;

    let now = Utc::now();

    // Query schedules that have buffered runs and are not paused or exhausted.
    let pending: Vec<HarvestSchedule> = dsl::harvest_schedules
        .filter(dsl::workflow_name.is_not_null())
        .filter(dsl::is_paused.eq(false))
        .filter(dsl::auto_paused_at.is_null())
        // Exhausted schedules (issue #478) must not drain buffered slots — the
        // schedule has reached its terminal state and no further runs should start.
        .filter(dsl::exhausted_at.is_null())
        .filter(diesel::dsl::sql::<diesel::sql_types::Bool>(
            "jsonb_array_length(buffered_runs) > 0",
        ))
        .select(HarvestSchedule::as_select())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    for schedule in pending {
        let Some(ref wf_name) = schedule.workflow_name else {
            continue;
        };

        // Skip DAG-backed schedules whose DAG is no longer registered so that
        // removing a DAG does not cause its stale buffered slots to be dispatched.
        if let Some(ref dag_name) = schedule.dag_name
            && !registered_dags.contains_key(dag_name)
        {
            tracing::debug!(
                workflow_name = %wf_name,
                dag_name = %dag_name,
                "harvest: skipping buffered drain for unregistered DAG"
            );
            continue;
        }

        let mut buffered = parse_buffered_runs(&schedule.buffered_runs);
        if buffered.is_empty() {
            continue;
        }

        let mut running: i64 = harvest_workflow_executions::table
            .filter(harvest_workflow_executions::workflow_name.eq(wf_name))
            .filter(harvest_workflow_executions::state.eq_any(["RUNNING", "PAUSED"]))
            .count()
            .get_result(conn)
            .await
            .map_err(crate::error::database_error)?;
        // A throttled fire durably defers before any execution row exists --
        // count it toward max_active_runs so this loop can't drain more
        // buffered slots than the schedule's true remaining capacity allows
        // while an earlier fire is still sitting in the throttle queue
        // (code review, issue #607).
        running += crate::throttle::pending_throttle_count_for_workflow(conn, wf_name).await?;

        let available = i64::from(schedule.max_active_runs).saturating_sub(running);
        if available <= 0 {
            continue;
        }

        let dispatch_queue = schedule.queue_name.as_deref().unwrap_or("default");

        // issue #377: gate check — skip draining this schedule if any active gate matches.
        {
            let dag_lookup_key = schedule.dag_name.as_deref().unwrap_or(wf_name.as_str());
            let owner = registry
                .workflows
                .get(wf_name.as_str())
                .and_then(|i| i.owner)
                .or_else(|| {
                    registered_dags
                        .get(dag_lookup_key)
                        .and_then(|d| d.owner.as_deref())
                });
            if let Some(gate) = crate::admission_gate::check_admission(
                active_gates,
                wf_name,
                dispatch_queue,
                current_shard.as_i32(),
                owner,
            ) {
                tracing::info!(
                    workflow_name = %wf_name,
                    gate_id = %gate.id,
                    reason = %gate.reason,
                    "harvest: buffered drain skipped due to admission gate"
                );
                metrics.record_schedule_skipped("workflow", wf_name, "admission_blocked");
                // issue #618, F-round17: also count the block in
                // harvest.admission.blocked (see the tick path above) so the
                // scheduler's buffered/overlap drain blocks appear like every other
                // gated producer's.
                metrics.record_admission_blocked(gate.scope.kind_str(), &gate.reason);
                continue;
            }
        }

        let mut dispatched: u32 = 0;
        // Set to true when the whole buffer is cleared because the first slot is already
        // past end_at. Used below to decide whether to exhaust the schedule even though
        // `buffered` is empty after the clear (normal empty-after-drain must not exhaust).
        let mut all_buffered_past_end_at = false;

        while dispatched < u32::try_from(available).unwrap_or(u32::MAX) && !buffered.is_empty() {
            let scheduled_for = buffered[0];

            // Per-slot end_at guard (issue #478): skip buffered slots past the cutoff.
            if let Some(end_at) = schedule.end_at
                && scheduled_for >= end_at
            {
                buffered.clear(); // all remaining buffered slots are also past end_at
                all_buffered_past_end_at = true;
                break;
            }
            // Budget cap (issue #478): don't let buffered drains exceed max_runs.
            if let Some(max_runs) = schedule.max_runs {
                let already = schedule
                    .runs_started
                    .saturating_add(i32::try_from(dispatched).unwrap_or(i32::MAX));
                if max_runs > 0 && already >= max_runs {
                    break;
                }
            }

            buffered.remove(0);
            let workflow_id = scheduled_workflow_id(schedule.id, wf_name, scheduled_for);
            let exec_id = if schedule.dag_name.is_some() {
                ExecutionId::new_for_shard(current_shard)
            } else {
                ExecutionId::new()
            };
            let input = schedule
                .workflow_input
                .clone()
                .unwrap_or(serde_json::Value::Null);
            let wf_info = registry.workflows.get(wf_name);
            let (concurrency_key, concurrency_limit) = wf_info
                .and_then(|info| info.concurrency.as_ref())
                .map_or((None, None), |policy| {
                    let key = crate::concurrency::resolve_concurrency_key(policy.key_expr, &input);
                    (key, Some(policy.limit))
                });
            let (owner, runbook_url, severity) = {
                let wf_meta = wf_info.map(|info| (info.owner, info.runbook_url, info.severity));
                let dag_meta = registered_dags.get(wf_name).map(|dag| {
                    (
                        dag.owner.as_deref(),
                        dag.runbook_url.as_deref(),
                        dag.severity.as_deref(),
                    )
                });
                match (wf_meta, dag_meta) {
                    (Some((o, r, s)), Some((dag_owner, dag_runbook, dag_severity))) => {
                        (o.or(dag_owner), r.or(dag_runbook), s.or(dag_severity))
                    }
                    (Some((o, r, s)), None) => (o, r, s),
                    (None, Some((dag_owner, dag_runbook, dag_severity))) => {
                        (dag_owner, dag_runbook, dag_severity)
                    }
                    (None, None) => (None, None, None),
                }
            };
            // Only workflows carry an SLA default; DAGs have no SLA concept.
            let sla = wf_info
                .and_then(|info| info.sla)
                .and_then(|d| chrono::Duration::from_std(d).ok());

            tracing::info!(
                workflow_name = %wf_name,
                workflow_id = %workflow_id,
                buffered_for = %scheduled_for,
                "harvest: dispatching buffered scheduled workflow run"
            );

            // Effective per-workflow input cap (issue #607 code review): this
            // loop previously enforced no cap at all on either the throttle or
            // immediate path (`None`/`0` are both "no cap" sentinels to
            // `StartWorkflowParams`/`DebounceStartOptions`). Mirrors the
            // scheduler-tick path's `effective_cap` computation.
            let effective_cap = wf_info
                .and_then(|info| info.max_input_bytes)
                .map_or(registry.max_workflow_input_bytes, |per| {
                    per.max(registry.max_workflow_input_bytes)
                });

            // Start-throttle admission (issue #607): pace buffered/backfilled fires,
            // defer the excess. A deferred fire counts as dispatched (advances the
            // slot) and is admitted later by the throttle scanner with its
            // schedule_id/scheduled_for/origin preserved for carryover (#488).
            let mut buffered_throttle_bucket: Option<String> = None;
            if let Some(throttle_policy) = wf_info.and_then(|info| info.throttle) {
                let throttle_key = throttle_policy.key_expr.map_or_else(
                    || Some(String::new()),
                    |k| crate::throttle::resolve_throttle_key(k, &input),
                );
                if let Some(resolved_throttle_key) = throttle_key {
                    // Fail fast on an oversized input rather than persisting a
                    // pending row that would fail at fire time on every
                    // scanner tick. `break` (not `return Err`) matches this
                    // loop's own failure-handling convention below: drop this
                    // and any remaining buffered slots this tick rather than
                    // retrying a permanently-failing input forever. Skipped
                    // when `reserve_or_defer` would resolve via `Bypassed` or
                    // an idempotent attach to an already-pending row.
                    let skip_cap_check = crate::throttle::skip_size_check(
                        conn,
                        wf_name,
                        &workflow_id,
                        Some("reject_duplicate"),
                    )
                    .await?;
                    if !skip_cap_check && effective_cap > 0 {
                        let observed =
                            serde_json::to_string(&input).map_or(0u64, |s| s.len() as u64);
                        if observed > effective_cap {
                            tracing::warn!(
                                workflow_name = %wf_name,
                                workflow_id = %workflow_id,
                                buffered_for = %scheduled_for,
                                observed_bytes = observed,
                                cap_bytes = effective_cap,
                                "harvest: buffered scheduled workflow input exceeds cap; dropping slot"
                            );
                            break;
                        }
                    }
                    let effective_retry = schedule
                        .retry_policy
                        .as_ref()
                        .and_then(|v| {
                            serde_json::from_value::<crate::policy::RetryPolicy>(v.clone()).ok()
                        })
                        .or_else(|| wf_info.and_then(|info| info.retry_policy.clone()))
                        .and_then(|p| serde_json::to_value(&p).ok());
                    let start_options = crate::debounce::DebounceStartOptions {
                        reuse_policy: Some("reject_duplicate".to_string()),
                        execution_timeout_secs: None,
                        memo: None,
                        search_attrs: None,
                        sla_secs: sla.map(|d| d.num_seconds()),
                        context_headers: None,
                        priority: None,
                        concurrency_key: concurrency_key.clone(),
                        concurrency_limit,
                        owner: owner.map(str::to_string),
                        runbook_url: runbook_url.map(str::to_string),
                        severity: severity.map(str::to_string),
                        max_execution_timeout_ceiling_secs: None,
                        max_workflow_input_bytes: Some(effective_cap),
                        trace_context: None,
                        workflow_retry_policy: effective_retry,
                        max_workflow_attempts_ceiling: registry.max_workflow_attempts_ceiling,
                        completion_callbacks: None,
                        schedule_id: Some(schedule.id),
                        scheduled_for: Some(scheduled_for),
                        origin: Some(crate::execution::ORIGIN_SCHEDULED.to_string()),
                    };
                    match crate::throttle::reserve_or_defer(
                        conn,
                        crate::throttle::AdmitThrottleParams {
                            workflow_name: wf_name,
                            throttle_key: &resolved_throttle_key,
                            workflow_id: &workflow_id,
                            queue_name: dispatch_queue,
                            input: input.clone(),
                            start_options,
                            refill_per_sec: throttle_policy.refill_per_sec,
                            burst: throttle_policy.burst,
                            schedule_to_start: throttle_policy.schedule_to_start,
                            shard_id: current_shard.as_i32(),
                        },
                    )
                    .await
                    {
                        Ok(crate::throttle::ThrottleAdmission::Deferred(_)) => {
                            metrics.record_start_throttled(wf_name);
                            dispatched += 1;
                            continue;
                        }
                        Ok(crate::throttle::ThrottleAdmission::Reserved { bucket_key }) => {
                            buffered_throttle_bucket = Some(bucket_key);
                        }
                        Ok(crate::throttle::ThrottleAdmission::Bypassed) => {
                            // Active execution already resolves this reuse policy as a
                            // no-op/immediate reject; no token reserved, fall through to
                            // the normal start below.
                        }
                        Err(e) => return Err(e),
                    }
                }
            }

            let start_result = crate::execution::start_or_load_workflow_execution(
                conn,
                crate::execution::StartWorkflowParams {
                    workflow_name: wf_name,
                    workflow_id: &workflow_id,
                    exec_id,
                    input,
                    parent_id: None,
                    queue_name: dispatch_queue,
                    execution_timeout: None,
                    memo: None,
                    search_attrs: None,
                    reuse_policy: scheduled_workflow_reuse_policy(),
                    trace_context: None,
                    max_execution_timeout_ceiling: None,
                    concurrency_key,
                    concurrency_limit,
                    priority: Priority::default(),
                    max_workflow_input_bytes: effective_cap,
                    start_at: None,
                    delay: None,
                    max_workflow_start_delay: None,
                    owner,
                    runbook_url,
                    severity,
                    context_headers: None,
                    sla,
                    schedule_id: Some(schedule.id),
                    scheduled_for: Some(scheduled_for),
                    workflow_attempt: 1,
                    workflow_retry_policy: schedule
                        .retry_policy
                        .as_ref()
                        .and_then(|v| serde_json::from_value(v.clone()).ok())
                        .or_else(|| wf_info.and_then(|info| info.retry_policy.clone())),
                    retry_of_exec_id: None,
                    max_workflow_attempts_ceiling: registry.max_workflow_attempts_ceiling,
                    // Normal scheduler-tick fire — attributed as the schedule's cadence (issue #534).
                    origin: Some(crate::execution::ORIGIN_SCHEDULED),
                    completion_callbacks: None,
                },
            )
            .await;

            match scheduled_start_outcome(start_result) {
                Ok(outcome) => {
                    dispatched += 1;
                    if outcome.created() {
                        metrics.record_schedule_run("workflow", wf_name);
                    } else if let Some(ref bucket) = buffered_throttle_bucket {
                        // AC-a: RejectDuplicate returned an existing run — refund.
                        let _ = crate::queue::refund_rate_limit_token(conn, bucket).await;
                    }
                    tracing::info!(
                        workflow_name = %wf_name,
                        execution_id = %outcome.exec_id(),
                        state = %outcome.state(),
                        created = outcome.created(),
                        "harvest: buffered scheduled workflow run dispatched"
                    );
                    crate::schedule_decision::record_decision_graceful(
                        conn,
                        Some(&**metrics),
                        Some(schedule.id),
                        wf_name,
                        "workflow",
                        "fired",
                        "fired_ok",
                        Some(serde_json::json!({
                            "execution_id": outcome.exec_id(),
                            "state": outcome.state().to_string(),
                            "created": outcome.created(),
                            "buffered": true,
                        })),
                        now,
                        schedule.next_run_at.unwrap_or(now),
                        i16::try_from(current_shard.as_i32()).unwrap_or(0),
                    )
                    .await;
                }
                Err(error) => {
                    // No run admitted — refund the reserved throttle token.
                    if let Some(ref bucket) = buffered_throttle_bucket {
                        let _ = crate::queue::refund_rate_limit_token(conn, bucket).await;
                    }
                    // Drop the failing slot rather than re-inserting it. Re-queuing a
                    // permanently-failing slot (e.g. deleted workflow, bad input) would
                    // create an infinite retry loop on every scheduler tick. Transient
                    // failures are rare for buffered slots (same path as normal dispatch);
                    // if they occur the schedule's regular tick will generate fresh firings.
                    tracing::warn!(
                        error = %error,
                        workflow_name = %wf_name,
                        workflow_id = %workflow_id,
                        buffered_for = %scheduled_for,
                        "harvest: failed to dispatch buffered workflow run; dropping slot"
                    );
                    break;
                }
            }
        }

        // Persist the updated buffer and budget accounting (issue #478).
        let dispatched_i32 = i32::try_from(dispatched).unwrap_or(i32::MAX);
        let new_runs_started = schedule.runs_started.saturating_add(dispatched_i32);
        let budget_exhausted = dispatched > 0
            && schedule
                .max_runs
                .is_some_and(|max| max > 0 && new_runs_started >= max);
        let end_at_exhausted = schedule.end_at.is_some_and(|end| {
            if all_buffered_past_end_at {
                // The entire buffer was cleared because every slot was past end_at.
                // Exhaust only when the schedule's regular next_run_at is also at/past
                // the cutoff (or absent). If next_run_at is still before end_at, the
                // regular tick fires inside the window and the drain must not exhaust.
                schedule.next_run_at.is_none_or(|next| next >= end)
            } else {
                // Only exhaust from the drain when *remaining* buffered slots are all
                // past the cutoff. An empty buffer means capacity opened and the drain
                // completed normally — the regular tick detects end_at on next_run_at.
                !buffered.is_empty() && buffered.iter().all(|&t| t >= end)
            }
        });
        let any_drain_exhausted = budget_exhausted || end_at_exhausted;
        let exhausted_reason: Option<&str> = if budget_exhausted {
            Some("max_runs_exhausted")
        } else if end_at_exhausted {
            Some("end_at_reached")
        } else {
            None
        };
        // Use two separate UPDATE paths so the non-exhausting path never writes
        // NULL for exhausted_at/exhausted_reason, which would silently undo a
        // concurrent exhaustion set by another HA replica (issue #478).
        if any_drain_exhausted {
            diesel::update(dsl::harvest_schedules.find(schedule.id))
                .set((
                    dsl::buffered_runs.eq(buffered_runs_to_json(&buffered)),
                    dsl::runs_started.eq(new_runs_started),
                    dsl::exhausted_at.eq(Some(now)),
                    dsl::exhausted_reason.eq(exhausted_reason),
                    dsl::next_run_at.eq(Option::<DateTime<Utc>>::None),
                    dsl::updated_at.eq(now),
                ))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
        } else {
            // Guard on exhausted_at IS NULL so a concurrent exhaustion is never
            // overwritten. The row may have been exhausted by the regular tick or
            // another drain between the SELECT above and this UPDATE.
            // Use a DB-side increment so concurrent manual trigger pre-increments
            // are preserved rather than overwritten by this stale in-memory value.
            diesel::update(
                dsl::harvest_schedules
                    .find(schedule.id)
                    .filter(dsl::exhausted_at.is_null()),
            )
            .set((
                dsl::buffered_runs.eq(buffered_runs_to_json(&buffered)),
                dsl::runs_started.eq(dsl::runs_started + dispatched_i32),
                dsl::updated_at.eq(now),
            ))
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;
        }
    }

    Ok(())
}

// ── Worker-completion helpers (issue #360) ────────────────────────────────────
//
// These functions are called from the worker completion path (`worker.rs`) after
// a schedule-triggered execution reaches a terminal state.  They are best-effort:
// errors are logged but never propagated back to the task loop — the workflow task
// result has already been committed at that point.
//
// Only executions whose `workflow_id` starts with `"sched:"` are considered
// schedule-triggered.  All other executions are ignored.

/// Increment the consecutive failure counter for the schedule associated with
/// `workflow_name` when a schedule-triggered execution reaches `FAILED` or
/// `TIMED_OUT`.  If the counter now equals or exceeds the configured limit,
/// auto-pause the schedule and emit the `harvest.schedule.auto_paused` metric.
///
/// `schedule_id` overrides the `workflow_id`-prefix heuristic when the caller
/// already has the schedule UUID (e.g. retry executions whose `workflow_id` does
/// not start with `"sched:"`).
#[cfg(feature = "db")]
#[allow(clippy::too_many_lines)]
pub(crate) async fn maybe_increment_schedule_failure_counter(
    conn: &mut diesel_async::AsyncPgConnection,
    workflow_id: &str,
    workflow_name: &str,
    schedule_id: Option<uuid::Uuid>,
    origin: Option<&str>,
    metrics: &dyn crate::telemetry::MetricsRecorder,
) {
    use crate::schema::harvest_schedules::dsl;

    // Only scheduled-cadence failures count toward the auto-pause threshold.
    // Backfill and manual-trigger runs are deliberately excluded: a backfill
    // storm or an operator ad-hoc fire should not trip the consecutive-failure
    // circuit breaker.  NULL origin is treated as scheduled (legacy rows
    // pre-dating the origin column, or the quarantine path which lacks
    // execution context).
    if matches!(origin, Some(o) if o != crate::execution::ORIGIN_SCHEDULED) {
        return;
    }

    // Retry executions carry an explicit `schedule_id`; original scheduled
    // executions embed the UUID in the `workflow_id` prefix.  Bail out only
    // when neither source provides a schedule reference.
    if schedule_id.is_none() && !workflow_id.starts_with("sched:") {
        return;
    }

    // Extract the schedule UUID from the explicit field or from the
    // `workflow_id` prefix ("sched:{schedule_uuid}:{workflow_name}:{ts}").
    let schedule_uuid: Option<uuid::Uuid> = schedule_id.or_else(|| {
        workflow_id
            .strip_prefix("sched:")
            .and_then(|s| s.split(':').next())
            .and_then(|s| uuid::Uuid::parse_str(s).ok())
    });

    let now = Utc::now();

    // Resolve the schedule IDs to update.  When the workflow_id encodes a schedule
    // UUID (new format) we target that row directly.  For legacy workflow_ids we fall
    // back to a workflow_name scan so old in-flight executions are still counted.
    let ids_to_update: Vec<uuid::Uuid> = if let Some(sid) = schedule_uuid {
        vec![sid]
    } else {
        match dsl::harvest_schedules
            .filter(dsl::workflow_name.eq(workflow_name))
            .filter(dsl::consecutive_failure_limit.is_not_null())
            .filter(dsl::consecutive_failure_limit.gt(0))
            .filter(dsl::auto_paused_at.is_null())
            .select(dsl::id)
            .load(conn)
            .await
            .map_err(crate::error::database_error)
        {
            Ok(ids) => ids,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    workflow_name,
                    "harvest: failed to load schedule ids for failure counter increment"
                );
                return;
            }
        }
    };

    for id in ids_to_update {
        // Atomic SQL increment — avoids the read-modify-write race when two scheduled
        // executions for the same schedule fail concurrently.
        let incremented: Option<(i32, Option<i32>)> = diesel::update(
            dsl::harvest_schedules
                .find(id)
                .filter(dsl::consecutive_failure_limit.is_not_null())
                .filter(dsl::consecutive_failure_limit.gt(0))
                .filter(dsl::auto_paused_at.is_null()),
        )
        .set((
            dsl::consecutive_failure_count.eq(dsl::consecutive_failure_count + 1),
            dsl::updated_at.eq(now),
        ))
        .returning((
            dsl::consecutive_failure_count,
            dsl::consecutive_failure_limit,
        ))
        .get_result(conn)
        .await
        .optional()
        .unwrap_or_else(|e| {
            tracing::warn!(
                error = %e,
                workflow_name,
                schedule_id = %id,
                "harvest: failed to increment schedule failure counter"
            );
            None
        });

        let Some((new_count, Some(limit))) = incremented else {
            continue;
        };

        if limit > 0 && new_count >= limit {
            // Transition to auto-paused.  The filter guards against a double-set when
            // two concurrent failures both cross the threshold at the same time.
            let pause_result = diesel::update(
                dsl::harvest_schedules
                    .find(id)
                    .filter(dsl::auto_paused_at.is_null()),
            )
            .set(dsl::auto_paused_at.eq(Some(now)))
            .execute(conn)
            .await;

            match pause_result {
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        workflow_name,
                        schedule_id = %id,
                        "harvest: failed to set auto_paused_at on schedule"
                    );
                }
                Ok(rows_set) if rows_set > 0 => {
                    tracing::info!(
                        workflow_name,
                        schedule_id = %id,
                        consecutive_failure_count = new_count,
                        consecutive_failure_limit = limit,
                        "harvest: schedule auto-paused after consecutive execution failures"
                    );
                    metrics.record_schedule_auto_paused(workflow_name);
                }
                Ok(_) => {}
            }
        }
    }
}

/// Reset the consecutive failure counter to zero for the schedule associated with
/// `workflow_name` when a schedule-triggered execution reaches `COMPLETED`.
/// Also clears `auto_paused_at` so the schedule resumes firing automatically.
///
/// `schedule_id` overrides the `workflow_id`-prefix heuristic when the caller
/// already has the schedule UUID (e.g. retry executions whose `workflow_id` does
/// not start with `"sched:"`).
#[cfg(feature = "db")]
pub(crate) async fn maybe_reset_schedule_failure_counter(
    conn: &mut diesel_async::AsyncPgConnection,
    workflow_id: &str,
    workflow_name: &str,
    schedule_id: Option<uuid::Uuid>,
    origin: Option<&str>,
) {
    use crate::schema::harvest_schedules::dsl;

    // Only reset on scheduled-cadence successes (mirrors the increment guard).
    if matches!(origin, Some(o) if o != crate::execution::ORIGIN_SCHEDULED) {
        return;
    }

    if schedule_id.is_none() && !workflow_id.starts_with("sched:") {
        return;
    }

    let schedule_uuid: Option<uuid::Uuid> = schedule_id.or_else(|| {
        workflow_id
            .strip_prefix("sched:")
            .and_then(|s| s.split(':').next())
            .and_then(|s| uuid::Uuid::parse_str(s).ok())
    });

    let now = Utc::now();
    let result = if let Some(sid) = schedule_uuid {
        diesel::update(
            dsl::harvest_schedules.find(sid).filter(
                dsl::consecutive_failure_count
                    .gt(0)
                    .or(dsl::auto_paused_at.is_not_null()),
            ),
        )
        .set((
            dsl::consecutive_failure_count.eq(0),
            dsl::auto_paused_at.eq(Option::<DateTime<Utc>>::None),
            dsl::updated_at.eq(now),
        ))
        .execute(conn)
        .await
    } else {
        diesel::update(
            dsl::harvest_schedules
                .filter(dsl::workflow_name.eq(workflow_name))
                .filter(dsl::consecutive_failure_limit.is_not_null())
                .filter(
                    dsl::consecutive_failure_count
                        .gt(0)
                        .or(dsl::auto_paused_at.is_not_null()),
                ),
        )
        .set((
            dsl::consecutive_failure_count.eq(0),
            dsl::auto_paused_at.eq(Option::<DateTime<Utc>>::None),
            dsl::updated_at.eq(now),
        ))
        .execute(conn)
        .await
    };

    if let Err(e) = result {
        tracing::warn!(
            error = %e,
            workflow_name,
            "harvest: failed to reset schedule failure counter on completion"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A representative workflow-schedule row for `merge_schedule_patch` unit
    /// tests (no database required).
    fn merge_base_row() -> HarvestSchedule {
        let now = Utc::now();
        HarvestSchedule {
            id: uuid::Uuid::new_v4(),
            dag_name: None,
            schedule_expr: Some("interval:3600".to_string()),
            timezone: "UTC".to_string(),
            catchup: false,
            max_active_runs: 2,
            is_paused: false,
            last_run_at: None,
            next_run_at: Some(now + chrono::Duration::seconds(3600)),
            created_at: now,
            updated_at: now,
            workflow_name: Some("merge_wf".to_string()),
            workflow_input: Some(serde_json::json!({"env": "A"})),
            queue_name: Some("etl".to_string()),
            paused_at: None,
            paused_by: None,
            pause_reason: None,
            jitter_secs: 30,
            overlap_policy: "buffer_one".to_string(),
            buffered_runs: serde_json::json!([]),
            buffer_all_max: 7,
            calendar_name: Some("us-holidays".to_string()),
            skip_policy: "run_next_business_day".to_string(),
            fire_claim_token: None,
            fire_claimed_until: None,
            consecutive_failure_limit: Some(4),
            consecutive_failure_count: 0,
            auto_paused_at: None,
            end_at: Some(now + chrono::Duration::days(30)),
            max_runs: Some(9),
            runs_started: 3,
            exhausted_at: None,
            exhausted_reason: None,
            catchup_policy: None,
            catchup_window_secs: None,
            last_catchup_dropped: 0,
            last_catchup_at: None,
            retry_policy: None,
        }
    }

    /// An empty patch round-trips every stored value (only-provided-fields-
    /// change semantics: nothing provided, nothing changes).
    #[test]
    fn merge_schedule_patch_empty_patch_round_trips() {
        let row = merge_base_row();
        let merged = merge_schedule_patch(&row, &WorkflowSchedulePatch::default()).expect("merge");

        assert_eq!(merged.workflow_name, "merge_wf");
        assert_eq!(
            schedule_expr(Some(&merged.schedule)).as_deref(),
            Some("interval:3600"),
            "the stored expression must round-trip so next_run_at is preserved"
        );
        assert_eq!(merged.input, serde_json::json!({"env": "A"}));
        assert_eq!(merged.queue_name, "etl");
        assert!(!merged.catchup);
        assert_eq!(merged.max_active_runs, 2);
        assert_eq!(merged.jitter, Duration::from_secs(30));
        assert_eq!(merged.overlap_policy, OverlapPolicy::BufferOne);
        assert_eq!(merged.buffer_all_max, 7);
        assert_eq!(merged.calendar.as_deref(), Some("us-holidays"));
        assert_eq!(
            merged.skip_policy,
            crate::policy::SkipPolicy::RunNextBusinessDay
        );
        assert_eq!(merged.consecutive_failure_limit, Some(4));
        assert_eq!(merged.end_at, row.end_at);
        assert_eq!(merged.max_runs, Some(9));
        assert!(
            merged.catchup_policy.is_none(),
            "a NULL-policy row must stay legacy-bool, never converted to an explicit policy"
        );
        assert!(merged.retry_policy.is_none());
    }

    /// Provided fields override; everything else keeps the stored value.
    #[test]
    fn merge_schedule_patch_overrides_only_provided_fields() {
        let row = merge_base_row();
        let patch = WorkflowSchedulePatch {
            schedule: Some(Schedule::Cron("0 3 * * *".to_string())),
            input: Some(serde_json::json!({"env": "B"})),
            max_active_runs: Some(5),
            ..Default::default()
        };
        let merged = merge_schedule_patch(&row, &patch).expect("merge");

        assert_eq!(
            schedule_expr(Some(&merged.schedule)).as_deref(),
            Some("cron:0 3 * * *")
        );
        assert_eq!(merged.input, serde_json::json!({"env": "B"}));
        assert_eq!(merged.max_active_runs, 5);
        // Untouched fields keep the stored values.
        assert_eq!(merged.queue_name, "etl");
        assert_eq!(merged.overlap_policy, OverlapPolicy::BufferOne);
        assert_eq!(merged.calendar.as_deref(), Some("us-holidays"));
    }

    /// Tri-state fields: `Some(None)` clears, outer `None` preserves.
    #[test]
    fn merge_schedule_patch_tristate_clear_vs_absent() {
        let row = merge_base_row();
        let patch = WorkflowSchedulePatch {
            calendar: Some(None),
            end_at: Some(None),
            consecutive_failure_limit: Some(None),
            ..Default::default()
        };
        let merged = merge_schedule_patch(&row, &patch).expect("merge");
        assert!(merged.calendar.is_none(), "Some(None) must clear calendar");
        assert!(merged.end_at.is_none(), "Some(None) must clear end_at");
        assert!(merged.consecutive_failure_limit.is_none());
        // Absent tri-state fields are preserved.
        assert_eq!(merged.max_runs, Some(9));
    }

    /// `max_runs = 0` is normalized to "no limit" on both patch arms.
    #[test]
    fn merge_schedule_patch_normalizes_zero_max_runs_to_none() {
        let row = merge_base_row();
        let patch = WorkflowSchedulePatch {
            max_runs: Some(Some(0)),
            ..Default::default()
        };
        let merged = merge_schedule_patch(&row, &patch).expect("merge");
        assert!(merged.max_runs.is_none(), "0 must normalize to None");

        let mut zero_row = merge_base_row();
        zero_row.max_runs = Some(0);
        let merged =
            merge_schedule_patch(&zero_row, &WorkflowSchedulePatch::default()).expect("merge");
        assert!(merged.max_runs.is_none(), "stored 0 must normalize to None");
    }

    /// A stored `"manual"` (or NULL/unparseable) expression merges to
    /// `Schedule::Manual`, matching the tick loop's lenient parse rules.
    #[test]
    fn merge_schedule_patch_manual_and_unparseable_exprs_map_to_manual() {
        let mut row = merge_base_row();
        row.schedule_expr = Some("manual".to_string());
        let merged = merge_schedule_patch(&row, &WorkflowSchedulePatch::default()).expect("merge");
        assert!(matches!(merged.schedule, Schedule::Manual));

        row.schedule_expr = None;
        let merged = merge_schedule_patch(&row, &WorkflowSchedulePatch::default()).expect("merge");
        assert!(matches!(merged.schedule, Schedule::Manual));
    }

    /// A row with an explicit stored catchup policy reconstructs it (window
    /// included); a patch clearing it falls back to the legacy bool.
    #[test]
    fn merge_schedule_patch_reconstructs_and_clears_catchup_policy() {
        let mut row = merge_base_row();
        row.catchup_policy = Some("window".to_string());
        row.catchup_window_secs = Some(7200);
        let merged = merge_schedule_patch(&row, &WorkflowSchedulePatch::default()).expect("merge");
        assert_eq!(
            merged.catchup_policy,
            Some(crate::policy::CatchupPolicy::Window(Duration::from_secs(
                7200
            )))
        );

        let patch = WorkflowSchedulePatch {
            catchup_policy: Some(None),
            ..Default::default()
        };
        let merged = merge_schedule_patch(&row, &patch).expect("merge");
        assert!(merged.catchup_policy.is_none(), "Some(None) must clear");
    }

    /// A row without a `workflow_name` is not a workflow schedule.
    #[test]
    fn merge_schedule_patch_rejects_rows_without_workflow_name() {
        let mut row = merge_base_row();
        row.workflow_name = None;
        let err =
            merge_schedule_patch(&row, &WorkflowSchedulePatch::default()).expect_err("must reject");
        assert!(matches!(err, HarvestError::Config(_)));
    }

    /// A stored `retry_policy` JSON blob that fails to deserialize must fail
    /// an UNRELATED patch loudly instead of being silently round-tripped to
    /// NULL (which would destroy the stored policy as a side effect of, say,
    /// an input-only edit).
    #[test]
    fn merge_schedule_patch_corrupt_stored_retry_policy_errors_on_unrelated_patch() {
        let mut row = merge_base_row();
        row.retry_policy = Some(serde_json::json!({"not": "a retry policy"}));
        let patch = WorkflowSchedulePatch {
            input: Some(serde_json::json!({"unrelated": true})),
            ..Default::default()
        };
        let err = merge_schedule_patch(&row, &patch)
            .expect_err("corrupt stored retry_policy must not be silently dropped");
        match err {
            HarvestError::Config(msg) => {
                assert!(
                    msg.contains("retry_policy"),
                    "error must name retry_policy: {msg}"
                );
            }
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    /// The corrupt-row error is repairable: a patch that explicitly sets or
    /// clears `retry_policy` bypasses the stored blob entirely.
    #[test]
    fn merge_schedule_patch_corrupt_stored_retry_policy_repairable_by_set_or_clear() {
        let mut row = merge_base_row();
        row.retry_policy = Some(serde_json::json!({"not": "a retry policy"}));

        let clear = WorkflowSchedulePatch {
            retry_policy: Some(None),
            ..Default::default()
        };
        let merged = merge_schedule_patch(&row, &clear).expect("explicit clear must repair");
        assert!(merged.retry_policy.is_none());

        let replacement =
            crate::policy::RetryPolicy::exponential(3, std::time::Duration::from_secs(1));
        let set = WorkflowSchedulePatch {
            retry_policy: Some(Some(replacement.clone())),
            ..Default::default()
        };
        let merged = merge_schedule_patch(&row, &set).expect("explicit set must repair");
        assert_eq!(
            serde_json::to_value(merged.retry_policy).unwrap(),
            serde_json::to_value(Some(replacement)).unwrap()
        );
    }

    #[cfg(all(feature = "db", not(feature = "unified-dag-execution")))]
    fn test_pool(database_url: &str) -> DbPool {
        let manager = diesel_async::pooled_connection::AsyncDieselConnectionManager::<
            diesel_async::AsyncPgConnection,
        >::new(database_url);
        deadpool::managed::Pool::builder(manager)
            .max_size(1)
            .build()
            .expect("test pool should build")
    }

    #[cfg(all(feature = "db", not(feature = "unified-dag-execution")))]
    fn classic_scheduled_dag_info() -> DagInfo {
        fn build(_dag: &mut crate::dag::DagBuilder) {}

        DagInfo {
            name: "classic_scheduled",
            module: "tests",
            schedule: Some(Schedule::Interval(Duration::from_secs(60))),
            catchup: false,
            max_active_runs: 1,
            default_queue: Some("default"),
            builder: build,
            workflow_handler: None,
            jitter: ::std::time::Duration::ZERO,
            overlap_policy: crate::policy::OverlapPolicy::Skip,
            buffer_all_max: 100,
            owner: None,
            runbook_url: None,
            severity: None,
            mcp: false,
        }
    }

    fn parse_utc(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .expect("timestamp should parse")
            .with_timezone(&Utc)
    }

    #[test]
    fn due_run_plan_without_catchup_keeps_next_live_slot() {
        let schedule = Schedule::Interval(Duration::from_secs(60));
        let first_due = parse_utc("2026-04-06T12:00:00Z");
        let now = parse_utc("2026-04-06T12:05:00Z");

        let (created, next_run_at) = due_run_plan(Some(&schedule), first_due, now, false);

        assert_eq!(created, vec![first_due]);
        assert_eq!(next_run_at, Some(parse_utc("2026-04-06T12:06:00Z")));
    }

    #[test]
    fn due_run_plan_without_catchup_anchors_next_slot_to_first_due() {
        // When now is only slightly past first_due (e.g. jitter delay of 3 min
        // inside a 60-min interval), next slot must be first_due + period, not
        // now + period, so the schedule doesn't drift on every fired slot.
        let schedule = Schedule::Interval(Duration::from_secs(3600));
        let first_due = parse_utc("2026-04-06T10:00:00Z");
        let now = parse_utc("2026-04-06T10:03:00Z"); // 3 min jitter delay

        let (created, next_run_at) = due_run_plan(Some(&schedule), first_due, now, false);

        assert_eq!(created, vec![first_due]);
        // Should be 11:00, not 11:03
        assert_eq!(next_run_at, Some(parse_utc("2026-04-06T11:00:00Z")));
    }

    #[test]
    fn due_run_plan_with_catchup_stops_at_first_future_slot() {
        let schedule = Schedule::Interval(Duration::from_secs(60));
        let first_due = parse_utc("2026-04-06T12:00:00Z");
        let now = parse_utc("2026-04-06T12:02:30Z");

        let (created, next_run_at) = due_run_plan(Some(&schedule), first_due, now, true);

        assert_eq!(
            created,
            vec![
                parse_utc("2026-04-06T12:00:00Z"),
                parse_utc("2026-04-06T12:01:00Z"),
                parse_utc("2026-04-06T12:02:00Z")
            ]
        );
        assert_eq!(next_run_at, Some(parse_utc("2026-04-06T12:03:00Z")));
    }

    // ── catchup_run_plan ──────────────────────────────────────────────────────
    // These tests are the red-phase assertions for issue #484.  They compile
    // once `catchup_run_plan` and `CatchupPlan` are added below `due_run_plan`.

    #[test]
    fn catchup_run_plan_skip_all_fires_first_slot_only() {
        use crate::policy::CatchupPolicy;
        let schedule = Schedule::Interval(Duration::from_secs(60));
        let first_due = parse_utc("2026-04-06T12:00:00Z");
        let now = parse_utc("2026-04-06T12:05:00Z"); // 5 missed slots

        let plan = catchup_run_plan(
            Some(&schedule),
            first_due,
            now,
            CatchupPolicy::SkipAll,
            None,
        );

        // Identical to due_run_plan(false): one slot, zero drops. next_run_at
        // is anchored to first_due + period (12:01), but since that is already
        // behind `now` (12:05) it advances to the first future slot (12:06).
        assert_eq!(plan.run_dates, vec![first_due]);
        assert_eq!(plan.dropped, 0, "SkipAll should not record drops");
        assert_eq!(plan.next_run_at, Some(parse_utc("2026-04-06T12:06:00Z")));
    }

    #[test]
    fn catchup_run_plan_unbounded_fires_all_slots() {
        use crate::policy::CatchupPolicy;
        let schedule = Schedule::Interval(Duration::from_secs(60));
        let first_due = parse_utc("2026-04-06T12:00:00Z");
        let now = parse_utc("2026-04-06T12:02:30Z");

        let plan = catchup_run_plan(
            Some(&schedule),
            first_due,
            now,
            CatchupPolicy::Unbounded,
            None,
        );

        assert_eq!(
            plan.run_dates,
            vec![
                parse_utc("2026-04-06T12:00:00Z"),
                parse_utc("2026-04-06T12:01:00Z"),
                parse_utc("2026-04-06T12:02:00Z"),
            ]
        );
        assert_eq!(plan.dropped, 0, "Unbounded should not drop any slots");
        assert_eq!(plan.next_run_at, Some(parse_utc("2026-04-06T12:03:00Z")));
    }

    #[test]
    fn catchup_run_plan_most_recent_fires_exactly_one_newest_slot() {
        use crate::policy::CatchupPolicy;
        let schedule = Schedule::Interval(Duration::from_secs(60 * 15)); // 15-min
        let now = parse_utc("2026-04-06T12:00:00Z");
        // Simulate 24-hour outage: first_due = 24h ago.
        let first_due = now - chrono::Duration::hours(24);

        let plan = catchup_run_plan(
            Some(&schedule),
            first_due,
            now,
            CatchupPolicy::MostRecent,
            None,
        );

        // 24h / 15m = 96 intervals; the slot at exactly `now` is also due, so
        // 97 total missed slots. Fire exactly 1 (the most recent).
        assert_eq!(plan.run_dates.len(), 1, "MostRecent fires exactly one slot");
        // The fired slot must be the most recent (the one closest to now).
        let fired = plan.run_dates[0];
        let next_after_fired = next_run_after(Some(&schedule), fired).unwrap_or(now);
        assert!(
            next_after_fired > now || (now - fired) < chrono::Duration::minutes(15),
            "fired slot {fired} should be the most recent: next={next_after_fired} now={now}"
        );
        // 97 slots total, 1 fired, 96 dropped.
        assert_eq!(plan.dropped, 96, "MostRecent should drop 96 of 97 slots");
        assert!(plan.next_run_at.is_some_and(|t| t > now));
    }

    #[test]
    fn catchup_run_plan_most_recent_with_single_slot_has_zero_drops() {
        use crate::policy::CatchupPolicy;
        let schedule = Schedule::Interval(Duration::from_secs(3600));
        let first_due = parse_utc("2026-04-06T12:00:00Z");
        let now = parse_utc("2026-04-06T12:30:00Z"); // only one slot due

        let plan = catchup_run_plan(
            Some(&schedule),
            first_due,
            now,
            CatchupPolicy::MostRecent,
            None,
        );

        assert_eq!(plan.run_dates, vec![first_due]);
        assert_eq!(plan.dropped, 0, "single slot ⇒ no drops");
    }

    #[test]
    fn catchup_run_plan_window_fires_in_window_drops_older() {
        use crate::policy::CatchupPolicy;
        let schedule = Schedule::Interval(Duration::from_secs(60 * 15)); // 15-min
        let now = parse_utc("2026-04-06T12:00:00Z");
        let first_due = now - chrono::Duration::hours(24); // 96 missed slots

        // 1-hour window: only the ~4 slots in the last hour fire.
        let window = Duration::from_secs(3600);
        let plan = catchup_run_plan(
            Some(&schedule),
            first_due,
            now,
            CatchupPolicy::Window(window),
            None,
        );

        // All fired slots must be >= now - window.
        let cutoff = now - chrono::Duration::seconds(3600);
        for slot in &plan.run_dates {
            assert!(
                *slot >= cutoff,
                "slot {slot} is before the catchup cutoff {cutoff}"
            );
        }
        // Total missed = 97 (96 intervals + the slot at exactly now). Slots in
        // the 1-hour window (>= 11:00): 11:00, 11:15, 11:30, 11:45, 12:00 = 5.
        // Exactly fired + dropped = 97.
        let total = plan.run_dates.len() as u64 + plan.dropped;
        assert_eq!(total, 97, "fired + dropped must equal total missed slots");
        assert!(
            plan.dropped > 0,
            "Window(1h) should drop the older 92 slots"
        );
        assert!(
            plan.run_dates.len() <= 5,
            "Window(1h) fires at most 5 of 97 slots"
        );
    }

    #[test]
    fn catchup_run_plan_window_zero_fires_only_slot_exactly_at_now() {
        use crate::policy::CatchupPolicy;
        let schedule = Schedule::Interval(Duration::from_secs(3600));
        let first_due = parse_utc("2026-04-06T12:00:00Z");
        let now = parse_utc("2026-04-06T12:00:00Z"); // exactly at first_due

        let plan = catchup_run_plan(
            Some(&schedule),
            first_due,
            now,
            CatchupPolicy::Window(Duration::ZERO),
            None,
        );

        // A zero window fires the slot at exactly `first_due = now`.
        assert!(!plan.run_dates.is_empty(), "slot at now should still fire");
    }

    #[test]
    fn catchup_run_plan_window_cron_jump_starts_at_cutoff() {
        use crate::policy::CatchupPolicy;
        // Hourly cron, down for 7 days. A 2-hour window must fire only the in-
        // window slots; the long pre-window backlog must NOT block the bounded
        // result (issue #484 / Codex #3069 — `window_eligible_slots` jump-starts
        // the cron walk at the window cutoff instead of walking from first_due).
        let schedule = Schedule::Cron("0 * * * *".to_string()); // top of every hour
        let now = parse_utc("2026-04-08T00:00:00Z");
        let first_due = now - chrono::Duration::days(7);
        let window = Duration::from_secs(2 * 3600); // 2 hours

        let plan = catchup_run_plan(
            Some(&schedule),
            first_due,
            now,
            CatchupPolicy::Window(window),
            None,
        );

        // Cutoff = now - 2h = 2026-04-07T22:00:00Z (inclusive): 22:00, 23:00, 00:00.
        assert_eq!(
            plan.run_dates,
            vec![
                parse_utc("2026-04-07T22:00:00Z"),
                parse_utc("2026-04-07T23:00:00Z"),
                parse_utc("2026-04-08T00:00:00Z"),
            ],
            "Window must fire exactly the in-window hourly slots"
        );
        // The whole bounded set fires this tick, so next_run_at advances past now.
        assert_eq!(
            plan.next_run_at,
            Some(parse_utc("2026-04-08T01:00:00Z")),
            "cron Window next_run_at must be the next occurrence strictly after now"
        );
        // 7d hourly + slot at now = 169 eligible; 3 fired ⇒ 166 dropped.
        assert_eq!(plan.dropped, 166, "older out-of-window slots are dropped");
    }

    #[test]
    fn catchup_run_plan_most_recent_high_frequency_is_bounded() {
        use crate::policy::CatchupPolicy;
        // 1-second interval down for 30 days = ~2.6M missed slots. The arithmetic
        // fast-path must return in O(1) without materializing the slot vector.
        let schedule = Schedule::Interval(Duration::from_secs(1));
        let now = parse_utc("2026-04-06T00:00:00Z");
        let first_due = now - chrono::Duration::days(30);

        let plan = catchup_run_plan(
            Some(&schedule),
            first_due,
            now,
            CatchupPolicy::MostRecent,
            None,
        );

        let total = 30 * 24 * 3600_u64; // intervals
        assert_eq!(plan.run_dates.len(), 1, "MostRecent fires exactly one slot");
        assert_eq!(plan.run_dates[0], now, "fires the slot at exactly now");
        // total slots = intervals + 1 (slot at now); dropped = total - 1.
        assert_eq!(plan.dropped, total, "dropped = (intervals + 1) - 1");
        assert_eq!(plan.next_run_at, Some(now + chrono::Duration::seconds(1)));
    }

    #[test]
    fn catchup_run_plan_most_recent_respects_end_at() {
        use crate::policy::CatchupPolicy;
        // 15-min interval; end_at sits between older slots and the newest slot.
        // MostRecent must pick the newest slot STRICTLY BEFORE end_at, not the
        // newest overdue slot (which is at/after end_at).
        let schedule = Schedule::Interval(Duration::from_secs(60 * 15));
        let first_due = parse_utc("2026-04-06T00:00:00Z");
        let now = parse_utc("2026-04-06T02:00:00Z"); // 8 missed slots + slot at now = 9
        // end_at at 01:00: eligible slots are 00:00,00:15,00:30,00:45 (4 slots,
        // since 01:00 itself is == end_at and excluded).
        let end_at = parse_utc("2026-04-06T01:00:00Z");

        let plan = catchup_run_plan(
            Some(&schedule),
            first_due,
            now,
            CatchupPolicy::MostRecent,
            Some(end_at),
        );

        assert_eq!(plan.run_dates.len(), 1, "fires exactly one eligible slot");
        assert_eq!(
            plan.run_dates[0],
            parse_utc("2026-04-06T00:45:00Z"),
            "fires the newest slot strictly before end_at"
        );
        // 4 eligible slots (00:00..00:45), 1 fired, 3 dropped.
        assert_eq!(plan.dropped, 3, "drops the 3 older eligible slots");
        // next_run_at is the natural next slot after now, ignoring end_at.
        assert_eq!(plan.next_run_at, Some(parse_utc("2026-04-06T02:15:00Z")));
    }

    #[test]
    fn catchup_run_plan_most_recent_all_slots_after_end_at_fires_nothing() {
        use crate::policy::CatchupPolicy;
        let schedule = Schedule::Interval(Duration::from_secs(60 * 15));
        let first_due = parse_utc("2026-04-06T05:00:00Z");
        let now = parse_utc("2026-04-06T06:00:00Z");
        // end_at before first_due: no eligible slots at all.
        let end_at = parse_utc("2026-04-06T04:00:00Z");

        let plan = catchup_run_plan(
            Some(&schedule),
            first_due,
            now,
            CatchupPolicy::MostRecent,
            Some(end_at),
        );

        assert!(
            plan.run_dates.is_empty(),
            "no slot before end_at => fire nothing"
        );
        assert_eq!(plan.dropped, 0, "no eligible slots => no catchup drops");
    }

    #[test]
    fn catchup_run_plan_window_respects_end_at() {
        use crate::policy::CatchupPolicy;
        let schedule = Schedule::Interval(Duration::from_secs(60 * 15));
        let first_due = parse_utc("2026-04-06T00:00:00Z");
        let now = parse_utc("2026-04-06T03:00:00Z");
        // 2-hour window keeps slots >= 01:00; end_at at 02:00 caps the top.
        // Eligible (< 02:00): 00:00..01:45 (8 slots). In-window (>=01:00 and
        // <02:00): 01:00,01:15,01:30,01:45 = 4 fired; dropped = 8 - 4 = 4.
        let end_at = parse_utc("2026-04-06T02:00:00Z");

        let plan = catchup_run_plan(
            Some(&schedule),
            first_due,
            now,
            CatchupPolicy::Window(Duration::from_secs(2 * 3600)),
            Some(end_at),
        );

        for slot in &plan.run_dates {
            assert!(*slot < end_at, "no slot at/after end_at may fire");
            assert!(
                *slot >= parse_utc("2026-04-06T01:00:00Z"),
                "no slot before the window may fire"
            );
        }
        assert_eq!(plan.run_dates.len(), 4, "4 in-window slots before end_at");
        assert_eq!(
            plan.dropped, 4,
            "4 eligible-but-out-of-window slots dropped"
        );
    }

    #[test]
    fn catchup_run_plan_most_recent_cron_walk_matches_arithmetic() {
        use crate::policy::CatchupPolicy;
        // Exercise the cron walk fallback (non-interval schedule).
        let schedule = Schedule::Cron("0 * * * *".to_string()); // hourly
        let now = parse_utc("2026-04-06T05:00:00Z");
        let first_due = parse_utc("2026-04-06T00:00:00Z"); // 00:00..05:00 = 6 slots

        let plan = catchup_run_plan(
            Some(&schedule),
            first_due,
            now,
            CatchupPolicy::MostRecent,
            None,
        );

        assert_eq!(plan.run_dates.len(), 1, "cron MostRecent fires one slot");
        assert_eq!(plan.run_dates[0], now, "fires the most recent hourly slot");
        assert_eq!(plan.dropped, 5, "6 slots total, 1 fired, 5 dropped");
    }

    #[test]
    fn scheduled_workflow_starts_use_reject_duplicate_policy() {
        assert_eq!(
            scheduled_workflow_reuse_policy(),
            crate::types::WorkflowIdReusePolicy::RejectDuplicate
        );
    }

    #[test]
    fn scheduled_start_already_exists_counts_as_duplicate_slot() {
        let existing_exec_id = ExecutionId::new();

        let outcome = scheduled_start_outcome(Err(HarvestError::AlreadyExists {
            existing_exec_id,
            existing_state: "RUNNING".to_string(),
        }))
        .expect("scheduled duplicate should be treated as an already dispatched slot");

        assert_eq!(
            outcome,
            ScheduledStartOutcome::Duplicate {
                exec_id: existing_exec_id,
                state: "RUNNING".to_string(),
            }
        );
        assert!(!outcome.created());
    }

    // ── plan_backfill_timestamps ──────────────────────────────────────────────

    #[cfg(all(feature = "db", not(feature = "unified-dag-execution")))]
    #[tokio::test]
    async fn scheduler_rejects_classic_dags_when_unified_execution_is_disabled() {
        let dags = Arc::new(
            compile_dag_catalog(vec![classic_scheduled_dag_info()])
                .expect("classic DAG should compile into the catalog"),
        );
        let registry = Arc::new(HandlerRegistry::new(vec![], vec![]));

        let result = tick_once(
            test_pool("postgres://postgres:postgres@127.0.0.1:1/unreachable"),
            registry,
            dags,
            Arc::new(Vec::new()),
            SchedulerMonitor::offline(),
        )
        .await;

        let err = result.expect_err("classic DAG scheduler startup should be rejected");
        assert!(matches!(err, HarvestError::Config(_)));
        assert!(
            err.to_string().contains("classic DAG execution"),
            "error should identify the unsupported classic DAG configuration: {err}"
        );
    }

    #[test]
    fn plan_backfill_timestamps_hourly_cron_inclusive_bounds() {
        let schedule = Schedule::Cron("0 * * * *".to_string()); // fires at :00 every hour
        let from = parse_utc("2026-04-01T10:00:00Z");
        let to = parse_utc("2026-04-01T13:00:00Z");

        let timestamps = plan_backfill_timestamps(Some(&schedule), from, to, 100)
            .expect("hourly cron backfill over 3-hour window should succeed");

        assert_eq!(
            timestamps,
            vec![
                parse_utc("2026-04-01T10:00:00Z"),
                parse_utc("2026-04-01T11:00:00Z"),
                parse_utc("2026-04-01T12:00:00Z"),
                parse_utc("2026-04-01T13:00:00Z"),
            ]
        );
    }

    #[test]
    fn plan_backfill_timestamps_to_before_from_returns_empty() {
        let schedule = Schedule::Cron("0 * * * *".to_string());
        let from = parse_utc("2026-04-08T00:00:00Z");
        let to = parse_utc("2026-04-01T00:00:00Z");

        let timestamps = plan_backfill_timestamps(Some(&schedule), from, to, 100)
            .expect("inverted window should return empty without error");

        assert!(timestamps.is_empty());
    }

    #[test]
    fn plan_backfill_timestamps_manual_schedule_returns_empty() {
        let from = parse_utc("2026-04-01T00:00:00Z");
        let to = parse_utc("2026-04-08T00:00:00Z");

        let timestamps = plan_backfill_timestamps(None, from, to, 100)
            .expect("unset schedule backfill should succeed with empty plan");

        assert!(timestamps.is_empty());

        let timestamps = plan_backfill_timestamps(Some(&Schedule::Manual), from, to, 100)
            .expect("manual schedule backfill should succeed with empty plan");

        assert!(timestamps.is_empty());
    }

    #[test]
    fn plan_backfill_timestamps_enforces_max_count() {
        // Every-minute interval over a 2-hour window = 120 timestamps > limit of 10
        let schedule = Schedule::Interval(Duration::from_secs(60));
        let from = parse_utc("2026-04-01T00:00:00Z");
        let to = parse_utc("2026-04-01T02:00:00Z");

        let result = plan_backfill_timestamps(Some(&schedule), from, to, 10);

        assert_eq!(result, Err(BackfillPlanError::LimitExceeded { limit: 10 }));
    }

    #[test]
    fn plan_backfill_timestamps_interval_from_is_first_slot() {
        let schedule = Schedule::Interval(Duration::from_secs(3600)); // 1-hour interval
        let from = parse_utc("2026-04-01T10:00:00Z");
        let to = parse_utc("2026-04-01T12:00:00Z");

        let timestamps = plan_backfill_timestamps(Some(&schedule), from, to, 100)
            .expect("interval backfill should succeed");

        assert_eq!(
            timestamps,
            vec![
                parse_utc("2026-04-01T10:00:00Z"),
                parse_utc("2026-04-01T11:00:00Z"),
                parse_utc("2026-04-01T12:00:00Z"),
            ]
        );
    }

    #[test]
    fn plan_backfill_timestamps_equal_from_and_to_returns_single_slot() {
        let schedule = Schedule::Interval(Duration::from_secs(3600));
        let ts = parse_utc("2026-04-01T10:00:00Z");

        let timestamps = plan_backfill_timestamps(Some(&schedule), ts, ts, 100)
            .expect("single-point window should succeed");

        assert_eq!(timestamps, vec![ts]);
    }

    #[test]
    fn plan_backfill_timestamps_7_day_hourly_cron_within_default_limit() {
        let schedule = Schedule::Cron("0 * * * *".to_string());
        let from = parse_utc("2026-04-01T00:00:00Z");
        let to = parse_utc("2026-04-08T00:00:00Z");

        let timestamps =
            plan_backfill_timestamps(Some(&schedule), from, to, DEFAULT_BACKFILL_MAX_COUNT)
                .expect("168-timestamp 7-day backfill should succeed under default limit");

        assert_eq!(timestamps.len(), 169); // 0h..168h inclusive = 169 slots
    }

    // ── apply_overlap_policy ──────────────────────────────────────────────────

    #[test]
    fn overlap_skip_always_returns_drop_with_max_active_runs_reason() {
        let fire = parse_utc("2026-05-01T10:00:00Z");
        let action = apply_overlap_policy(crate::policy::OverlapPolicy::Skip, fire, &[], 100);
        assert_eq!(
            action,
            OverlapAction::Drop {
                reason: "max_active_runs_reached"
            }
        );
        // Even with empty buffer
        let action2 = apply_overlap_policy(
            crate::policy::OverlapPolicy::Skip,
            fire,
            &[parse_utc("2026-05-01T09:00:00Z")],
            100,
        );
        assert_eq!(
            action2,
            OverlapAction::Drop {
                reason: "max_active_runs_reached"
            }
        );
    }

    #[test]
    fn overlap_buffer_one_buffers_when_buffer_is_empty() {
        let fire = parse_utc("2026-05-01T10:00:00Z");
        let action = apply_overlap_policy(crate::policy::OverlapPolicy::BufferOne, fire, &[], 100);
        assert_eq!(action, OverlapAction::Buffer { fire_time: fire });
    }

    #[test]
    fn overlap_buffer_one_drops_when_buffer_has_entry() {
        let fire = parse_utc("2026-05-01T10:00:00Z");
        let existing = [parse_utc("2026-05-01T09:00:00Z")];
        let action = apply_overlap_policy(
            crate::policy::OverlapPolicy::BufferOne,
            fire,
            &existing,
            100,
        );
        assert_eq!(
            action,
            OverlapAction::Drop {
                reason: "buffered_slot_full"
            }
        );
    }

    #[test]
    fn overlap_buffer_all_buffers_within_cap() {
        let fire = parse_utc("2026-05-01T10:00:00Z");
        let existing = [
            parse_utc("2026-05-01T08:00:00Z"),
            parse_utc("2026-05-01T09:00:00Z"),
        ];
        let action =
            apply_overlap_policy(crate::policy::OverlapPolicy::BufferAll, fire, &existing, 5);
        assert_eq!(action, OverlapAction::Buffer { fire_time: fire });
    }

    #[test]
    fn overlap_buffer_all_drops_when_at_cap() {
        let fire = parse_utc("2026-05-01T10:00:00Z");
        let existing = [
            parse_utc("2026-05-01T06:00:00Z"),
            parse_utc("2026-05-01T07:00:00Z"),
            parse_utc("2026-05-01T08:00:00Z"),
        ];
        let action =
            apply_overlap_policy(crate::policy::OverlapPolicy::BufferAll, fire, &existing, 3);
        assert_eq!(
            action,
            OverlapAction::Drop {
                reason: "buffer_full"
            }
        );
    }

    #[test]
    fn overlap_cancel_other_returns_cancel_and_proceed() {
        let fire = parse_utc("2026-05-01T10:00:00Z");
        let action =
            apply_overlap_policy(crate::policy::OverlapPolicy::CancelOther, fire, &[], 100);
        assert_eq!(action, OverlapAction::CancelAndProceed);
    }

    #[test]
    fn overlap_terminate_other_returns_terminate_and_proceed() {
        let fire = parse_utc("2026-05-01T10:00:00Z");
        let action =
            apply_overlap_policy(crate::policy::OverlapPolicy::TerminateOther, fire, &[], 100);
        assert_eq!(action, OverlapAction::TerminateAndProceed);
    }

    #[test]
    fn parse_buffered_runs_parses_json_array_of_timestamps() {
        let json = serde_json::json!(["2026-05-01T08:00:00Z", "2026-05-01T09:00:00Z",]);
        let parsed = parse_buffered_runs(&json);
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0], parse_utc("2026-05-01T08:00:00Z"));
        assert_eq!(parsed[1], parse_utc("2026-05-01T09:00:00Z"));
    }

    #[test]
    fn parse_buffered_runs_returns_empty_for_null_or_invalid() {
        assert!(parse_buffered_runs(&serde_json::Value::Null).is_empty());
        assert!(parse_buffered_runs(&serde_json::json!([])).is_empty());
        assert!(parse_buffered_runs(&serde_json::json!("not-an-array")).is_empty());
    }

    #[test]
    fn buffered_runs_to_json_serializes_as_iso_strings() {
        let runs = vec![
            parse_utc("2026-05-01T08:00:00Z"),
            parse_utc("2026-05-01T09:00:00Z"),
        ];
        let json = buffered_runs_to_json(&runs);
        let parsed = parse_buffered_runs(&json);
        assert_eq!(parsed, runs);
    }

    // ── Timezone-aware schedule: next_run_after ───────────────────────────────

    #[test]
    fn next_run_after_spring_forward_skips_nonexistent_local_time() {
        // America/Los_Angeles spring-forward 2026: 2026-03-08 at 2:00 AM PST → 3:00 AM PDT.
        // "30 2 * * *" has no valid local instant on 2026-03-08; scheduler must
        // skip that day and fire at 2026-03-09 02:30 PDT = 09:30 UTC.
        let schedule = Schedule::CronInTimezone {
            expr: "30 2 * * *".to_string(),
            tz: "America/Los_Angeles".to_string(),
        };
        // Reference: 2026-03-08 01:50 AM PST = 09:50 UTC (just before the gap)
        let reference = parse_utc("2026-03-08T09:50:00Z");
        let next =
            next_run_after(Some(&schedule), reference).expect("should produce a next occurrence");
        // Expected: 2026-03-09 02:30 PDT = 09:30 UTC
        let expected = parse_utc("2026-03-09T09:30:00Z");
        assert_eq!(
            next, expected,
            "spring-forward: 02:30 must not fire on Mar 8 (gap), must fire on Mar 9 instead"
        );
    }

    #[test]
    fn next_run_after_fall_back_fires_first_occurrence_only() {
        // America/Los_Angeles fall-back 2026: 2026-11-01 at 2:00 AM PDT → 1:00 AM PST.
        // 01:30 appears twice; the cron "30 1 * * *" must fire on the FIRST 01:30 (PDT = UTC-7).
        let schedule = Schedule::CronInTimezone {
            expr: "30 1 * * *".to_string(),
            tz: "America/Los_Angeles".to_string(),
        };
        // Reference: 2026-11-01 00:50 AM PDT = 07:50 UTC
        let reference = parse_utc("2026-11-01T07:50:00Z");
        let first_next =
            next_run_after(Some(&schedule), reference).expect("should fire on fall-back day");
        // First 01:30 PDT (UTC-7) = 2026-11-01T08:30:00Z
        let expected_first = parse_utc("2026-11-01T08:30:00Z");
        assert_eq!(
            first_next, expected_first,
            "fall-back: 01:30 must fire at first occurrence (PDT)"
        );
        // Calling next_run_after again from that instant must NOT return the
        // same time (no double-fire on the repeated hour).
        let second_next =
            next_run_after(Some(&schedule), first_next).expect("should advance past fall-back");
        assert_ne!(
            second_next, first_next,
            "must not double-fire the repeated fall-back hour"
        );
        // Next occurrence is 2026-11-02 01:30 PST (UTC-8) = 09:30 UTC
        let expected_next_day = parse_utc("2026-11-02T09:30:00Z");
        assert_eq!(
            second_next, expected_next_day,
            "fall-back: after first fire, next must be the following day in PST"
        );
    }

    #[test]
    fn schedule_expr_and_parse_round_trip_cron_in_timezone() {
        let schedule = Schedule::CronInTimezone {
            expr: "0 9 * * 1-5".to_string(),
            tz: "America/Los_Angeles".to_string(),
        };
        let expr_str = schedule_expr(Some(&schedule)).expect("should produce expr string");
        let parsed = parse_schedule_from_expr(&expr_str).expect("should round-trip parse");
        assert!(
            matches!(&parsed, Schedule::CronInTimezone { tz, .. } if tz == "America/Los_Angeles"),
            "round-trip failed: {parsed:?}"
        );
    }
}
