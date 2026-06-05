//! DAG scheduler and runtime execution.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use croner::Cron;
use diesel::OptionalExtension;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel::{BoolExpressionMethods, ExpressionMethods, TextExpressionMethods};
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
        )
        .await
        {
            tracing::warn!(
                error = %error,
                shard_id = shard.as_i32(),
                "harvest: buffered schedule drain error"
            );
        }

        if let Err(error) =
            tick_workflow_schedules(&mut conn, shard, dags.as_ref(), registry.as_ref(), &metrics)
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
            .filter(harvest_workflow_executions::state.eq("RUNNING"))
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

async fn upsert_workflow_schedule(
    conn: &mut AsyncPgConnection,
    ws: &WorkflowSchedule,
) -> HarvestResult<HarvestSchedule> {
    use crate::schema::harvest_schedules::dsl;

    let now = Utc::now();
    let expr = schedule_expr(Some(&ws.schedule));
    let existing = find_or_insert_workflow_schedule(conn, ws, expr.as_deref()).await?;
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
    diesel::update(dsl::harvest_schedules.find(existing.id))
        .set((
            dsl::schedule_expr.eq(expr),
            dsl::timezone.eq(ws.schedule.timezone_str()),
            dsl::catchup.eq(ws.catchup),
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
        ))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;

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

    dsl::harvest_schedules
        .find(existing.id)
        .select(HarvestSchedule::as_select())
        .first(conn)
        .await
        .map_err(crate::error::database_error)
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
) -> HarvestResult<()> {
    use crate::schema::harvest_schedules::dsl;

    let now = Utc::now();

    // issue #377: load active admission gates once per tick for the gate-check below.
    let active_gates = crate::admission_gate::db::load_active_gates(conn)
        .await
        .unwrap_or_default();

    let due: Vec<HarvestSchedule> = dsl::harvest_schedules
        .filter(dsl::workflow_name.is_not_null())
        .filter(dsl::is_paused.eq(false))
        // Auto-paused schedules (issue #360) are excluded from the due list.
        .filter(dsl::auto_paused_at.is_null())
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
        let Some(logical_date) = schedule.next_run_at else {
            continue;
        };

        // issue #377: skip this schedule if an active admission gate applies.
        {
            let queue_name = schedule.queue_name.as_deref().unwrap_or("default");
            let owner = registry
                .workflows
                .get(wf_name.as_str())
                .and_then(|i| i.owner)
                .or_else(|| {
                    registered_dags
                        .get(wf_name.as_str())
                        .and_then(|d| d.owner.as_deref())
                });
            if let Some(gate) = crate::admission_gate::check_admission(
                &active_gates,
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
                // Advance next_run_at so this slot doesn't get re-attempted on the
                // next tick; let the schedule continue ticking normally once the gate
                // is lifted.
                let parsed_schedule = schedule
                    .schedule_expr
                    .as_deref()
                    .and_then(parse_schedule_from_expr);
                let next_run = next_run_after(parsed_schedule.as_ref(), now);
                let _ = diesel::update(dsl::harvest_schedules.find(schedule.id))
                    .set((dsl::next_run_at.eq(next_run), dsl::updated_at.eq(now)))
                    .execute(conn)
                    .await;
                continue;
            }
        }

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
        // Parse the schedule expression stored in the DB row. This covers both
        // in-process registered schedules and schedules created via the API
        // (which are DB-only and do not appear in the in-memory list).
        let parsed_schedule = schedule
            .schedule_expr
            .as_deref()
            .and_then(parse_schedule_from_expr);
        let catchup = schedule.catchup;

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
        .bind::<diesel::sql_types::Uuid, _>(schedule.id)
        .bind::<diesel::sql_types::Timestamptz, _>(logical_date)
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;

        if claim_rows_affected == 0 {
            metrics.record_schedule_fire_attempt(wf_name, "lost_race");
            tracing::debug!(
                schedule_id = %schedule.id,
                workflow_name = %wf_name,
                "harvest: schedule slot claim lost to peer replica; skipping this tick"
            );
            continue;
        }
        metrics.record_schedule_fire_attempt(wf_name, "claimed");

        if let Err(error) = tick_one_workflow_schedule(
            conn,
            wf_name,
            catchup,
            parsed_schedule.as_ref(),
            &schedule,
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
    }

    Ok(())
}

/// Cancel the oldest scheduled RUNNING executions for `workflow_name`, up to `max_to_cancel`.
///
/// Only cancels executions with a `sched:` workflow ID so operator-triggered manual runs are
/// not inadvertently cancelled. Orders by `started_at ASC` so the oldest executions are
/// cancelled first, preserving the most recent progress.
#[cfg(feature = "db")]
async fn cancel_in_flight_runs(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    reason: &str,
    max_to_cancel: u32,
) -> HarvestResult<u32> {
    use crate::execution::cancel_workflow_execution;

    let running_ids: Vec<uuid::Uuid> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(workflow_name))
        .filter(harvest_workflow_executions::workflow_id.like("sched:%"))
        .filter(harvest_workflow_executions::state.eq("RUNNING"))
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
        match cancel_workflow_execution(conn, exec_id, reason).await {
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

/// Terminate the oldest scheduled RUNNING executions for `workflow_name`, up to `max_to_terminate`.
///
/// Only terminates executions with a `sched:` workflow ID. Orders by `started_at ASC` so the
/// oldest executions are terminated first.
#[cfg(feature = "db")]
async fn terminate_in_flight_runs(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    reason: &str,
    max_to_terminate: u32,
) -> HarvestResult<u32> {
    use crate::execution::terminate_workflow_execution;

    let active_ids: Vec<uuid::Uuid> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(workflow_name))
        .filter(harvest_workflow_executions::workflow_id.like("sched:%"))
        .filter(harvest_workflow_executions::state.eq("RUNNING"))
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
        match terminate_workflow_execution(conn, exec_id, reason).await {
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

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn tick_one_workflow_schedule(
    conn: &mut AsyncPgConnection,
    wf_name: &str,
    catchup: bool,
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
            None => {
                // Firing is suppressed. For catchup schedules advance to the next
                // slot after the excluded date so overdue non-excluded slots are
                // not dropped; for non-catchup schedules advance from now.
                tracing::info!(
                    workflow_name = %wf_name,
                    calendar = %cal_name,
                    fire_date = %fire_date,
                    "harvest: workflow schedule firing suppressed by calendar"
                );
                metrics.record_schedule_skipped("workflow", wf_name, "calendar");
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
    // Re-read auto_paused_at and consecutive_failure_count from DB to guard
    // against a concurrent worker completion that set auto_paused_at after this
    // tick loaded the schedule row.  This is the only field that can be written
    // by a racing path while the HA claim is held; the single SELECT is cheaper
    // than reloading the full row.
    let (db_failure_count, db_auto_paused_at): (i32, Option<DateTime<Utc>>) =
        dsl::harvest_schedules
            .find(schedule.id)
            .select((dsl::consecutive_failure_count, dsl::auto_paused_at))
            .first(conn)
            .await
            .map_err(crate::error::database_error)?;

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

    let mut running: i64 = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(wf_name))
        .filter(harvest_workflow_executions::state.eq("RUNNING"))
        .count()
        .get_result(conn)
        .await
        .map_err(crate::error::database_error)?;

    if running >= i64::from(schedule.max_active_runs) {
        let overlap_policy = OverlapPolicy::from_db(&schedule.overlap_policy);
        let mut buffered = parse_buffered_runs(&schedule.buffered_runs);
        let buffer_all_max = usize::try_from(schedule.buffer_all_max.max(1)).unwrap_or(usize::MAX);

        let action = apply_overlap_policy(overlap_policy, logical_date, &buffered, buffer_all_max);

        match action {
            OverlapAction::Drop { reason } => {
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
                    "overlap policy CancelOther: new firing",
                    needed,
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
                    "overlap policy TerminateOther: new firing",
                    needed,
                )
                .await?;
                running -= i64::from(terminated);
            }
        }
    }

    let (run_dates, next_run_after_plan) =
        due_run_plan(parsed_schedule, logical_date, now, catchup);
    let dispatch_queue = schedule.queue_name.as_deref().unwrap_or("default");
    // jitter_window already computed at function entry; reused here.

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
        tracing::info!(
            workflow_name = %wf_name, workflow_id = %workflow_id,
            scheduled_for = %scheduled_for, "harvest: dispatching scheduled workflow run"
        );
        let start_result = crate::execution::start_or_load_workflow_execution(
            conn,
            StartWorkflowParams {
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
                max_workflow_input_bytes: 0,
                start_at: None,
                delay: None,
                max_workflow_start_delay: None,
                owner,
                runbook_url,
                severity,
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
    diesel::update(
        dsl::harvest_schedules
            .find(schedule.id)
            .filter(dsl::fire_claim_token.eq(Some(claim_token))),
    )
    .set((
        dsl::last_run_at.eq(effective_last_run_at),
        dsl::next_run_at.eq(effective_next_run_at),
        // Clear the HA claim so the column stays clean after a successful
        // fire. Guarded by token so a slow late tick cannot overwrite a
        // successor replica's live claim if the 30 s TTL expired.
        dsl::fire_claim_token.eq(Option::<uuid::Uuid>::None),
        dsl::fire_claimed_until.eq(Option::<DateTime<Utc>>::None),
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
) -> HarvestResult<()> {
    use crate::schema::harvest_schedules::dsl;
    use diesel_async::RunQueryDsl;

    let now = Utc::now();

    // Query schedules that have buffered runs and are not paused (manually or auto-paused).
    let pending: Vec<HarvestSchedule> = dsl::harvest_schedules
        .filter(dsl::workflow_name.is_not_null())
        .filter(dsl::is_paused.eq(false))
        .filter(dsl::auto_paused_at.is_null())
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

        let running: i64 = harvest_workflow_executions::table
            .filter(harvest_workflow_executions::workflow_name.eq(wf_name))
            .filter(harvest_workflow_executions::state.eq("RUNNING"))
            .count()
            .get_result(conn)
            .await
            .map_err(crate::error::database_error)?;

        let available = i64::from(schedule.max_active_runs).saturating_sub(running);
        if available <= 0 {
            continue;
        }

        let dispatch_queue = schedule.queue_name.as_deref().unwrap_or("default");
        let mut dispatched: u32 = 0;

        while dispatched < u32::try_from(available).unwrap_or(u32::MAX) && !buffered.is_empty() {
            let scheduled_for = buffered.remove(0);
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

            tracing::info!(
                workflow_name = %wf_name,
                workflow_id = %workflow_id,
                buffered_for = %scheduled_for,
                "harvest: dispatching buffered scheduled workflow run"
            );

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
                    max_workflow_input_bytes: 0,
                    start_at: None,
                    delay: None,
                    max_workflow_start_delay: None,
                    owner,
                    runbook_url,
                    severity,
                },
            )
            .await;

            match scheduled_start_outcome(start_result) {
                Ok(outcome) => {
                    dispatched += 1;
                    if outcome.created() {
                        metrics.record_schedule_run("workflow", wf_name);
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

        // Persist the updated buffer.
        diesel::update(dsl::harvest_schedules.find(schedule.id))
            .set((
                dsl::buffered_runs.eq(buffered_runs_to_json(&buffered)),
                dsl::updated_at.eq(now),
            ))
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;
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
#[cfg(feature = "db")]
pub(crate) async fn maybe_increment_schedule_failure_counter(
    conn: &mut diesel_async::AsyncPgConnection,
    workflow_id: &str,
    workflow_name: &str,
    metrics: &dyn crate::telemetry::MetricsRecorder,
) {
    use crate::schema::harvest_schedules::dsl;

    if !workflow_id.starts_with("sched:") {
        return;
    }

    // Extract the schedule UUID embedded in the workflow_id by `scheduled_workflow_id`.
    // Format: "sched:{schedule_uuid}:{workflow_name}:{timestamp}[.{micros}]"
    // If the UUID cannot be parsed (e.g. executions created before this format was
    // introduced) we fall back to a workflow_name-scoped update.
    let schedule_uuid: Option<uuid::Uuid> = workflow_id
        .strip_prefix("sched:")
        .and_then(|s| s.split(':').next())
        .and_then(|s| uuid::Uuid::parse_str(s).ok());

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
#[cfg(feature = "db")]
pub(crate) async fn maybe_reset_schedule_failure_counter(
    conn: &mut diesel_async::AsyncPgConnection,
    workflow_id: &str,
    workflow_name: &str,
) {
    use crate::schema::harvest_schedules::dsl;

    if !workflow_id.starts_with("sched:") {
        return;
    }

    let schedule_uuid: Option<uuid::Uuid> = workflow_id
        .strip_prefix("sched:")
        .and_then(|s| s.split(':').next())
        .and_then(|s| uuid::Uuid::parse_str(s).ok());

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
