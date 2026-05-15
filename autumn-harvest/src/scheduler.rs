//! DAG scheduler and runtime execution.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
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
use crate::policy::{Schedule, WorkflowSchedule};
use crate::schema::{harvest_schedules, harvest_workflow_executions};
use crate::shard::{ShardRouter, ShardedDbPool};
use crate::types::{ExecutionId, ShardId, WorkflowIdReusePolicy};
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
        Some(Schedule::Cron(_)) => {
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
        if dag.is_unified {
            continue;
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
        if dag.is_unified || router.pick_for_dag(&dag.name) != shard {
            continue;
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
            .filter(dsl::workflow_name.eq(&ws.workflow_name))
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

        if let Err(error) = tick_workflow_schedules(&mut conn, shard, &metrics).await {
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
pub async fn trigger_unified_dag(
    pool: DbPool,
    dag_name: &str,
    run_conf: Option<Value>,
    shard: crate::types::ShardId,
    default_queue: &str,
) -> HarvestResult<StartedWorkflowExecution> {
    let mut db = pool
        .get()
        .await
        .map_err(|error| HarvestError::Database(error.to_string()))?;

    let exec_id = ExecutionId::new_for_shard(shard);
    // Use the exec_id UUID as the deduplication key so back-to-back manual
    // triggers always produce distinct workflow IDs regardless of clock resolution.
    let workflow_id = format!("{dag_name}-{exec_id}");

    // Resolve the task queue from the schedule row written by HarvestBuilder; fall
    // back to "default" when no row exists (e.g. on the very first trigger before
    // register_workflow_schedules has run).
    let queue_name = {
        use crate::schema::harvest_schedules::dsl;
        dsl::harvest_schedules
            .filter(dsl::workflow_name.eq(dag_name))
            .select(dsl::queue_name)
            .first::<Option<String>>(&mut db)
            .await
            .ok()
            .flatten()
            .unwrap_or_else(|| default_queue.to_string())
    };

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
        },
    )
    .await
}

async fn upsert_schedule(
    conn: &mut AsyncPgConnection,
    dag: &RegisteredDag,
) -> HarvestResult<HarvestSchedule> {
    use crate::schema::harvest_schedules::dsl;

    let existing = dsl::harvest_schedules
        .filter(dsl::dag_name.eq(&dag.name))
        .select(HarvestSchedule::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?;
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
        diesel::update(dsl::harvest_schedules.find(existing.id))
            .set((
                dsl::schedule_expr.eq(expr.clone()),
                dsl::timezone.eq("UTC"),
                dsl::catchup.eq(dag.catchup),
                dsl::max_active_runs.eq(i32::try_from(dag.max_active_runs).unwrap_or(i32::MAX)),
                dsl::updated_at.eq(now),
                dsl::next_run_at.eq(next_run_at),
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
            timezone: "UTC",
            catchup: dag.catchup,
            max_active_runs: i32::try_from(dag.max_active_runs).unwrap_or(i32::MAX),
            is_paused: false,
            workflow_name: None,
            workflow_input: None,
            queue_name: None,
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

    dsl::harvest_schedules
        .filter(dsl::dag_name.eq(dag_name))
        .or_filter(
            dsl::workflow_name
                .eq(workflow_name)
                .and(dsl::dag_name.is_null()),
        )
        .select(HarvestSchedule::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)
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
        timezone: "UTC",
        catchup: ws.catchup,
        max_active_runs: i32::try_from(ws.max_active_runs).unwrap_or(i32::MAX),
        is_paused: ws.paused,
        workflow_name: Some(&ws.workflow_name),
        workflow_input: Some(ws.input.clone()),
        queue_name: Some(ws.queue_name.as_str()),
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
        timezone: "UTC",
        catchup: ws.catchup,
        max_active_runs: i32::try_from(ws.max_active_runs).unwrap_or(i32::MAX),
        // is_paused is set on initial insert only; subsequent upserts preserve the
        // current value so that pause/resume state is not accidentally overwritten.
        is_paused: ws.paused,
        workflow_name: Some(&ws.workflow_name),
        workflow_input: Some(ws.input.clone()),
        queue_name: Some(ws.queue_name.as_str()),
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
    diesel::update(dsl::harvest_schedules.find(existing.id))
        .set((
            dsl::schedule_expr.eq(expr),
            dsl::timezone.eq("UTC"),
            dsl::catchup.eq(ws.catchup),
            dsl::max_active_runs.eq(i32::try_from(ws.max_active_runs).unwrap_or(i32::MAX)),
            dsl::dag_name.eq(dag_name),
            dsl::workflow_name.eq(Some(ws.workflow_name.as_str())),
            dsl::workflow_input.eq(Some(ws.input.clone())),
            dsl::queue_name.eq(Some(ws.queue_name.as_str())),
            dsl::updated_at.eq(now),
            dsl::next_run_at.eq(next_run_at),
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
}

/// Derive a deterministic, idempotent `workflow_id` for a scheduled run.
///
/// The id is stable across retries: if the scheduler ticks twice before
/// updating `last_run_at`, `RejectDuplicate` reports the already-created
/// execution and the scheduler treats that slot as dispatched.
fn scheduled_workflow_id(workflow_name: &str, scheduled_for: DateTime<Utc>) -> String {
    format!("sched:{}:{}", workflow_name, scheduled_for.timestamp())
}

/// Public re-export of `scheduled_workflow_id` for use in the backfill handler.
#[must_use]
pub fn scheduled_workflow_id_pub(workflow_name: &str, scheduled_for: DateTime<Utc>) -> String {
    scheduled_workflow_id(workflow_name, scheduled_for)
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
    expr.strip_prefix("cron:").map_or_else(
        || {
            expr.strip_prefix("interval:")
                .and_then(|s| s.parse::<u64>().ok())
                .map(|secs| Schedule::Interval(Duration::from_secs(secs)))
        },
        |cron| Some(Schedule::Cron(cron.to_string())),
    )
}

async fn tick_workflow_schedules(
    conn: &mut AsyncPgConnection,
    current_shard: ShardId,
    metrics: &Arc<dyn crate::telemetry::MetricsRecorder>,
) -> HarvestResult<()> {
    use crate::schema::harvest_schedules::dsl;

    let now = Utc::now();

    let due: Vec<HarvestSchedule> = dsl::harvest_schedules
        .filter(dsl::workflow_name.is_not_null())
        .filter(dsl::is_paused.eq(false))
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
        // Parse the schedule expression stored in the DB row. This covers both
        // in-process registered schedules and schedules created via the API
        // (which are DB-only and do not appear in the in-memory list).
        let parsed_schedule = schedule
            .schedule_expr
            .as_deref()
            .and_then(parse_schedule_from_expr);
        let catchup = schedule.catchup;
        if let Err(error) = tick_one_workflow_schedule(
            conn,
            wf_name,
            catchup,
            parsed_schedule.as_ref(),
            &schedule,
            logical_date,
            now,
            current_shard,
            metrics,
        )
        .await
        {
            tracing::warn!(
                error = %error, workflow_name = %wf_name,
                "harvest: workflow schedule tick failed; continuing to next schedule"
            );
        }
    }

    Ok(())
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
    metrics: &Arc<dyn crate::telemetry::MetricsRecorder>,
) -> HarvestResult<()> {
    use crate::execution::StartWorkflowParams;
    use crate::schema::harvest_schedules::dsl;

    let running: i64 = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(wf_name))
        .filter(harvest_workflow_executions::state.eq("RUNNING"))
        .count()
        .get_result(conn)
        .await
        .map_err(crate::error::database_error)?;

    if running >= i64::from(schedule.max_active_runs) {
        tracing::info!(
            workflow_name = %wf_name,
            running,
            max_active_runs = schedule.max_active_runs,
            "harvest workflow schedule skipped: max_active_runs reached"
        );
        metrics.record_schedule_skipped("workflow", wf_name, "max_active_runs_reached");
        // For catchup schedules keep next_run_at at logical_date so the
        // overdue slot is retried on the next tick once a run slot opens.
        // For non-catchup schedules advance past overdue slots to the next
        // future firing so the scheduler doesn't spin on an old timestamp.
        let next = if catchup {
            Some(logical_date)
        } else {
            next_run_after(parsed_schedule, now)
        };
        diesel::update(dsl::harvest_schedules.find(schedule.id))
            .set((dsl::next_run_at.eq(next), dsl::updated_at.eq(now)))
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;
        return Ok(());
    }

    let (run_dates, next_run_after_plan) =
        due_run_plan(parsed_schedule, logical_date, now, catchup);
    let dispatch_queue = schedule.queue_name.as_deref().unwrap_or("default");

    let mut dispatched: u32 = 0;
    let mut last_dispatched_at: Option<DateTime<Utc>> = None;
    // Set to the first slot we could not dispatch due to max_active_runs; if Some,
    // it becomes next_run_at so catchup slots are not silently dropped.
    let mut deferred_next_run_at: Option<DateTime<Utc>> = None;
    for scheduled_for in &run_dates {
        if running + i64::from(dispatched) >= i64::from(schedule.max_active_runs) {
            deferred_next_run_at = Some(*scheduled_for);
            tracing::info!(
                workflow_name = %wf_name,
                max_active_runs = schedule.max_active_runs,
                "harvest workflow schedule: max_active_runs reached during catchup; deferring remaining"
            );
            break;
        }
        let workflow_id = scheduled_workflow_id(wf_name, *scheduled_for);
        let exec_id = if schedule.dag_name.is_some() {
            ExecutionId::new_for_shard(current_shard)
        } else {
            ExecutionId::new()
        };
        let input = schedule
            .workflow_input
            .clone()
            .unwrap_or(serde_json::Value::Null);
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
            },
        )
        .await;
        match scheduled_start_outcome(start_result) {
            Ok(outcome) => {
                dispatched += 1;
                last_dispatched_at = Some(*scheduled_for);
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
    let effective_next_run_at = deferred_next_run_at.or(next_run_after_plan);
    diesel::update(dsl::harvest_schedules.find(schedule.id))
        .set((
            dsl::last_run_at.eq(effective_last_run_at),
            dsl::next_run_at.eq(effective_next_run_at),
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
        Some(Schedule::Interval(interval)) => Some(format!("interval:{}", interval.as_secs())),
        Some(Schedule::Manual) => Some("manual".to_string()),
        None => None,
    }
}

fn next_run_after(schedule: Option<&Schedule>, reference: DateTime<Utc>) -> Option<DateTime<Utc>> {
    match schedule {
        Some(Schedule::Cron(expr)) => Cron::new(expr)
            .with_seconds_optional()
            .parse()
            .ok()
            .and_then(|cron| cron.find_next_occurrence(&reference, false).ok()),
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
        return (vec![first_due], next_run_after(schedule, now));
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
}
