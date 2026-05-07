//! DAG scheduler and runtime execution.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use croner::Cron;
use diesel::ExpressionMethods;
use diesel::OptionalExtension;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::context::ActivityContext;
use crate::error::{HarvestError, HarvestResult};
use crate::info::DagInfo;
use crate::models::{DagRun, HarvestSchedule, NewDagRun, NewHarvestSchedule};
use crate::policy::{RetryPolicy, Schedule, TaskStatus, WorkflowSchedule};
use crate::schema::{harvest_dag_runs, harvest_schedules, harvest_workflow_executions};
use crate::types::{ActivityExecId, IdempotencyKey};
use crate::worker::{DbPool, HandlerRegistry};

const DEFAULT_SCHEDULER_TICK_INTERVAL: Duration = Duration::from_secs(1);

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
        let shutdown = CancellationToken::new();
        let shutdown_for_task = shutdown.clone();
        let total = dags.len() + workflow_schedules.len();
        let monitor = SchedulerMonitor::new(total);
        let monitor_for_task = monitor.clone();
        let handle = tokio::spawn(async move {
            while !shutdown_for_task.is_cancelled() {
                if let Ok(mut conn) = pool.get().await {
                    if let Err(error) = register_schedules(&mut conn, dags.as_ref()).await {
                        tracing::warn!(error = %error, "failed to register harvest DAG schedules");
                    }
                    if let Err(error) =
                        register_workflow_schedules(&mut conn, workflow_schedules.as_ref()).await
                    {
                        tracing::warn!(error = %error, "failed to register harvest workflow schedules");
                    }
                }

                if let Err(error) = tick_once(
                    pool.clone(),
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
                definition,
            },
        );
    }

    Ok(catalog)
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
    for dag in dags.values() {
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

/// Run one scheduler tick: create due DAG runs, activate queued runs, execute
/// runnable DAG runs, and dispatch due workflow-schedule runs.
///
/// # Errors
///
/// Returns [`HarvestError`] if Postgres cannot be reached or a DAG run cannot
/// be driven to completion.
pub async fn tick_once(
    pool: DbPool,
    registry: Arc<HandlerRegistry>,
    dags: Arc<DagCatalog>,
    workflow_schedules: Arc<Vec<WorkflowSchedule>>,
    monitor: SchedulerMonitor,
) -> HarvestResult<()> {
    let total = dags.len() + workflow_schedules.len();
    monitor.mark_tick(total);

    let mut conn = pool
        .get()
        .await
        .map_err(|error| HarvestError::Database(error.to_string()))?;
    let metrics = Arc::clone(&registry.telemetry().metrics);
    create_due_runs(&mut conn, dags.as_ref()).await?;
    let runnable = activate_queued_runs(&mut conn, dags.as_ref(), &metrics).await?;

    // Dispatch due workflow-schedule runs directly via start_or_load_workflow_execution.
    // Always check the DB — API-created schedules are DB-only and won't appear in the
    // in-memory workflow_schedules list.
    if let Err(error) = tick_workflow_schedules(&mut conn, &metrics).await {
        tracing::warn!(error = %error, "harvest workflow-schedule tick error");
    }
    drop(conn);

    for (run, dag) in runnable {
        execute_dag_run(pool.clone(), Arc::clone(&registry), dag, run).await?;
    }

    Ok(())
}

/// Insert a manual DAG run and kick the scheduler so it can execute promptly.
///
/// # Errors
///
/// Returns [`HarvestError::NotFound`] if the DAG name is unknown, or
/// [`HarvestError::Database`] if the run cannot be recorded.
pub async fn trigger_dag(
    pool: DbPool,
    registry: Arc<HandlerRegistry>,
    dags: Arc<DagCatalog>,
    dag_name: &str,
    run_conf: Option<Value>,
    monitor: SchedulerMonitor,
) -> HarvestResult<DagRun> {
    let dag = dags
        .get(dag_name)
        .ok_or_else(|| HarvestError::NotFound(format!("dag '{dag_name}'")))?;
    let mut db = pool
        .get()
        .await
        .map_err(|error| HarvestError::Database(error.to_string()))?;
    upsert_schedule(&mut db, dag).await?;
    let run = insert_dag_run(&mut db, dag_name, Utc::now(), run_conf).await?;
    drop(db);

    tokio::spawn(async move {
        let _ = tick_once(
            pool,
            registry,
            dags,
            Arc::new(Vec::new()), // no workflow schedules needed for a DAG trigger kick
            monitor,
        )
        .await;
    });

    Ok(run)
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
/// The insert uses `ON CONFLICT (workflow_name) DO NOTHING` so that concurrent
/// scheduler instances or API requests cannot produce duplicate rows even without
/// a serialisable transaction. A subsequent `UPDATE` then refreshes all mutable
/// fields, preserving `is_paused` (managed independently via pause/resume).
async fn upsert_workflow_schedule(
    conn: &mut AsyncPgConnection,
    ws: &WorkflowSchedule,
) -> HarvestResult<HarvestSchedule> {
    use crate::schema::harvest_schedules::dsl;

    let now = Utc::now();
    let expr = schedule_expr(Some(&ws.schedule));

    // Attempt an atomic insert. The UNIQUE constraint on workflow_name means a
    // concurrent writer will hit DO NOTHING rather than inserting a duplicate.
    let row = NewHarvestSchedule {
        id: uuid::Uuid::new_v4(),
        dag_name: None,
        schedule_expr: expr.as_deref(),
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

    // Read back whichever row now exists (just-inserted or pre-existing).
    let existing: HarvestSchedule = dsl::harvest_schedules
        .filter(dsl::workflow_name.eq(&ws.workflow_name))
        .select(HarvestSchedule::as_select())
        .first(conn)
        .await
        .map_err(crate::error::database_error)?;

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

async fn create_due_runs(conn: &mut AsyncPgConnection, dags: &DagCatalog) -> HarvestResult<()> {
    use crate::schema::harvest_schedules::dsl;

    let schedules = dsl::harvest_schedules
        .filter(dsl::dag_name.is_not_null()) // DAG-only rows
        .filter(dsl::is_paused.eq(false))
        .filter(dsl::next_run_at.is_not_null())
        .filter(dsl::next_run_at.le(Utc::now()))
        .order(dsl::next_run_at.asc())
        .select(HarvestSchedule::as_select())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    for schedule in schedules {
        let Some(dag_name) = &schedule.dag_name else {
            continue;
        };
        let Some(dag) = dags.get(dag_name) else {
            continue;
        };
        let Some(logical_date) = schedule.next_run_at else {
            continue;
        };
        let now = Utc::now();
        let (created, next_run_at) =
            due_run_plan(dag.schedule.as_ref(), logical_date, now, dag.catchup);

        if !created.is_empty() {
            let rows = create_new_dag_runs(dag_name, &created);
            diesel::insert_into(harvest_dag_runs::table)
                .values(&rows)
                .on_conflict((harvest_dag_runs::dag_name, harvest_dag_runs::logical_date))
                .do_nothing()
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
        }

        diesel::update(dsl::harvest_schedules.find(schedule.id))
            .set((
                dsl::last_run_at.eq(created.last().copied()),
                dsl::next_run_at.eq(next_run_at),
                dsl::updated_at.eq(Utc::now()),
            ))
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;
    }

    Ok(())
}

async fn activate_queued_runs<'a>(
    conn: &mut AsyncPgConnection,
    dags: &'a DagCatalog,
    metrics: &Arc<dyn crate::telemetry::MetricsRecorder>,
) -> HarvestResult<Vec<(DagRun, &'a RegisteredDag)>> {
    use crate::schema::harvest_dag_runs::dsl as dag_runs_dsl;
    use crate::schema::harvest_schedules::dsl as schedules_dsl;

    let schedules = schedules_dsl::harvest_schedules
        .filter(schedules_dsl::dag_name.is_not_null()) // DAG-only rows
        .filter(schedules_dsl::is_paused.eq(false))
        .select(HarvestSchedule::as_select())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;
    let mut runnable = Vec::with_capacity(schedules.len());

    for schedule in schedules {
        let Some(dag_name) = &schedule.dag_name else {
            continue;
        };
        let Some(dag) = dags.get(dag_name) else {
            continue;
        };
        let running_count = dag_runs_dsl::harvest_dag_runs
            .filter(dag_runs_dsl::dag_name.eq(dag_name))
            .filter(dag_runs_dsl::state.eq("RUNNING"))
            .count()
            .get_result::<i64>(conn)
            .await
            .map_err(crate::error::database_error)?;
        let available = i64::from(schedule.max_active_runs) - running_count;

        let queued = dag_runs_dsl::harvest_dag_runs
            .filter(dag_runs_dsl::dag_name.eq(dag_name))
            .filter(dag_runs_dsl::state.eq("QUEUED"))
            .order(dag_runs_dsl::logical_date.asc())
            .limit(available.max(1)) // load at least 1 to detect skip-worthy backlog
            .select(DagRun::as_select())
            .load(conn)
            .await
            .map_err(crate::error::database_error)?;

        if queued.is_empty() {
            continue;
        }

        if available <= 0 {
            metrics.record_schedule_skipped("dag", dag_name, "max_active_runs_reached");
            continue;
        }
        let queued_ids: Vec<_> = queued.iter().map(|r| r.id).collect();

        let mut updated_runs = diesel::update(
            dag_runs_dsl::harvest_dag_runs.filter(dag_runs_dsl::id.eq_any(queued_ids)),
        )
        .set((
            dag_runs_dsl::state.eq("RUNNING"),
            dag_runs_dsl::started_at.eq(Some(Utc::now())),
        ))
        .returning(DagRun::as_select())
        .get_results::<DagRun>(conn)
        .await
        .map_err(crate::error::database_error)?;

        // Sort by logical_date to preserve the original queue ordering (logical_date ASC).
        updated_runs.sort_by_key(|r| r.logical_date);

        for updated in updated_runs {
            // Emit at activation, not at completion, so interrupted/failed runs
            // are still counted — consistent with workflow schedule semantics.
            metrics.record_schedule_run("dag", dag_name);
            runnable.push((updated, dag));
        }
    }

    Ok(runnable)
}

/// Derive a deterministic, idempotent `workflow_id` for a scheduled run.
///
/// The id is stable across retries: if the scheduler ticks twice before
/// updating `last_run_at`, `start_or_load_workflow_execution` returns the
/// existing execution rather than starting a duplicate.
fn scheduled_workflow_id(workflow_name: &str, scheduled_for: DateTime<Utc>) -> String {
    format!("sched:{}:{}", workflow_name, scheduled_for.timestamp())
}

/// Process due workflow-schedule rows and dispatch workflow starts.
/// Parse a stored `schedule_expr` string back into a [`Schedule`] variant.
///
/// The format written by [`schedule_expr`] is `"cron:<expr>"`, `"interval:<secs>"`,
/// or `"manual"`. Unrecognised strings return `None` and the row is treated as
/// `Schedule::Manual` (no automatic `next_run_at`).
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
    metrics: &Arc<dyn crate::telemetry::MetricsRecorder>,
) -> HarvestResult<()> {
    use crate::execution::StartWorkflowParams;
    use crate::schema::harvest_schedules::dsl;
    use crate::types::{ExecutionId, WorkflowIdReusePolicy};

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
        let exec_id = ExecutionId::new();
        let input = schedule
            .workflow_input
            .clone()
            .unwrap_or(serde_json::Value::Null);
        tracing::info!(
            workflow_name = %wf_name, workflow_id = %workflow_id,
            scheduled_for = %scheduled_for, "harvest: dispatching scheduled workflow run"
        );
        match crate::execution::start_or_load_workflow_execution(
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
                // TODO(#87): switch to RejectDuplicate once #87 lands.
                reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
                trace_context: None,
            },
        )
        .await
        {
            Ok(started) => {
                dispatched += 1;
                last_dispatched_at = Some(*scheduled_for);
                metrics.record_schedule_run("workflow", wf_name);
                tracing::info!(
                    workflow_name = %wf_name, execution_id = %started.exec_id,
                    created = started.created, "harvest: scheduled workflow run dispatched"
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

async fn execute_dag_run(
    pool: DbPool,
    registry: Arc<HandlerRegistry>,
    dag: &RegisteredDag,
    run: DagRun,
) -> HarvestResult<()> {
    // Bolt: Use Arc to avoid deep cloning the JSON Value for every task in the DAG
    let run_input = Arc::new(run.conf.unwrap_or(Value::Null));
    let mut statuses = vec![TaskStatus::Skipped; dag.definition.tasks().len()];

    for level in dag.definition.execution_levels() {
        let tasks = level.iter().map(|task_index| {
            // Avoid an unnecessary heap allocation of `DagTask` per task when `&DagTask` works.
            let task = &dag.definition.tasks()[*task_index];
            let registry = Arc::clone(&registry);
            let task_input = Arc::clone(&run_input);

            // ⚡ Bolt: Remove an intermediate `.collect::<Vec<_>>()` when fetching upstream
            // statuses by passing an Iterator into `execute_dag_task`. This avoids an
            // unnecessary heap allocation per DAG task inside this loop.
            let statuses_ref = &statuses;
            let upstream_statuses = task
                .upstreams
                .iter()
                .map(move |upstream| &statuses_ref[*upstream]);
            let dag_run_id = run.id;
            let node_index = *task_index;
            async move {
                execute_dag_task(
                    &registry,
                    task,
                    upstream_statuses,
                    &task_input,
                    dag_run_id,
                    node_index,
                )
                .await
            }
        });
        let results = futures::future::join_all(tasks).await;
        for (task_index, result) in level.iter().zip(results) {
            statuses[*task_index] = result;
        }
    }

    let final_state = if statuses.contains(&TaskStatus::Failed) {
        "FAILED"
    } else {
        "SUCCESS"
    };
    let mut db = pool
        .get()
        .await
        .map_err(|error| HarvestError::Database(error.to_string()))?;
    diesel::update(harvest_dag_runs::table.find(run.id))
        .set((
            harvest_dag_runs::state.eq(final_state),
            harvest_dag_runs::completed_at.eq(Some(Utc::now())),
        ))
        .execute(&mut db)
        .await
        .map_err(crate::error::database_error)?;

    Ok(())
}

/// Namespace UUID for deriving stable DAG task idempotency keys.
///
/// Keyed on (`dag_run_id`, `task_name`) so each logical DAG task invocation
/// always produces the same [`ActivityExecId`] regardless of retry attempt.
const DAG_TASK_KEY_NAMESPACE: uuid::Uuid = uuid::Uuid::from_bytes([
    0x6b, 0xa7, 0xb8, 0x14, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8,
]);

async fn execute_dag_task<'a>(
    registry: &HandlerRegistry,
    task: &crate::dag::DagTask,
    upstream_statuses: impl IntoIterator<Item = &'a TaskStatus>,
    conf: &Value,
    dag_run_id: uuid::Uuid,
    node_index: usize,
) -> TaskStatus {
    if !task.trigger_rule.should_run(upstream_statuses) {
        return TaskStatus::Skipped;
    }

    let Some(activity) = registry.activities.get(&task.activity_name) else {
        return TaskStatus::Failed;
    };
    let retry_policy = task
        .retry_policy
        .clone()
        .or_else(|| activity.default_retry_policy.clone());
    let timeout = task.start_to_close.or(activity.default_start_to_close);
    let input = task_input(conf, &task.activity_name);
    let mut attempt = 1u32;

    // Derive a stable ActivityExecId from the DAG run ID and task name so
    // the idempotency key is the same across retries for this logical task.
    // node_index is included so two nodes that share the same activity_name
    // within a DAG run receive distinct keys.
    let task_exec_id = ActivityExecId::from_uuid(uuid::Uuid::new_v5(
        &DAG_TASK_KEY_NAMESPACE,
        format!("{dag_run_id}:{node_index}:{}", task.activity_name).as_bytes(),
    ));
    let idempotency_key = IdempotencyKey::from_activity_exec_id(task_exec_id);

    loop {
        let cancel = CancellationToken::new();
        let ctx = ActivityContext::new(registry.shared_state(), None, cancel.clone())
            .with_idempotency_key(idempotency_key.clone())
            .with_attempt(attempt);
        let future = (activity.handler)(&ctx, input.clone());
        let result = match timeout {
            Some(timeout) => tokio::time::timeout(timeout, future)
                .await
                .unwrap_or_else(|_| Err(format!("dag task '{}' timed out", task.activity_name))),
            None => future.await,
        };
        cancel.cancel();

        let Err(error) = result else {
            return TaskStatus::Succeeded;
        };

        let Some(policy) = retry_policy.as_ref() else {
            return TaskStatus::Failed;
        };

        if policy
            .non_retryable_errors
            .iter()
            .any(|non_retryable| non_retryable == &error)
        {
            return TaskStatus::Failed;
        }

        let Some(delay) = next_retry_delay(policy, attempt) else {
            return TaskStatus::Failed;
        };

        attempt = attempt.saturating_add(1);
        tokio::time::sleep(delay).await;
    }
}

fn next_retry_delay(policy: &RetryPolicy, attempt: u32) -> Option<Duration> {
    policy.next_delay(attempt)
}

fn task_input(conf: &Value, activity_name: &str) -> Value {
    match conf {
        Value::Object(map) => {
            let mut payload = map.clone();
            payload.insert(
                "dag_task".to_string(),
                Value::String(activity_name.to_string()),
            );
            Value::Object(payload)
        }
        _ => json!({
            "conf": conf,
            "dag_task": activity_name,
        }),
    }
}

fn create_new_dag_runs<'a>(dag_name: &'a str, run_dates: &[DateTime<Utc>]) -> Vec<NewDagRun<'a>> {
    run_dates
        .iter()
        .map(|&logical_date| NewDagRun {
            id: uuid::Uuid::new_v4(),
            dag_name,
            workflow_exec_id: None,
            logical_date,
            data_interval_start: logical_date,
            data_interval_end: logical_date,
            conf: None,
        })
        .collect()
}

async fn insert_dag_run(
    db: &mut AsyncPgConnection,
    dag_name: &str,
    logical_date: DateTime<Utc>,
    run_conf: Option<Value>,
) -> HarvestResult<DagRun> {
    let row = NewDagRun {
        id: uuid::Uuid::new_v4(),
        dag_name,
        workflow_exec_id: None,
        logical_date,
        data_interval_start: logical_date,
        data_interval_end: logical_date,
        conf: run_conf,
    };

    diesel::insert_into(harvest_dag_runs::table)
        .values(&row)
        .on_conflict((harvest_dag_runs::dag_name, harvest_dag_runs::logical_date))
        .do_nothing()
        .execute(db)
        .await
        .map_err(crate::error::database_error)?;

    harvest_dag_runs::table
        .filter(harvest_dag_runs::dag_name.eq(dag_name))
        .filter(harvest_dag_runs::logical_date.eq(logical_date))
        .select(DagRun::as_select())
        .first(db)
        .await
        .map_err(crate::error::database_error)
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
    fn create_new_dag_runs_generates_correct_rows() {
        let first_due = parse_utc("2026-04-06T12:00:00Z");
        let second_due = parse_utc("2026-04-06T12:01:00Z");
        let dates = vec![first_due, second_due];

        let rows = create_new_dag_runs("test_dag", &dates);

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].dag_name, "test_dag");
        assert_eq!(rows[0].logical_date, first_due);
        assert_eq!(rows[0].data_interval_start, first_due);
        assert_eq!(rows[0].data_interval_end, first_due);
        assert_eq!(rows[0].workflow_exec_id, None);
        assert_eq!(rows[0].conf, None);
        assert_eq!(rows[1].dag_name, "test_dag");
        assert_eq!(rows[1].logical_date, second_due);
        assert_eq!(rows[1].data_interval_start, second_due);
        assert_eq!(rows[1].data_interval_end, second_due);
        assert_eq!(rows[1].workflow_exec_id, None);
        assert_eq!(rows[1].conf, None);
    }

    #[test]
    fn create_new_dag_runs_returns_empty_for_no_dates() {
        let rows = create_new_dag_runs("test_dag", &[]);
        assert!(rows.is_empty());
    }
}
