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
use crate::policy::{RetryPolicy, Schedule, TaskStatus};
use crate::schema::{harvest_dag_runs, harvest_schedules};
use crate::worker::{DbPool, HandlerRegistry};

const DEFAULT_SCHEDULER_TICK_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub struct RegisteredDag {
    pub name: String,
    pub module: String,
    pub schedule: Option<Schedule>,
    pub catchup: bool,
    pub max_active_runs: u32,
    pub definition: crate::dag::DagDefinition,
}

impl RegisteredDag {
    #[must_use]
    pub fn task_count(&self) -> usize {
        self.definition.tasks().len()
    }
}

pub type DagCatalog = HashMap<String, RegisteredDag>;

#[derive(Debug, Clone, Serialize)]
pub struct SchedulerSnapshot {
    pub running: bool,
    pub dag_count: usize,
    pub tick_interval_ms: u64,
    pub last_tick_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct SchedulerMonitor {
    inner: Arc<Mutex<SchedulerSnapshot>>,
}

impl SchedulerMonitor {
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

pub struct SchedulerRuntime {
    shutdown: CancellationToken,
    handle: JoinHandle<()>,
    monitor: SchedulerMonitor,
}

impl SchedulerRuntime {
    #[must_use]
    pub fn spawn(pool: DbPool, registry: Arc<HandlerRegistry>, dags: Arc<DagCatalog>) -> Self {
        let shutdown = CancellationToken::new();
        let shutdown_for_task = shutdown.clone();
        let monitor = SchedulerMonitor::new(dags.len());
        let monitor_for_task = monitor.clone();
        let handle = tokio::spawn(async move {
            while !shutdown_for_task.is_cancelled() {
                if let Ok(mut conn) = pool.get().await {
                    if let Err(error) = register_schedules(&mut conn, dags.as_ref()).await {
                        tracing::warn!(error = %error, "failed to register harvest schedules");
                    }
                }

                if let Err(error) = tick_once(
                    pool.clone(),
                    Arc::clone(&registry),
                    Arc::clone(&dags),
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

            monitor_for_task.mark_stopped(dags.len());
        });

        Self {
            shutdown,
            handle,
            monitor,
        }
    }

    #[must_use]
    pub fn monitor(&self) -> SchedulerMonitor {
        self.monitor.clone()
    }

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

/// Run one scheduler tick: create due runs, activate queued runs, and execute
/// any runs that became runnable.
///
/// # Errors
///
/// Returns [`HarvestError`] if Postgres cannot be reached or a DAG run cannot
/// be driven to completion.
pub async fn tick_once(
    pool: DbPool,
    registry: Arc<HandlerRegistry>,
    dags: Arc<DagCatalog>,
    monitor: SchedulerMonitor,
) -> HarvestResult<()> {
    monitor.mark_tick(dags.len());

    let mut conn = pool
        .get()
        .await
        .map_err(|error| HarvestError::Database(error.to_string()))?;
    create_due_runs(&mut conn, dags.as_ref()).await?;
    let runnable = activate_queued_runs(&mut conn, dags.as_ref()).await?;
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
        let _ = tick_once(pool, registry, dags, monitor).await;
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
            dag_name: &dag.name,
            schedule_expr: expr.as_deref(),
            timezone: "UTC",
            catchup: dag.catchup,
            max_active_runs: i32::try_from(dag.max_active_runs).unwrap_or(i32::MAX),
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

async fn create_due_runs(conn: &mut AsyncPgConnection, dags: &DagCatalog) -> HarvestResult<()> {
    use crate::schema::harvest_schedules::dsl;

    let schedules = dsl::harvest_schedules
        .filter(dsl::is_paused.eq(false))
        .filter(dsl::next_run_at.is_not_null())
        .filter(dsl::next_run_at.le(Utc::now()))
        .order(dsl::next_run_at.asc())
        .select(HarvestSchedule::as_select())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    for schedule in schedules {
        let Some(dag) = dags.get(&schedule.dag_name) else {
            continue;
        };
        let Some(logical_date) = schedule.next_run_at else {
            continue;
        };
        let now = Utc::now();
        let (created, next_run_at) =
            due_run_plan(dag.schedule.as_ref(), logical_date, now, dag.catchup);

        if !created.is_empty() {
            let rows = create_new_dag_runs(&schedule.dag_name, &created);
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

async fn activate_queued_runs(
    conn: &mut AsyncPgConnection,
    dags: &DagCatalog,
) -> HarvestResult<Vec<(DagRun, RegisteredDag)>> {
    use crate::schema::harvest_dag_runs::dsl as dag_runs_dsl;
    use crate::schema::harvest_schedules::dsl as schedules_dsl;

    let schedules = schedules_dsl::harvest_schedules
        .filter(schedules_dsl::is_paused.eq(false))
        .select(HarvestSchedule::as_select())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;
    let mut runnable = Vec::new();

    for schedule in schedules {
        let Some(dag) = dags.get(&schedule.dag_name) else {
            continue;
        };
        let running_count = dag_runs_dsl::harvest_dag_runs
            .filter(dag_runs_dsl::dag_name.eq(&schedule.dag_name))
            .filter(dag_runs_dsl::state.eq("RUNNING"))
            .count()
            .get_result::<i64>(conn)
            .await
            .map_err(crate::error::database_error)?;
        let available = i64::from(schedule.max_active_runs) - running_count;
        if available <= 0 {
            continue;
        }

        let queued = dag_runs_dsl::harvest_dag_runs
            .filter(dag_runs_dsl::dag_name.eq(&schedule.dag_name))
            .filter(dag_runs_dsl::state.eq("QUEUED"))
            .order(dag_runs_dsl::logical_date.asc())
            .limit(available)
            .select(DagRun::as_select())
            .load(conn)
            .await
            .map_err(crate::error::database_error)?;

        if queued.is_empty() {
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
            runnable.push((updated, dag.clone()));
        }
    }

    Ok(runnable)
}

async fn execute_dag_run(
    pool: DbPool,
    registry: Arc<HandlerRegistry>,
    dag: RegisteredDag,
    run: DagRun,
) -> HarvestResult<()> {
    let run_input = run.conf.clone().unwrap_or(Value::Null);
    let mut statuses = vec![TaskStatus::Skipped; dag.definition.tasks().len()];

    for level in dag.definition.execution_levels() {
        let tasks = level.iter().map(|task_index| {
            let task = dag.definition.tasks()[*task_index].clone();
            let upstream_statuses = task
                .upstreams
                .iter()
                .map(|upstream| statuses[*upstream].clone())
                .collect::<Vec<_>>();
            let registry = Arc::clone(&registry);
            let task_input = run_input.clone();
            async move { execute_dag_task(&registry, &task, &upstream_statuses, &task_input).await }
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

async fn execute_dag_task(
    registry: &HandlerRegistry,
    task: &crate::dag::DagTask,
    upstream_statuses: &[TaskStatus],
    conf: &Value,
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
    let mut attempt = 1;

    loop {
        let cancel = CancellationToken::new();
        let ctx = ActivityContext::new(registry.shared_state(), None, cancel.clone());
        let future = (activity.handler)(&ctx, input.clone());
        let result = match timeout {
            Some(timeout) => tokio::time::timeout(timeout, future)
                .await
                .unwrap_or_else(|_| Err(format!("dag task '{}' timed out", task.activity_name))),
            None => future.await,
        };
        cancel.cancel();

        match result {
            Ok(_) => return TaskStatus::Succeeded,
            Err(error) => {
                if let Some(policy) = retry_policy.as_ref() {
                    if policy
                        .non_retryable_errors
                        .iter()
                        .any(|non_retryable| non_retryable == &error)
                    {
                        return TaskStatus::Failed;
                    }
                    if let Some(delay) = next_retry_delay(policy, attempt) {
                        attempt = attempt.saturating_add(1);
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                }
                return TaskStatus::Failed;
            }
        }
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

    let mut created = Vec::new();
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
