#![cfg(feature = "db")]
//! Auto-pause schedule tests — issue #360.
//!
//! Verifies that a schedule with `consecutive_failure_limit` set is
//! automatically paused after the configured number of consecutive
//! execution failures, and can be resumed by clearing the auto-pause state.
//!
//! The tests use a real Postgres container (testcontainers) to exercise the
//! scheduler tick path end-to-end.

use std::sync::{Arc, Mutex};

use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::schema::harvest_schedules;
use autumn_harvest::telemetry::{METRIC_SCHEDULE_AUTO_PAUSED, MetricsRecorder};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::{DagCatalog, SchedulerMonitor, WorkflowContext, tick_once};
use chrono::Utc;
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// All migrations up to and including the auto-pause migration.
const INIT_SQL: &str = concat!(
    include_str!("../migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../migrations/20260427000000_harvest_continue_as_new/up.sql"),
    "\n",
    include_str!("../migrations/20260429000000_harvest_concurrency_key/up.sql"),
    "\n",
    include_str!("../migrations/20260430000000_harvest_workflow_schedules/up.sql"),
    "\n",
    include_str!("../migrations/20260430000001_harvest_external_tasks/up.sql"),
    "\n",
    include_str!("../migrations/20260508000000_harvest_external_task_updated_at/up.sql"),
    "\n",
    include_str!("../migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../migrations/20260508010000_harvest_workers_drain_deadline/up.sql"),
    "\n",
    include_str!("../migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!("../migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"),
    "\n",
    include_str!("../migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    include_str!("../migrations/20260518000000_harvest_signal_idempotency/up.sql"),
    "\n",
    include_str!("../migrations/20260517000000_harvest_schedule_jitter/up.sql"),
    "\n",
    include_str!("../migrations/20260517000001_harvest_schedule_overlap_policy/up.sql"),
    "\n",
    include_str!("../migrations/20260518000001_harvest_workflow_execution_timeout/up.sql"),
    "\n",
    include_str!("../migrations/20260519000000_harvest_calendar_awareness/up.sql"),
    "\n",
    include_str!("../migrations/20260522000000_harvest_schedule_decisions/up.sql"),
    "\n",
    include_str!("../migrations/20260522000001_harvest_rate_limiting/up.sql"),
    "\n",
    include_str!("../migrations/20260526000001_harvest_parent_close_policy/up.sql"),
    "\n",
    include_str!("../migrations/20260530000000_harvest_schedule_ha_claim/up.sql"),
    "\n",
    // Auto-pause columns (issue #360)
    include_str!("../migrations/20260601000000_harvest_schedule_auto_pause/up.sql"),
    "\n",
    include_str!("../migrations/20260601000001_harvest_poison_pill_strikes/up.sql"),
    "\n",
    include_str!("../migrations/20260601000002_harvest_ownership_metadata/up.sql"),
    "\n",
    include_str!("../migrations/20260603000000_harvest_completion_triggers/up.sql"),
    include_str!("../migrations/20260605000000_harvest_admission_gates/up.sql"),
    include_str!("../migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"),
    include_str!("../migrations/20260607000000_harvest_worker_capability_labels/up.sql"),
    include_str!("../migrations/20260607000001_harvest_task_required_capabilities/up.sql"),
    "\n",
    include_str!("../migrations/20260607000002_harvest_workflow_pause/up.sql"),
    "\n",
    include_str!("../migrations/20260609000001_harvest_workflow_current_details/up.sql")
);

// ── Recording metrics ──────────────────────────────────────────────────────

#[derive(Debug, Default)]
struct RecordingMetrics {
    auto_paused: Mutex<Vec<String>>,
}

impl RecordingMetrics {
    fn auto_paused_names(&self) -> Vec<String> {
        self.auto_paused.lock().unwrap().clone()
    }
}

impl MetricsRecorder for RecordingMetrics {
    fn record_schedule_auto_paused(&self, schedule_name: &str) {
        self.auto_paused
            .lock()
            .unwrap()
            .push(schedule_name.to_owned());
    }
}

// ── Test helpers ───────────────────────────────────────────────────────────

async fn setup_db() -> (AsyncPgConnection, String, ContainerAsync<Postgres>) {
    let container = Postgres::default().start().await.expect("postgres start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    conn.batch_execute(INIT_SQL).await.expect("migration");
    (conn, url, container)
}

fn noop_handler<'a>(
    _ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>,
> {
    Box::pin(async { Ok(serde_json::Value::Null) })
}

fn make_registry(workflow_name: &'static str) -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            name: workflow_name,
            module: "scheduler_auto_pause_tests",
            handler: noop_handler,
            execution_timeout: None,
            concurrency: None,
            max_input_bytes: None,

            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
        }],
        vec![],
    ))
}

fn make_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool")
}

/// Insert a due schedule with a `consecutive_failure_limit` and return its id.
async fn insert_schedule_with_failure_limit(
    conn: &mut AsyncPgConnection,
    wf_name: &str,
    failure_limit: i32,
) -> Uuid {
    use autumn_harvest::schema::harvest_schedules::dsl;
    let now = Utc::now();
    let id = Uuid::new_v4();
    diesel::insert_into(harvest_schedules::table)
        .values((
            dsl::id.eq(id),
            dsl::workflow_name.eq(wf_name),
            dsl::schedule_expr.eq("interval:60"),
            dsl::timezone.eq("UTC"),
            dsl::catchup.eq(false),
            dsl::max_active_runs.eq(10),
            dsl::is_paused.eq(false),
            dsl::next_run_at.eq(now - chrono::Duration::seconds(5)),
            dsl::jitter_secs.eq(0_i64),
            dsl::overlap_policy.eq("skip"),
            dsl::buffered_runs.eq(serde_json::json!([])),
            dsl::buffer_all_max.eq(100),
            dsl::skip_policy.eq("skip"),
            dsl::consecutive_failure_limit.eq(failure_limit),
        ))
        .execute(conn)
        .await
        .expect("insert schedule");
    id
}

// ── Unit tests (compile-time RED markers) ─────────────────────────────────

/// The `METRIC_SCHEDULE_AUTO_PAUSED` constant must be defined with the expected value.
#[test]
fn metric_constant_schedule_auto_paused_is_defined() {
    assert_eq!(METRIC_SCHEDULE_AUTO_PAUSED, "harvest.schedule.auto_paused");
}

/// `MetricsRecorder::record_schedule_auto_paused` must exist and dispatch to the impl.
#[test]
fn metrics_recorder_has_auto_paused_method() {
    let m = RecordingMetrics::default();
    m.record_schedule_auto_paused("my_schedule");
    m.record_schedule_auto_paused("other_schedule");
    let names = m.auto_paused_names();
    assert_eq!(names, vec!["my_schedule", "other_schedule"]);
}

/// `WorkflowSchedule::with_consecutive_failure_limit` must exist and set the field.
#[test]
fn workflow_schedule_builder_has_consecutive_failure_limit() {
    use autumn_harvest::policy::{Schedule, WorkflowSchedule};

    let sched = WorkflowSchedule::new("my_wf", Schedule::Manual).with_consecutive_failure_limit(5);
    assert_eq!(sched.consecutive_failure_limit, Some(5));
}

/// Default `WorkflowSchedule` must have `consecutive_failure_limit = None`.
#[test]
fn workflow_schedule_default_consecutive_failure_limit_is_none() {
    use autumn_harvest::policy::{Schedule, WorkflowSchedule};

    let sched = WorkflowSchedule::new("my_wf", Schedule::Manual);
    assert!(sched.consecutive_failure_limit.is_none());
}

// ── Integration tests ──────────────────────────────────────────────────────

/// A schedule whose `consecutive_failure_count` has reached `consecutive_failure_limit`
/// must be auto-paused on the next tick: `auto_paused_at` is set and no new
/// execution is created.
#[tokio::test]
async fn schedule_auto_pauses_after_consecutive_failures() {
    let (mut conn, url, _c) = setup_db().await;

    let wf_name = "ap_auto_pause_wf";
    let sched_id = insert_schedule_with_failure_limit(&mut conn, wf_name, 3).await;

    // Simulate 3 prior failures by setting the counter directly.
    // (In production this counter is incremented by the worker completion path.)
    diesel::update(harvest_schedules::table.find(sched_id))
        .set(harvest_schedules::dsl::consecutive_failure_count.eq(3))
        .execute(&mut conn)
        .await
        .expect("set failure count");

    drop(conn);

    let pool = make_pool(&url);
    let registry = make_registry(wf_name);
    let dags = Arc::new(DagCatalog::default());

    tick_once(
        pool.clone(),
        registry,
        dags,
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("tick_once must succeed");

    let mut check = AsyncPgConnection::establish(&url)
        .await
        .expect("check conn");

    // The schedule must be auto-paused.
    let auto_paused_at: Option<chrono::DateTime<Utc>> = harvest_schedules::table
        .find(sched_id)
        .select(harvest_schedules::dsl::auto_paused_at)
        .first(&mut check)
        .await
        .expect("select auto_paused_at");
    assert!(
        auto_paused_at.is_some(),
        "auto_paused_at must be set after consecutive_failure_count reached the limit"
    );

    // No new execution must have been created.
    let exec_count: i64 = autumn_harvest::schema::harvest_workflow_executions::table
        .filter(autumn_harvest::schema::harvest_workflow_executions::dsl::workflow_name.eq(wf_name))
        .count()
        .get_result(&mut check)
        .await
        .expect("count executions");
    assert_eq!(
        exec_count, 0,
        "no execution must be created when the schedule is auto-paused on this tick"
    );
}

/// A subsequent tick on an already-auto-paused schedule must be a no-op:
/// `auto_paused_at` stays set and no new execution is created.
#[tokio::test]
async fn auto_paused_schedule_remains_paused_on_subsequent_ticks() {
    let (mut conn, url, _c) = setup_db().await;

    let wf_name = "ap_remains_paused_wf";
    let sched_id = insert_schedule_with_failure_limit(&mut conn, wf_name, 2).await;

    let pause_time = Utc::now() - chrono::Duration::seconds(30);
    diesel::update(harvest_schedules::table.find(sched_id))
        .set((
            harvest_schedules::dsl::consecutive_failure_count.eq(2),
            harvest_schedules::dsl::auto_paused_at.eq(Some(pause_time)),
        ))
        .execute(&mut conn)
        .await
        .expect("set auto-paused state");

    drop(conn);

    let pool = make_pool(&url);
    let registry = make_registry(wf_name);
    let dags = Arc::new(DagCatalog::default());

    // Two extra ticks: neither must fire nor clear the pause.
    for _ in 0..2 {
        tick_once(
            pool.clone(),
            Arc::clone(&registry),
            Arc::clone(&dags),
            Arc::new(vec![]),
            SchedulerMonitor::offline(),
        )
        .await
        .expect("tick must not error");
    }

    let mut check = AsyncPgConnection::establish(&url)
        .await
        .expect("check conn");

    let auto_paused_at: Option<chrono::DateTime<Utc>> = harvest_schedules::table
        .find(sched_id)
        .select(harvest_schedules::dsl::auto_paused_at)
        .first(&mut check)
        .await
        .expect("select auto_paused_at");
    assert!(
        auto_paused_at.is_some(),
        "auto_paused_at must remain set through subsequent ticks"
    );

    let exec_count: i64 = autumn_harvest::schema::harvest_workflow_executions::table
        .filter(autumn_harvest::schema::harvest_workflow_executions::dsl::workflow_name.eq(wf_name))
        .count()
        .get_result(&mut check)
        .await
        .expect("count executions");
    assert_eq!(
        exec_count, 0,
        "no execution must be created on a paused schedule"
    );
}

/// Resuming a schedule (clearing `auto_paused_at` and resetting
/// `consecutive_failure_count = 0`) must allow the next tick to fire normally.
#[tokio::test]
async fn schedule_resumes_and_resets_failure_count() {
    let (mut conn, url, _c) = setup_db().await;

    let wf_name = "ap_resume_wf";
    let sched_id = insert_schedule_with_failure_limit(&mut conn, wf_name, 3).await;

    // Put the schedule into auto-paused state.
    diesel::update(harvest_schedules::table.find(sched_id))
        .set((
            harvest_schedules::dsl::consecutive_failure_count.eq(3),
            harvest_schedules::dsl::auto_paused_at.eq(Some(Utc::now())),
        ))
        .execute(&mut conn)
        .await
        .expect("set auto-paused state");

    // Simulate a resume: clear auto_paused_at and reset the counter to 0.
    // This mirrors what `POST /admin/schedules/{id}/resume` does.
    diesel::update(harvest_schedules::table.find(sched_id))
        .set((
            harvest_schedules::dsl::auto_paused_at.eq(Option::<chrono::DateTime<Utc>>::None),
            harvest_schedules::dsl::consecutive_failure_count.eq(0),
            harvest_schedules::dsl::is_paused.eq(false),
        ))
        .execute(&mut conn)
        .await
        .expect("resume schedule");

    // Verify the schedule is no longer auto-paused and count is reset.
    let (auto_paused_at, failure_count): (Option<chrono::DateTime<Utc>>, i32) =
        harvest_schedules::table
            .find(sched_id)
            .select((
                harvest_schedules::dsl::auto_paused_at,
                harvest_schedules::dsl::consecutive_failure_count,
            ))
            .first(&mut conn)
            .await
            .expect("select after resume");
    assert!(
        auto_paused_at.is_none(),
        "auto_paused_at must be NULL after resume"
    );
    assert_eq!(
        failure_count, 0,
        "consecutive_failure_count must be reset to 0 after resume"
    );

    drop(conn);

    // The next tick must fire normally (count = 0 < limit = 3).
    let pool = make_pool(&url);
    let registry = make_registry(wf_name);
    let dags = Arc::new(DagCatalog::default());

    tick_once(
        pool.clone(),
        registry,
        dags,
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("tick_once must succeed after resume");

    let mut check = AsyncPgConnection::establish(&url)
        .await
        .expect("check conn");

    // The tick should have fired the schedule, creating an execution.
    let exec_count: i64 = autumn_harvest::schema::harvest_workflow_executions::table
        .filter(autumn_harvest::schema::harvest_workflow_executions::dsl::workflow_name.eq(wf_name))
        .count()
        .get_result(&mut check)
        .await
        .expect("count executions");
    assert_eq!(
        exec_count, 1,
        "exactly one execution must be created after resume"
    );

    // auto_paused_at must still be NULL after the successful tick.
    let auto_paused_after_tick: Option<chrono::DateTime<Utc>> = harvest_schedules::table
        .find(sched_id)
        .select(harvest_schedules::dsl::auto_paused_at)
        .first(&mut check)
        .await
        .expect("select auto_paused_at after tick");
    assert!(
        auto_paused_after_tick.is_none(),
        "auto_paused_at must remain NULL after a successful tick following resume"
    );
}

/// `consecutive_failure_count` below the limit must NOT trigger auto-pause:
/// the schedule must fire normally.
#[tokio::test]
async fn schedule_fires_when_below_failure_limit() {
    let (mut conn, url, _c) = setup_db().await;

    let wf_name = "ap_below_limit_wf";
    let sched_id = insert_schedule_with_failure_limit(&mut conn, wf_name, 5).await;

    // Set count = 2, which is below the limit of 5.
    diesel::update(harvest_schedules::table.find(sched_id))
        .set(harvest_schedules::dsl::consecutive_failure_count.eq(2))
        .execute(&mut conn)
        .await
        .expect("set failure count");

    drop(conn);

    let pool = make_pool(&url);
    let registry = make_registry(wf_name);
    let dags = Arc::new(DagCatalog::default());

    tick_once(
        pool.clone(),
        registry,
        dags,
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("tick_once must succeed");

    let mut check = AsyncPgConnection::establish(&url)
        .await
        .expect("check conn");

    // Schedule must NOT be auto-paused.
    let auto_paused_at: Option<chrono::DateTime<Utc>> = harvest_schedules::table
        .find(sched_id)
        .select(harvest_schedules::dsl::auto_paused_at)
        .first(&mut check)
        .await
        .expect("select auto_paused_at");
    assert!(
        auto_paused_at.is_none(),
        "auto_paused_at must remain NULL when count ({}) is below limit (5)",
        2
    );

    // Execution must have been created.
    let exec_count: i64 = autumn_harvest::schema::harvest_workflow_executions::table
        .filter(autumn_harvest::schema::harvest_workflow_executions::dsl::workflow_name.eq(wf_name))
        .count()
        .get_result(&mut check)
        .await
        .expect("count executions");
    assert_eq!(exec_count, 1, "one execution must be created");
}

/// A schedule without `consecutive_failure_limit` (limit IS NULL) must never
/// auto-pause regardless of `consecutive_failure_count`.
#[tokio::test]
async fn schedule_without_limit_never_auto_pauses() {
    let (mut conn, url, _c) = setup_db().await;

    let wf_name = "ap_no_limit_wf";
    // Insert without a failure limit.
    let sched_id = {
        use autumn_harvest::schema::harvest_schedules::dsl;
        let now = Utc::now();
        let id = Uuid::new_v4();
        diesel::insert_into(harvest_schedules::table)
            .values((
                dsl::id.eq(id),
                dsl::workflow_name.eq(wf_name),
                dsl::schedule_expr.eq("interval:60"),
                dsl::timezone.eq("UTC"),
                dsl::catchup.eq(false),
                dsl::max_active_runs.eq(10),
                dsl::is_paused.eq(false),
                dsl::next_run_at.eq(now - chrono::Duration::seconds(5)),
                dsl::jitter_secs.eq(0_i64),
                dsl::overlap_policy.eq("skip"),
                dsl::buffered_runs.eq(serde_json::json!([])),
                dsl::buffer_all_max.eq(100),
                dsl::skip_policy.eq("skip"),
                // consecutive_failure_limit intentionally omitted (NULL by default)
            ))
            .execute(&mut conn)
            .await
            .expect("insert schedule");
        id
    };

    // Set a high failure count — should not matter with no limit.
    diesel::update(harvest_schedules::table.find(sched_id))
        .set(harvest_schedules::dsl::consecutive_failure_count.eq(100))
        .execute(&mut conn)
        .await
        .expect("set failure count");

    drop(conn);

    let pool = make_pool(&url);
    let registry = make_registry(wf_name);
    let dags = Arc::new(DagCatalog::default());

    tick_once(
        pool.clone(),
        registry,
        dags,
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("tick_once must succeed");

    let mut check = AsyncPgConnection::establish(&url)
        .await
        .expect("check conn");

    let auto_paused_at: Option<chrono::DateTime<Utc>> = harvest_schedules::table
        .find(sched_id)
        .select(harvest_schedules::dsl::auto_paused_at)
        .first(&mut check)
        .await
        .expect("select auto_paused_at");
    assert!(
        auto_paused_at.is_none(),
        "auto_paused_at must never be set when consecutive_failure_limit is NULL"
    );

    let exec_count: i64 = autumn_harvest::schema::harvest_workflow_executions::table
        .filter(autumn_harvest::schema::harvest_workflow_executions::dsl::workflow_name.eq(wf_name))
        .count()
        .get_result(&mut check)
        .await
        .expect("count executions");
    assert_eq!(exec_count, 1, "execution must fire when limit is NULL");
}
