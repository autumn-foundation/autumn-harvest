#![cfg(feature = "db")]
//! Bounded schedule run tests — issue #478.
//!
//! Verifies that `WorkflowSchedule::end_at` and `WorkflowSchedule::max_runs`
//! correctly terminate schedules after the configured boundary, and that skipped
//! firings (overlap policy, calendar) do NOT consume the `max_runs` budget.

use std::sync::Arc;

use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::schema::{harvest_schedule_decisions, harvest_schedules};
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
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// All migrations up to and including the bounded-runs migration.
const INIT_SQL: &str = concat!(
    include_str!("../../migrations/20260409000000_harvest_initial/up.sql"),
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_set_at TIMESTAMPTZ NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_until TIMESTAMPTZ NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_reason TEXT NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_actor TEXT NULL;\n",
    "\n",
    include_str!("../../migrations/20260619000000_harvest_task_queue_created_at/up.sql"),
    "\n",
    include_str!("../../migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../../migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../../migrations/20260427000000_harvest_continue_as_new/up.sql"),
    "\n",
    include_str!("../../migrations/20260429000000_harvest_concurrency_key/up.sql"),
    "\n",
    include_str!("../../migrations/20260430000000_harvest_workflow_schedules/up.sql"),
    "\n",
    include_str!("../../migrations/20260430000001_harvest_external_tasks/up.sql"),
    "\n",
    include_str!("../../migrations/20260508000000_harvest_external_task_updated_at/up.sql"),
    "\n",
    include_str!("../../migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../../migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../../migrations/20260508010000_harvest_workers_drain_deadline/up.sql"),
    "\n",
    include_str!("../../migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!("../../migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"),
    "\n",
    include_str!("../../migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    include_str!("../../migrations/20260518000000_harvest_signal_idempotency/up.sql"),
    "\n",
    include_str!("../../migrations/20260517000000_harvest_schedule_jitter/up.sql"),
    "\n",
    include_str!("../../migrations/20260517000001_harvest_schedule_overlap_policy/up.sql"),
    "\n",
    include_str!("../../migrations/20260518000001_harvest_workflow_execution_timeout/up.sql"),
    "\n",
    include_str!("../../migrations/20260613000000_harvest_workflow_sla/up.sql"),
    "\n",
    include_str!("../../migrations/20260519000000_harvest_calendar_awareness/up.sql"),
    "\n",
    include_str!("../../migrations/20260522000000_harvest_schedule_decisions/up.sql"),
    "\n",
    include_str!("../../migrations/20260522000001_harvest_rate_limiting/up.sql"),
    "\n",
    include_str!("../../migrations/20260526000001_harvest_parent_close_policy/up.sql"),
    "\n",
    include_str!("../../migrations/20260530000000_harvest_schedule_ha_claim/up.sql"),
    "\n",
    include_str!("../../migrations/20260601000000_harvest_schedule_auto_pause/up.sql"),
    "\n",
    include_str!("../../migrations/20260601000001_harvest_poison_pill_strikes/up.sql"),
    "\n",
    include_str!("../../migrations/20260601000002_harvest_ownership_metadata/up.sql"),
    "\n",
    include_str!("../../migrations/20260603000000_harvest_completion_triggers/up.sql"),
    include_str!("../../migrations/20260708000001_harvest_completion_trigger_condition/up.sql"),
    include_str!("../../migrations/20260605000000_harvest_admission_gates/up.sql"),
    include_str!("../../migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"),
    include_str!("../../migrations/20260607000000_harvest_worker_capability_labels/up.sql"),
    include_str!("../../migrations/20260607000001_harvest_task_required_capabilities/up.sql"),
    "\n",
    include_str!("../../migrations/20260607000002_harvest_workflow_pause/up.sql"),
    "\n",
    include_str!("../../migrations/20260609000001_harvest_workflow_current_details/up.sql"),
    "\n",
    // Bounded runs (issue #478)
    include_str!("../../migrations/20260610000001_harvest_schedule_bounded_runs/up.sql"),
    "\n",
    include_str!("../../migrations/20260613000001_harvest_schedule_catchup_window/up.sql"),
    "\n",
    include_str!("../../migrations/20260616000001_harvest_workflow_schedule_id/up.sql"),
    "\n",
    include_str!("../../migrations/20260615000001_harvest_context_headers/up.sql"),
    "\n",
    // issue #523: workflow-level retry policy columns.
    include_str!("../../migrations/20260626000001_harvest_workflow_retry/up.sql"),
    "\n",
    // issue #534: origin column + per-schedule run-history index.
    include_str!("../../migrations/20260628000001_harvest_execution_origin/up.sql"),
    include_str!("../../migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"),
    include_str!("../../migrations/20260704000001_harvest_build_policy_ramp/up.sql"),
    include_str!("../../migrations/20260704000000_harvest_workflow_nd_block/up.sql"),
    "\n",
    include_str!("../../migrations/20260705000000_harvest_completion_deliveries/up.sql"),
    include_str!("../../migrations/20260706000000_harvest_worker_sessions/up.sql"),
    include_str!("../../migrations/20260710000002_harvest_workflow_continue_chain/up.sql"),
);

// ── Unit tests (no DB) ─────────────────────────────────────────────────────

/// `WorkflowSchedule` must have an `end_at` field defaulting to `None`.
#[test]
fn workflow_schedule_end_at_defaults_to_none() {
    use autumn_harvest::policy::{Schedule, WorkflowSchedule};
    let sched = WorkflowSchedule::new("my_wf", Schedule::Manual);
    assert!(sched.end_at.is_none(), "end_at must default to None");
}

/// `WorkflowSchedule` must have a `max_runs` field defaulting to `None`.
#[test]
fn workflow_schedule_max_runs_defaults_to_none() {
    use autumn_harvest::policy::{Schedule, WorkflowSchedule};
    let sched = WorkflowSchedule::new("my_wf", Schedule::Manual);
    assert!(sched.max_runs.is_none(), "max_runs must default to None");
}

/// `WorkflowSchedule::with_end_at` must set the `end_at` field.
#[test]
fn workflow_schedule_with_end_at_builder() {
    use autumn_harvest::policy::{Schedule, WorkflowSchedule};
    let cutoff = Utc::now() + chrono::Duration::days(7);
    let sched = WorkflowSchedule::new("my_wf", Schedule::Manual).with_end_at(cutoff);
    assert_eq!(sched.end_at, Some(cutoff));
}

/// `WorkflowSchedule::with_max_runs` must set the `max_runs` field.
#[test]
fn workflow_schedule_with_max_runs_builder() {
    use autumn_harvest::policy::{Schedule, WorkflowSchedule};
    let sched = WorkflowSchedule::new("my_wf", Schedule::Manual).with_max_runs(24);
    assert_eq!(sched.max_runs, Some(24));
}

/// Both builder methods can be chained.
#[test]
fn workflow_schedule_end_conditions_can_be_chained() {
    use autumn_harvest::policy::{Schedule, WorkflowSchedule};
    let cutoff = Utc::now() + chrono::Duration::days(30);
    let sched = WorkflowSchedule::new(
        "promo_wf",
        Schedule::Interval(std::time::Duration::from_secs(86400)),
    )
    .with_end_at(cutoff)
    .with_max_runs(10);
    assert_eq!(sched.end_at, Some(cutoff));
    assert_eq!(sched.max_runs, Some(10));
}

/// A default schedule (no end conditions) must still have `None` on both fields.
#[test]
fn workflow_schedule_no_end_conditions_fires_forever() {
    use autumn_harvest::policy::{Schedule, WorkflowSchedule};
    let sched = WorkflowSchedule::new("forever_wf", Schedule::Cron("0 * * * *".to_string()));
    assert!(sched.end_at.is_none());
    assert!(sched.max_runs.is_none());
}

// ── Integration tests ──────────────────────────────────────────────────────

async fn setup_db() -> (AsyncPgConnection, String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres start");
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
            mcp: false,
            name: workflow_name,
            module: "scheduler_bounded_runs_tests",
            handler: noop_handler,
            execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,
            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
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

/// Insert a due schedule row, optionally with `end_at` and/or `max_runs`.
async fn insert_bounded_schedule(
    conn: &mut AsyncPgConnection,
    wf_name: &str,
    end_at: Option<chrono::DateTime<Utc>>,
    max_runs: Option<i32>,
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
            dsl::end_at.eq(end_at),
            dsl::max_runs.eq(max_runs),
        ))
        .execute(conn)
        .await
        .expect("insert schedule");
    id
}

/// When `next_run_at >= end_at`, the scheduler must NOT start a run and must
/// transition the schedule to an exhausted state (`exhausted_at IS NOT NULL`,
/// `exhausted_reason = "end_at_reached"`).
#[tokio::test]
async fn schedule_exhausted_by_end_at_does_not_fire() {
    let (mut conn, url, _c) = setup_db().await;

    let wf_name = "br_end_at_stop_wf";
    // end_at is in the past relative to next_run_at (which is 5s ago).
    let end_at = Utc::now() - chrono::Duration::seconds(60);
    let sched_id = insert_bounded_schedule(&mut conn, wf_name, Some(end_at), None).await;
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

    // No execution must have been created.
    let exec_count: i64 = autumn_harvest::schema::harvest_workflow_executions::table
        .filter(autumn_harvest::schema::harvest_workflow_executions::dsl::workflow_name.eq(wf_name))
        .count()
        .get_result(&mut check)
        .await
        .expect("count executions");
    assert_eq!(
        exec_count, 0,
        "no run must be started when end_at is reached"
    );

    // Schedule must be exhausted.
    let (exhausted_at, exhausted_reason): (Option<chrono::DateTime<Utc>>, Option<String>) =
        harvest_schedules::table
            .find(sched_id)
            .select((
                harvest_schedules::dsl::exhausted_at,
                harvest_schedules::dsl::exhausted_reason,
            ))
            .first(&mut check)
            .await
            .expect("select exhausted fields");

    assert!(
        exhausted_at.is_some(),
        "exhausted_at must be set when end_at is reached"
    );
    assert_eq!(
        exhausted_reason.as_deref(),
        Some("end_at_reached"),
        "exhausted_reason must be 'end_at_reached'"
    );
}

/// When `next_run_at < end_at`, the schedule must fire normally.
#[tokio::test]
async fn schedule_before_end_at_fires_normally() {
    let (mut conn, url, _c) = setup_db().await;

    let wf_name = "br_before_end_at_wf";
    // end_at is 1 hour in the future — schedule is still active.
    let end_at = Utc::now() + chrono::Duration::hours(1);
    let sched_id = insert_bounded_schedule(&mut conn, wf_name, Some(end_at), None).await;
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

    let exec_count: i64 = autumn_harvest::schema::harvest_workflow_executions::table
        .filter(autumn_harvest::schema::harvest_workflow_executions::dsl::workflow_name.eq(wf_name))
        .count()
        .get_result(&mut check)
        .await
        .expect("count executions");
    assert_eq!(
        exec_count, 1,
        "exactly one execution must be created when before end_at"
    );

    let exhausted_at: Option<chrono::DateTime<Utc>> = harvest_schedules::table
        .find(sched_id)
        .select(harvest_schedules::dsl::exhausted_at)
        .first(&mut check)
        .await
        .expect("select exhausted_at");
    assert!(
        exhausted_at.is_none(),
        "exhausted_at must remain NULL when end_at has not been reached"
    );
}

/// When `runs_started >= max_runs`, the scheduler must NOT start a run and must
/// transition the schedule to an exhausted state with reason `"max_runs_exhausted"`.
#[tokio::test]
async fn schedule_exhausted_by_max_runs_does_not_fire() {
    let (mut conn, url, _c) = setup_db().await;

    let wf_name = "br_max_runs_stop_wf";
    // Budget of 3, already used up.
    let sched_id = insert_bounded_schedule(&mut conn, wf_name, None, Some(3)).await;

    // Simulate budget already exhausted by setting runs_started = 3.
    diesel::update(harvest_schedules::table.find(sched_id))
        .set(harvest_schedules::dsl::runs_started.eq(3))
        .execute(&mut conn)
        .await
        .expect("set runs_started");
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

    let exec_count: i64 = autumn_harvest::schema::harvest_workflow_executions::table
        .filter(autumn_harvest::schema::harvest_workflow_executions::dsl::workflow_name.eq(wf_name))
        .count()
        .get_result(&mut check)
        .await
        .expect("count executions");
    assert_eq!(
        exec_count, 0,
        "no run must be started when max_runs budget is exhausted"
    );

    let (exhausted_at, exhausted_reason): (Option<chrono::DateTime<Utc>>, Option<String>) =
        harvest_schedules::table
            .find(sched_id)
            .select((
                harvest_schedules::dsl::exhausted_at,
                harvest_schedules::dsl::exhausted_reason,
            ))
            .first(&mut check)
            .await
            .expect("select exhausted fields");

    assert!(
        exhausted_at.is_some(),
        "exhausted_at must be set when max_runs is reached"
    );
    assert_eq!(
        exhausted_reason.as_deref(),
        Some("max_runs_exhausted"),
        "exhausted_reason must be 'max_runs_exhausted'"
    );
}

/// A schedule with `max_runs = 1` must fire exactly once, then be exhausted.
#[tokio::test]
async fn max_runs_1_fires_once_then_exhausts() {
    let (mut conn, url, _c) = setup_db().await;

    let wf_name = "br_max_runs_1_wf";
    let sched_id = insert_bounded_schedule(&mut conn, wf_name, None, Some(1)).await;
    drop(conn);

    let pool = make_pool(&url);
    let registry = make_registry(wf_name);
    let dags = Arc::new(DagCatalog::default());

    // First tick: must fire and exhaust.
    tick_once(
        pool.clone(),
        Arc::clone(&registry),
        Arc::clone(&dags),
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("first tick must succeed");

    let mut check = AsyncPgConnection::establish(&url)
        .await
        .expect("check conn");

    let exec_count: i64 = autumn_harvest::schema::harvest_workflow_executions::table
        .filter(autumn_harvest::schema::harvest_workflow_executions::dsl::workflow_name.eq(wf_name))
        .count()
        .get_result(&mut check)
        .await
        .expect("count executions after first tick");
    assert_eq!(
        exec_count, 1,
        "exactly one execution must be created on first tick"
    );

    let (exhausted_at, runs_started): (Option<chrono::DateTime<Utc>>, i32) =
        harvest_schedules::table
            .find(sched_id)
            .select((
                harvest_schedules::dsl::exhausted_at,
                harvest_schedules::dsl::runs_started,
            ))
            .first(&mut check)
            .await
            .expect("select after first tick");
    assert!(
        exhausted_at.is_some(),
        "schedule must be exhausted after consuming max_runs=1"
    );
    assert_eq!(
        runs_started, 1,
        "runs_started must equal 1 after one dispatch"
    );
}

/// `runs_started` must be incremented for each dispatched run.
#[tokio::test]
async fn runs_started_incremented_per_dispatch() {
    let (mut conn, url, _c) = setup_db().await;

    let wf_name = "br_runs_started_wf";
    // Budget of 5, nothing consumed yet.
    let sched_id = insert_bounded_schedule(&mut conn, wf_name, None, Some(5)).await;
    drop(conn);

    let pool = make_pool(&url);
    let registry = make_registry(wf_name);
    let dags = Arc::new(DagCatalog::default());

    // One tick fires one run (no catchup).
    tick_once(
        pool.clone(),
        registry,
        dags,
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("tick_once");

    let mut check = AsyncPgConnection::establish(&url)
        .await
        .expect("check conn");
    let runs_started: i32 = harvest_schedules::table
        .find(sched_id)
        .select(harvest_schedules::dsl::runs_started)
        .first(&mut check)
        .await
        .expect("select runs_started");
    assert_eq!(
        runs_started, 1,
        "runs_started must be 1 after one successful dispatch"
    );

    let exhausted_at: Option<chrono::DateTime<Utc>> = harvest_schedules::table
        .find(sched_id)
        .select(harvest_schedules::dsl::exhausted_at)
        .first(&mut check)
        .await
        .expect("select exhausted_at");
    assert!(
        exhausted_at.is_none(),
        "schedule must not be exhausted when budget remains"
    );
}

/// An already-exhausted schedule must not fire again on subsequent ticks.
#[tokio::test]
async fn exhausted_schedule_stays_exhausted() {
    let (mut conn, url, _c) = setup_db().await;

    let wf_name = "br_stays_exhausted_wf";
    let sched_id = insert_bounded_schedule(&mut conn, wf_name, None, Some(2)).await;
    // Pre-exhaust by setting runs_started = max_runs = 2.
    diesel::update(harvest_schedules::table.find(sched_id))
        .set((
            harvest_schedules::dsl::runs_started.eq(2),
            harvest_schedules::dsl::exhausted_at.eq(Some(Utc::now())),
            harvest_schedules::dsl::exhausted_reason.eq(Some("max_runs_exhausted")),
        ))
        .execute(&mut conn)
        .await
        .expect("pre-exhaust");
    drop(conn);

    let pool = make_pool(&url);
    let registry = make_registry(wf_name);
    let dags = Arc::new(DagCatalog::default());

    // Three extra ticks: all must be no-ops.
    for _ in 0..3 {
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

    let exec_count: i64 = autumn_harvest::schema::harvest_workflow_executions::table
        .filter(autumn_harvest::schema::harvest_workflow_executions::dsl::workflow_name.eq(wf_name))
        .count()
        .get_result(&mut check)
        .await
        .expect("count executions");
    assert_eq!(
        exec_count, 0,
        "no execution must be created for an exhausted schedule"
    );
}

/// A schedule without any end conditions (both `end_at` and `max_runs` NULL)
/// must continue to fire normally — backward compat.
#[tokio::test]
async fn schedule_without_bounds_fires_normally() {
    let (mut conn, url, _c) = setup_db().await;

    let wf_name = "br_unbounded_wf";
    let sched_id = insert_bounded_schedule(&mut conn, wf_name, None, None).await;
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
    .expect("tick_once");

    let mut check = AsyncPgConnection::establish(&url)
        .await
        .expect("check conn");
    let exec_count: i64 = autumn_harvest::schema::harvest_workflow_executions::table
        .filter(autumn_harvest::schema::harvest_workflow_executions::dsl::workflow_name.eq(wf_name))
        .count()
        .get_result(&mut check)
        .await
        .expect("count executions");
    assert_eq!(exec_count, 1, "unbounded schedule must fire normally");

    let exhausted_at: Option<chrono::DateTime<Utc>> = harvest_schedules::table
        .find(sched_id)
        .select(harvest_schedules::dsl::exhausted_at)
        .first(&mut check)
        .await
        .expect("select exhausted_at");
    assert!(
        exhausted_at.is_none(),
        "unbounded schedule must never be exhausted"
    );
}

/// A skipped run (operator-paused) must NOT consume `max_runs` budget.
/// The schedule is paused so no run fires, and `runs_started` stays 0.
#[tokio::test]
async fn paused_schedule_does_not_consume_budget() {
    let (mut conn, url, _c) = setup_db().await;

    let wf_name = "br_paused_no_budget_wf";
    let sched_id = insert_bounded_schedule(&mut conn, wf_name, None, Some(5)).await;

    // Pause the schedule so the tick skips it.
    diesel::update(harvest_schedules::table.find(sched_id))
        .set(harvest_schedules::dsl::is_paused.eq(true))
        .execute(&mut conn)
        .await
        .expect("pause schedule");
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
    .expect("tick_once");

    let mut check = AsyncPgConnection::establish(&url)
        .await
        .expect("check conn");
    let runs_started: i32 = harvest_schedules::table
        .find(sched_id)
        .select(harvest_schedules::dsl::runs_started)
        .first(&mut check)
        .await
        .expect("select runs_started");
    assert_eq!(
        runs_started, 0,
        "paused (skipped) schedule must not consume max_runs budget"
    );
}

/// When a schedule is exhausted by `end_at`, the `harvest_schedule_decisions`
/// table must contain a row with `decision = 'skipped'` and
/// `reason_code = 'end_at_reached'`.
#[tokio::test]
async fn decision_log_records_end_at_reached() {
    let (mut conn, url, _c) = setup_db().await;

    let wf_name = "br_decision_end_at_wf";
    let end_at = Utc::now() - chrono::Duration::minutes(1);
    let sched_id = insert_bounded_schedule(&mut conn, wf_name, Some(end_at), None).await;
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
    .expect("tick_once");

    let mut check = AsyncPgConnection::establish(&url)
        .await
        .expect("check conn");

    let (decision, reason_code): (String, String) = harvest_schedule_decisions::table
        .filter(harvest_schedule_decisions::dsl::schedule_id.eq(Some(sched_id)))
        .select((
            harvest_schedule_decisions::dsl::decision,
            harvest_schedule_decisions::dsl::reason_code,
        ))
        .first(&mut check)
        .await
        .expect("select decision row");

    assert_eq!(
        decision, "skipped",
        "decision must be 'skipped' for end_at exhaustion"
    );
    assert_eq!(
        reason_code, "end_at_reached",
        "reason_code must be 'end_at_reached'"
    );
}

/// When a schedule is exhausted by `max_runs`, the `harvest_schedule_decisions`
/// table must contain a row with `decision = 'skipped'` and
/// `reason_code = 'max_runs_exhausted'`.
#[tokio::test]
async fn decision_log_records_max_runs_exhausted() {
    let (mut conn, url, _c) = setup_db().await;

    let wf_name = "br_decision_max_runs_wf";
    let sched_id = insert_bounded_schedule(&mut conn, wf_name, None, Some(1)).await;

    // Pre-exhaust: runs_started = 1 = max_runs.
    diesel::update(harvest_schedules::table.find(sched_id))
        .set(harvest_schedules::dsl::runs_started.eq(1))
        .execute(&mut conn)
        .await
        .expect("set runs_started");
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
    .expect("tick_once");

    let mut check = AsyncPgConnection::establish(&url)
        .await
        .expect("check conn");

    let (decision, reason_code): (String, String) = harvest_schedule_decisions::table
        .filter(harvest_schedule_decisions::dsl::schedule_id.eq(Some(sched_id)))
        .select((
            harvest_schedule_decisions::dsl::decision,
            harvest_schedule_decisions::dsl::reason_code,
        ))
        .first(&mut check)
        .await
        .expect("select decision row");

    assert_eq!(
        decision, "skipped",
        "decision must be 'skipped' for max_runs exhaustion"
    );
    assert_eq!(
        reason_code, "max_runs_exhausted",
        "reason_code must be 'max_runs_exhausted'"
    );
}
