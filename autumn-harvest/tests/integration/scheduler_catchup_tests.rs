#![cfg(feature = "db")]
//! Bounded schedule catchup window tests — issue #484.
//!
//! Verifies that `CatchupPolicy` correctly controls how many missed slots are
//! fired after downtime, and that dropped counts are persisted to the
//! `last_catchup_dropped` / `last_catchup_at` columns.

use std::sync::Arc;

use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::policy::{CatchupPolicy, Schedule, WorkflowSchedule};
use autumn_harvest::schema::harvest_schedules;
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

// All migrations up to and including the catchup-window migration.
const INIT_SQL: &str = concat!(
    include_str!("../../migrations/20260409000000_harvest_initial/up.sql"),
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
    include_str!("../../migrations/20260605000000_harvest_admission_gates/up.sql"),
    include_str!("../../migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"),
    include_str!("../../migrations/20260607000000_harvest_worker_capability_labels/up.sql"),
    include_str!("../../migrations/20260607000001_harvest_task_required_capabilities/up.sql"),
    "\n",
    include_str!("../../migrations/20260607000002_harvest_workflow_pause/up.sql"),
    "\n",
    include_str!("../../migrations/20260609000001_harvest_workflow_current_details/up.sql"),
    "\n",
    include_str!("../../migrations/20260610000001_harvest_schedule_bounded_runs/up.sql"),
    "\n",
    include_str!("../../migrations/20260611000001_harvest_stalled_workflow_index/up.sql"),
    "\n",
    include_str!("../../migrations/20260613000000_harvest_workflow_sla/up.sql"),
    "\n",
    // Catchup window (issue #484)
    include_str!("../../migrations/20260613000000_harvest_workflow_sla/up.sql"),
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
);

// ── Unit tests (no DB) ─────────────────────────────────────────────────────

#[test]
fn catchup_policy_defaults_to_none() {
    let sched = WorkflowSchedule::new("my_wf", Schedule::Manual);
    assert!(sched.catchup_policy.is_none());
}

#[test]
fn with_catchup_policy_sets_most_recent() {
    let sched = WorkflowSchedule::new("my_wf", Schedule::Manual)
        .with_catchup_policy(CatchupPolicy::MostRecent);
    assert_eq!(sched.catchup_policy, Some(CatchupPolicy::MostRecent));
}

#[test]
fn with_catchup_window_sets_window_policy() {
    let window = std::time::Duration::from_secs(3600);
    let sched = WorkflowSchedule::new("my_wf", Schedule::Manual).with_catchup_window(window);
    assert_eq!(sched.catchup_policy, Some(CatchupPolicy::Window(window)));
}

#[test]
fn catchup_policy_from_db_null_uses_bool_false() {
    let policy = CatchupPolicy::from_db(None, None, false);
    assert_eq!(policy, CatchupPolicy::SkipAll);
}

#[test]
fn catchup_policy_from_db_null_uses_bool_true() {
    let policy = CatchupPolicy::from_db(None, None, true);
    assert_eq!(policy, CatchupPolicy::Unbounded);
}

#[test]
fn catchup_policy_from_db_most_recent() {
    let policy = CatchupPolicy::from_db(Some("most_recent"), None, false);
    assert_eq!(policy, CatchupPolicy::MostRecent);
}

#[test]
fn catchup_policy_from_db_window() {
    let policy = CatchupPolicy::from_db(Some("window"), Some(900), false);
    assert_eq!(
        policy,
        CatchupPolicy::Window(std::time::Duration::from_secs(900))
    );
}

#[test]
fn catchup_policy_from_db_unknown_falls_back_to_bool() {
    let policy = CatchupPolicy::from_db(Some("future_policy"), None, true);
    assert_eq!(policy, CatchupPolicy::Unbounded);
}

// ── Integration test helpers ───────────────────────────────────────────────

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
            module: "scheduler_catchup_tests",
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

/// Insert a schedule row with a specific catchup policy and `next_run_at` set
/// in the past by `overdue_secs`. Interval is 60 seconds.
async fn insert_catchup_schedule(
    conn: &mut AsyncPgConnection,
    wf_name: &str,
    catchup_policy: Option<&str>,
    catchup_window_secs: Option<i64>,
    catchup_bool: bool,
    overdue_secs: i64,
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
            dsl::catchup.eq(catchup_bool),
            dsl::max_active_runs.eq(200),
            dsl::is_paused.eq(false),
            dsl::next_run_at.eq(now - chrono::Duration::seconds(overdue_secs)),
            dsl::jitter_secs.eq(0_i64),
            dsl::overlap_policy.eq("allow"),
            dsl::buffered_runs.eq(serde_json::json!([])),
            dsl::buffer_all_max.eq(0),
            dsl::skip_policy.eq("skip"),
            dsl::catchup_policy.eq(catchup_policy),
            dsl::catchup_window_secs.eq(catchup_window_secs),
        ))
        .execute(conn)
        .await
        .expect("insert schedule");
    id
}

// ── Integration tests ──────────────────────────────────────────────────────

/// `MostRecent` policy: 24 h of 60-second slots = ~1440 missed slots.
/// After one tick exactly 1 execution must be created; `last_catchup_dropped`
/// must be 1439 (= `total_missed` - 1).
#[tokio::test]
async fn most_recent_policy_fires_one_and_records_drops() {
    let (mut conn, url, _c) = setup_db().await;

    let wf_name = "cw_most_recent_wf";
    // 24 h overdue on a 60-second interval → ~1440 missed slots.
    let overdue_secs = 24 * 3600_i64;
    let sched_id = insert_catchup_schedule(
        &mut conn,
        wf_name,
        Some("most_recent"),
        None,
        false,
        overdue_secs,
    )
    .await;
    drop(conn);

    let pool = make_pool(&url);
    let registry = make_registry(wf_name);
    let dags = Arc::new(DagCatalog::default());

    tick_once(
        pool.clone(),
        registry.clone(),
        dags.clone(),
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("tick_once must succeed");

    let mut check = AsyncPgConnection::establish(&url)
        .await
        .expect("check conn");

    // Exactly 1 execution must be created.
    let exec_count: i64 = autumn_harvest::schema::harvest_workflow_executions::table
        .filter(autumn_harvest::schema::harvest_workflow_executions::dsl::workflow_name.eq(wf_name))
        .count()
        .get_result(&mut check)
        .await
        .expect("count executions");
    assert_eq!(exec_count, 1, "MostRecent: exactly 1 execution must fire");

    // `last_catchup_dropped` must be > 0 (missed - 1).
    let (dropped, last_catchup_at): (i32, Option<chrono::DateTime<Utc>>) = harvest_schedules::table
        .find(sched_id)
        .select((
            harvest_schedules::dsl::last_catchup_dropped,
            harvest_schedules::dsl::last_catchup_at,
        ))
        .first(&mut check)
        .await
        .expect("select catchup fields");

    assert!(
        dropped > 0,
        "MostRecent: last_catchup_dropped must record the dropped count (got {dropped})"
    );
    assert!(
        last_catchup_at.is_some(),
        "MostRecent: last_catchup_at must be set after a recovery tick"
    );

    // A subsequent ordinary tick (no slots are due now) must NOT reset the
    // recovery audit trail back to 0 / NULL (issue #484 regression guard).
    tick_once(
        pool.clone(),
        registry,
        dags,
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("second tick_once must succeed");

    let (dropped_after, last_catchup_at_after): (i32, Option<chrono::DateTime<Utc>>) =
        harvest_schedules::table
            .find(sched_id)
            .select((
                harvest_schedules::dsl::last_catchup_dropped,
                harvest_schedules::dsl::last_catchup_at,
            ))
            .first(&mut check)
            .await
            .expect("select catchup fields after second tick");

    assert_eq!(
        dropped_after, dropped,
        "second tick must not reset last_catchup_dropped"
    );
    assert_eq!(
        last_catchup_at_after, last_catchup_at,
        "second tick must not reset last_catchup_at"
    );
}

/// Backward-compat regression: a schedule with `catchup = false` (no policy
/// column) fires exactly 1 slot and has `last_catchup_dropped = 0`.
#[tokio::test]
async fn legacy_catchup_false_fires_one_slot_compat() {
    let (mut conn, url, _c) = setup_db().await;

    let wf_name = "cw_legacy_false_wf";
    let overdue_secs = 3600_i64; // 1 hour overdue on 60-second interval.
    let sched_id =
        insert_catchup_schedule(&mut conn, wf_name, None, None, false, overdue_secs).await;
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

    // Exactly 1 execution (the oldest missed slot).
    let exec_count: i64 = autumn_harvest::schema::harvest_workflow_executions::table
        .filter(autumn_harvest::schema::harvest_workflow_executions::dsl::workflow_name.eq(wf_name))
        .count()
        .get_result(&mut check)
        .await
        .expect("count executions");
    assert_eq!(
        exec_count, 1,
        "legacy catchup=false must fire exactly 1 slot"
    );

    // No drops recorded for SkipAll (legacy false).
    let dropped: i32 = harvest_schedules::table
        .find(sched_id)
        .select(harvest_schedules::dsl::last_catchup_dropped)
        .first(&mut check)
        .await
        .expect("select last_catchup_dropped");
    assert_eq!(
        dropped, 0,
        "legacy catchup=false must leave last_catchup_dropped = 0"
    );
}

/// Backward-compat regression: a schedule with `catchup = true` (no policy
/// column) fires all missed slots and has `last_catchup_dropped = 0`.
#[tokio::test]
async fn legacy_catchup_true_fires_all_slots_compat() {
    let (mut conn, url, _c) = setup_db().await;

    let wf_name = "cw_legacy_true_wf";
    // 5 minutes overdue on 60-second interval → 5 missed slots.
    let overdue_secs = 5 * 60_i64;
    let sched_id =
        insert_catchup_schedule(&mut conn, wf_name, None, None, true, overdue_secs).await;
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
    assert!(
        exec_count >= 4,
        "legacy catchup=true must fire all (≥4) missed slots, got {exec_count}"
    );

    let dropped: i32 = harvest_schedules::table
        .find(sched_id)
        .select(harvest_schedules::dsl::last_catchup_dropped)
        .first(&mut check)
        .await
        .expect("select last_catchup_dropped");
    assert_eq!(
        dropped, 0,
        "Unbounded policy must leave last_catchup_dropped = 0"
    );
}

/// `Window` policy: 1-hour window on a schedule overdue by 2 hours on a
/// 60-second interval → ~120 missed slots; only the ~60 within the last hour
/// should fire, the rest are dropped.
#[tokio::test]
async fn window_policy_fires_only_in_window_slots() {
    let (mut conn, url, _c) = setup_db().await;

    let wf_name = "cw_window_wf";
    // 2 hours overdue on 60-second interval → ~120 missed slots.
    let overdue_secs = 2 * 3600_i64;
    let sched_id = insert_catchup_schedule(
        &mut conn,
        wf_name,
        Some("window"),
        Some(3600), // 1-hour window
        false,
        overdue_secs,
    )
    .await;
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

    // ~60 in-window slots — allow some timing slack.
    assert!(
        exec_count > 0 && exec_count < 100,
        "Window(1h): expected roughly 60 executions in window, got {exec_count}"
    );

    let (dropped, last_catchup_at): (i32, Option<chrono::DateTime<Utc>>) = harvest_schedules::table
        .find(sched_id)
        .select((
            harvest_schedules::dsl::last_catchup_dropped,
            harvest_schedules::dsl::last_catchup_at,
        ))
        .first(&mut check)
        .await
        .expect("select catchup fields");

    assert!(
        dropped > 0,
        "Window policy: last_catchup_dropped must record out-of-window drops (got {dropped})"
    );
    assert!(
        last_catchup_at.is_some(),
        "Window policy: last_catchup_at must be set"
    );
}
