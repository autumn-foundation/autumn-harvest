#![cfg(feature = "db")]
#![allow(
    clippy::too_many_lines,
    clippy::items_after_statements,
    clippy::doc_markdown,
    clippy::cast_possible_wrap,
    clippy::literal_string_with_formatting_args
)]
//! Nominal scheduled fire-time injection tests — issue #508.
//!
//! Verifies that `ctx.scheduled_time()` returns the pre-jitter logical slot
//! (`scheduled_for`) for scheduled / backfilled / caught-up runs, and `None`
//! for direct/manual API starts. Also verifies replay-stability.

use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::policy::{Schedule, WorkflowSchedule};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::store;
use autumn_harvest::testing::WorkflowReplayer;
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    DagCatalog, SchedulerMonitor, StartWorkflowParams, WorkflowContext, WorkflowIdReusePolicy,
    register_workflow_schedules, start_or_load_workflow_execution, tick_once,
};
use chrono::Utc;
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use serde_json::json;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// ── Workflow handler ──────────────────────────────────────────────────────────

/// Records `ctx.scheduled_time()` as an RFC3339 string (or null) in its output.
fn scheduled_time_recorder<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>,
> {
    Box::pin(async move {
        let slot: Option<String> = ctx.scheduled_time().map(|t| t.to_rfc3339());
        Ok(json!({ "scheduled_time": slot }))
    })
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async fn setup_db() -> (AsyncPgConnection, String, ContainerAsync<Postgres>) {
    let container = Postgres::default().start().await.expect("postgres start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    conn.batch_execute(autumn_harvest::full_migrations_sql())
        .await
        .expect("migration");
    (conn, url, container)
}

async fn arm_slot(conn: &mut AsyncPgConnection, wf_name: &str, secs_ago: i64) {
    use autumn_harvest::schema::harvest_schedules::dsl;
    diesel::update(dsl::harvest_schedules)
        .filter(dsl::workflow_name.eq(wf_name))
        .set(dsl::next_run_at.eq(Utc::now() - chrono::Duration::seconds(secs_ago)))
        .execute(conn)
        .await
        .expect("arm next_run_at slot");
}

fn make_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool")
}

fn make_registry(wf_name: &'static str) -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: wf_name,
            module: "scheduled_time_tests",
            handler: scheduled_time_recorder,
            execution_timeout: None,
            chain_execution_timeout: None,
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
            sla: None,
        }],
        vec![],
    ))
}

fn make_worker(worker_id: &str, registry: Arc<HandlerRegistry>) -> Arc<Worker> {
    Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                worker_id: worker_id.to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 4,
                max_concurrent_activities: 4,
                poll_interval: Duration::from_millis(20),
                shutdown_timeout: Duration::from_secs(2),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 100,
                priority_aging_secs: None,
                unknown_target_grace_window: Duration::from_secs(5),
                poison_pill_threshold: 3,
                capability_miss_max_redeliveries: 5,
                workflow_task_timeout: std::time::Duration::from_secs(10),
                workflow_panic_max_attempts: 3,
                labels: std::collections::HashMap::new(),
                queue_weights: std::collections::HashMap::new(),
                max_workflow_pause_duration: Duration::from_secs(24 * 3600),
                max_workflow_history_events: None,
                shard_notification_database_urls: Vec::new(),
                sharded_pool: None,
                slot_tuner: None,
                max_concurrent_sessions: 0,
            },
            registry,
        )
        .expect("worker"),
    )
}

/// Wait until at least `min_count` executions of `wf_name` reach `state`.
async fn wait_for_state(url: &str, wf_name: &str, state: &str, min_count: usize) -> Vec<Uuid> {
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let mut conn = AsyncPgConnection::establish(url).await.expect("connect");
            let rows: Vec<Uuid> = harvest_workflow_executions::table
                .filter(harvest_workflow_executions::dsl::workflow_name.eq(wf_name))
                .filter(harvest_workflow_executions::dsl::state.eq(state))
                .select(harvest_workflow_executions::dsl::id)
                .load(&mut conn)
                .await
                .unwrap_or_default();
            if rows.len() >= min_count {
                return rows;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("timed out waiting for execution state")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// A fired scheduled run's `WorkflowStarted.scheduled_time` equals its nominal slot
/// and is NOT `now()` (the execution-start wall-clock).
#[tokio::test]
async fn scheduled_run_has_correct_scheduled_time() {
    let (mut conn, url, _c) = setup_db().await;
    let wf_name = "sched_time_single";
    let pool = make_pool(&url);
    let registry = make_registry(wf_name);
    let dags = Arc::new(DagCatalog::default());

    let sched = WorkflowSchedule::new(wf_name, Schedule::Interval(Duration::from_secs(60)));
    register_workflow_schedules(&mut conn, &[sched])
        .await
        .expect("register schedules");

    let slot_secs_ago: i64 = 300;
    let before_arm = Utc::now();
    arm_slot(&mut conn, wf_name, slot_secs_ago).await;

    tick_once(
        pool.clone(),
        registry.clone(),
        dags.clone(),
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("tick");

    let worker = make_worker("w-single", registry.clone());
    let pool_clone = pool.clone();
    let worker_clone = worker.clone();
    tokio::spawn(async move { worker_clone.run(&pool_clone).await });

    let ids = wait_for_state(&url, wf_name, "COMPLETED", 1).await;
    let exec_id = ids[0];

    // Load event history and find the WorkflowStarted event.
    let mut check = AsyncPgConnection::establish(&url).await.unwrap();
    let history = store::load_history(&mut check, ExecutionId::from_uuid(exec_id))
        .await
        .expect("load history");

    let scheduled_time = match <[_]>::first(&history.events) {
        Some(autumn_harvest::event::WorkflowEvent::WorkflowStarted { scheduled_time, .. }) => {
            *scheduled_time
        }
        other => panic!("expected WorkflowStarted as first event, got: {other:?}"),
    };

    assert!(
        scheduled_time.is_some(),
        "scheduled run must have scheduled_time set"
    );

    let slot = scheduled_time.unwrap();
    // The slot should be close to `before_arm - slot_secs_ago` (within a few seconds margin).
    let expected_approx = before_arm - chrono::Duration::seconds(slot_secs_ago);
    let diff = (slot - expected_approx).num_seconds().abs();
    assert!(
        diff < 10,
        "scheduled_time should be close to the armed slot (diff={diff}s). slot={slot}, expected≈{expected_approx}"
    );

    // The slot must NOT be approximately now() (the execution start wall-clock).
    // It should differ by at least slot_secs_ago - 30s from now.
    let diff_from_now = (Utc::now() - slot).num_seconds();
    assert!(
        diff_from_now > (slot_secs_ago - 30),
        "scheduled_time must not be now(), got diff_from_now={diff_from_now}s"
    );

    // Also verify the workflow output carries the same slot.
    let output: Option<serde_json::Value> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::dsl::id.eq(exec_id))
        .select(harvest_workflow_executions::dsl::output)
        .first(&mut check)
        .await
        .expect("load output");
    let scheduled_time_out = output
        .as_ref()
        .and_then(|v| v["scheduled_time"].as_str())
        .expect("output must have scheduled_time string");
    // Round-trip: the ISO string from the output should parse back to the same instant.
    let parsed: chrono::DateTime<Utc> = scheduled_time_out.parse().expect("parse ISO string");
    assert_eq!(
        parsed, slot,
        "workflow output's scheduled_time must match the WorkflowStarted event field"
    );
}

/// A direct (non-scheduled) `start_or_load_workflow_execution` call with
/// `schedule_id/scheduled_for = None` produces a history with `scheduled_time = None`.
#[tokio::test]
async fn manual_start_has_no_scheduled_time() {
    let (mut conn, url, _c) = setup_db().await;
    let wf_name = "sched_time_manual";
    let pool = make_pool(&url);
    let registry = make_registry(wf_name);

    let exec_id = ExecutionId::new();
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: wf_name,
            workflow_id: "manual-test-wf",
            exec_id,
            input: json!(null),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            inherited_chain_deadline_at: None,
            concurrency_key: None,
            concurrency_limit: None,
            concurrency_on_conflict: Default::default(),
            priority: autumn_harvest::types::Priority::default(),
            max_workflow_input_bytes: 1_000_000,
            start_at: None,
            delay: None,
            max_workflow_start_delay: None,
            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,
            sla: None,
            schedule_id: None,
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: None,
            retry_of_exec_id: None,
            max_workflow_attempts_ceiling: None,
            origin: None,
            completion_callbacks: None,
            start_source: autumn_harvest::StartSource::Api,
            start_source_ref: None,
            started_by: None,
        },
        None,
    )
    .await
    .expect("start workflow");

    // Drain with a worker.
    let worker = make_worker("w-manual", registry.clone());
    let pool_clone = pool.clone();
    let worker_clone = worker.clone();
    tokio::spawn(async move { worker_clone.run(&pool_clone).await });

    wait_for_state(&url, wf_name, "COMPLETED", 1).await;

    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("load history");

    let scheduled_time = match <[_]>::first(&history.events) {
        Some(autumn_harvest::event::WorkflowEvent::WorkflowStarted { scheduled_time, .. }) => {
            *scheduled_time
        }
        other => panic!("expected WorkflowStarted, got: {other:?}"),
    };

    assert!(
        scheduled_time.is_none(),
        "manual start must have scheduled_time = None, got: {scheduled_time:?}"
    );
}

/// Success metric (issue #508): N distinct slots → N executions with N distinct
/// `scheduled_time` values, zero `now()` fallbacks.
#[tokio::test]
async fn n_slots_produce_n_distinct_scheduled_times() {
    const N: usize = 5;
    let (mut conn, url, _c) = setup_db().await;
    let wf_name = "sched_time_n_slots";
    let pool = make_pool(&url);
    let registry = make_registry(wf_name);
    let dags = Arc::new(DagCatalog::default());

    let sched = WorkflowSchedule::new(wf_name, Schedule::Interval(Duration::from_secs(60)))
        .with_max_active_runs(u32::try_from(N).unwrap());
    register_workflow_schedules(&mut conn, &[sched])
        .await
        .expect("register schedules");

    // Arm and tick N slots with strictly decreasing secs_ago (increasing slot time).
    for i in 0..N {
        let secs_ago = ((N - i) * 300) as i64; // 1500, 1200, 900, 600, 300
        arm_slot(&mut conn, wf_name, secs_ago).await;
        tick_once(
            pool.clone(),
            registry.clone(),
            dags.clone(),
            Arc::new(vec![]),
            SchedulerMonitor::offline(),
        )
        .await
        .expect("tick");
        // Small delay to ensure distinct DB timestamps.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Drain all N with a worker.
    let worker = make_worker("w-n-slots", registry.clone());
    let pool_clone = pool.clone();
    let worker_clone = worker.clone();
    tokio::spawn(async move { worker_clone.run(&pool_clone).await });

    wait_for_state(&url, wf_name, "COMPLETED", N).await;

    // Verify each completed execution has a distinct, non-now scheduled_time.
    let mut check = AsyncPgConnection::establish(&url).await.unwrap();
    let exec_ids: Vec<Uuid> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::dsl::workflow_name.eq(wf_name))
        .filter(harvest_workflow_executions::dsl::state.eq("COMPLETED"))
        .select(harvest_workflow_executions::dsl::id)
        .load(&mut check)
        .await
        .expect("load exec ids");

    assert_eq!(exec_ids.len(), N, "expected {N} COMPLETED executions");

    let mut seen_slots = std::collections::HashSet::new();
    for exec_id in &exec_ids {
        let history = store::load_history(&mut check, ExecutionId::from_uuid(*exec_id))
            .await
            .expect("load history");

        let scheduled_time = match <[_]>::first(&history.events) {
            Some(autumn_harvest::event::WorkflowEvent::WorkflowStarted {
                scheduled_time, ..
            }) => *scheduled_time,
            other => panic!("expected WorkflowStarted, got: {other:?}"),
        };

        let slot =
            scheduled_time.expect("scheduled run must have scheduled_time (no now() fallback)");

        // Must not be "now" — slot should be at least 240s in the past.
        let diff_from_now = (Utc::now() - slot).num_seconds();
        assert!(
            diff_from_now > 240,
            "scheduled_time must not be now(), got diff_from_now={diff_from_now}s for exec {exec_id}"
        );

        seen_slots.insert(slot.timestamp());
    }

    assert_eq!(
        seen_slots.len(),
        N,
        "expected {N} distinct scheduled_time values, got: {seen_slots:?}"
    );
}

/// Replay of a scheduled run's history reports `ReplaySucceeded` — no non-determinism.
#[tokio::test]
async fn scheduled_run_replays_deterministically() {
    let (mut conn, url, _c) = setup_db().await;
    let wf_name = "sched_time_replay";
    let pool = make_pool(&url);
    let registry = make_registry(wf_name);
    let dags = Arc::new(DagCatalog::default());

    let sched = WorkflowSchedule::new(wf_name, Schedule::Interval(Duration::from_secs(60)));
    register_workflow_schedules(&mut conn, &[sched])
        .await
        .expect("register schedules");

    arm_slot(&mut conn, wf_name, 300).await;
    tick_once(
        pool.clone(),
        registry.clone(),
        dags.clone(),
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("tick");

    let worker = make_worker("w-replay", registry.clone());
    let pool_clone = pool.clone();
    let worker_clone = worker.clone();
    tokio::spawn(async move { worker_clone.run(&pool_clone).await });

    let ids = wait_for_state(&url, wf_name, "COMPLETED", 1).await;
    let exec_id = ExecutionId::from_uuid(ids[0]);

    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("load history");

    let report = WorkflowReplayer::new()
        .register_fn(wf_name, scheduled_time_recorder)
        .replay_from_events(history.events)
        .await;

    assert!(
        matches!(
            report.status,
            autumn_harvest::testing::ReplayStatus::ReplaySucceeded
        ),
        "scheduled history must replay deterministically:\n{report}"
    );
}
