#![cfg(feature = "db")]
//! Auto-heartbeat guard behavioural tests — issue #682.
//!
//! Three complementary proofs:
//!
//! * TEST A (`auto_heartbeat_prevents_spurious_heartbeat_reclaim`): the value of
//!   the feature. A long-running activity that never manually heartbeats but
//!   installs `ctx.start_auto_heartbeat_default()` runs for well over 3× its
//!   `heartbeat_timeout` while a background timeout scanner sweeps repeatedly.
//!   The activity is **never** reclaimed via `TimeoutReason::Heartbeat` and the
//!   workflow completes — the auto-heartbeat's liveness pings keep
//!   `last_heartbeat_at` fresh (AC#8, success metric).
//!
//! * TEST B (`fresh_heartbeat_does_not_defeat_start_to_close`): the wedge
//!   safety-net proof, deterministic and pure (no wedged future needed). Seeds
//!   `harvest_task_queue` rows directly with a **fresh** `last_heartbeat_at`
//!   (what auto-heartbeat produces) and inspects the pure `find_timed_out_tasks`
//!   scanner. A fresh heartbeat protects from the *heartbeat* timeout but does
//!   NOT protect a wedged activity from the independent `start_to_close`
//!   ceiling. Also asserts the converse: a stale heartbeat IS reclaimed as
//!   `Heartbeat`, so the fresh-heartbeat protection is real, not vacuous.
//!
//! * TEST C (`auto_heartbeat_activity_still_reclaimed_by_start_to_close`): the
//!   end-to-end complement to TEST B. A real activity installs
//!   `ctx.start_auto_heartbeat_default()` and then wedges by sleeping past a
//!   tight `start_to_close`; driven through the worker with a live scanner, it
//!   IS reclaimed as `StartToClose` (never `Heartbeat`) and the workflow fails —
//!   proving auto-heartbeat defeats only the heartbeat timeout, never the hard
//!   `start_to_close` ceiling.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly; otherwise a fresh testcontainers Postgres is booted
//! with the full migration bundle.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::{NewWorkflowExecution, TaskQueueItem, WorkflowExecution};
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::telemetry::{MetricsRecorder, NoOpMetrics, TelemetryConfig};
use autumn_harvest::timeout::{self, TimeoutReason, find_timed_out_tasks};
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{WorkflowContext, store};

use chrono::Utc;
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// DB setup — env-var-redirectable, otherwise testcontainers.
// ---------------------------------------------------------------------------

fn init_sql() -> Vec<u8> {
    autumn_harvest::test_init_sql().as_bytes().to_vec()
}

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool build failed")
}

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("failed to connect to Postgres")
}

// ---------------------------------------------------------------------------
// Worker + registry construction.
// ---------------------------------------------------------------------------

fn build_registry(
    workflows: Vec<WorkflowInfo>,
    activities: Vec<ActivityInfo>,
) -> Arc<HandlerRegistry> {
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(Arc::new(NoOpMetrics) as Arc<dyn MetricsRecorder>)
            .build(),
    );
    Arc::new(HandlerRegistry::with_state_and_telemetry(
        workflows,
        activities,
        autumn_harvest::context::empty_shared_state(),
        telemetry,
    ))
}

fn build_worker(worker_id: &str, registry: Arc<HandlerRegistry>) -> Arc<Worker> {
    Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                codec_rotation_batch_size: 0,
                dr_fencing: false,
                worker_id: worker_id.to_string(),
                queues: vec!["default".to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 2,
                max_concurrent_activities: 2,
                poll_interval: Duration::from_millis(25),
                shutdown_timeout: Duration::from_secs(1),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 1000,
                priority_aging_secs: None,
                unknown_target_grace_window: Duration::from_secs(5),
                poison_pill_threshold: 3,
                capability_miss_max_redeliveries: 5,
                workflow_task_timeout: Duration::from_secs(10),
                workflow_panic_max_attempts: 3,
                labels: HashMap::new(),
                queue_weights: HashMap::new(),
                max_workflow_pause_duration: Duration::from_secs(24 * 3600),
                max_workflow_history_events: None,
                shard_notification_database_urls: Vec::new(),
                sharded_pool: None,
                slot_tuner: None,
                max_concurrent_sessions: 0,
            },
            registry,
        )
        .expect("worker should build"),
    )
}

// ---------------------------------------------------------------------------
// Seeding + read helpers.
// ---------------------------------------------------------------------------

async fn seed_workflow(
    conn: &mut AsyncPgConnection,
    workflow_name: &'static str,
    input: serde_json::Value,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let row = NewWorkflowExecution {
        quota_key: None,
        id: exec_id.as_uuid(),
        workflow_name,
        workflow_id: &format!("wf-{}", exec_id.as_uuid()),
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: input.clone(),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        deadline_at: None,
        chain_execution_timeout: None,
        chain_deadline_at: None,
        memo: None,
        search_attrs: None,
        assigned_build_id: None,
        parent_close_policy: None,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        sla: None,
        sla_deadline_at: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        origin: None,
        completion_callbacks: None,
        continued_from_exec_id: None,
        first_exec_id: None,
        start_source: None,
        start_source_ref: None,
        started_by: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("insert workflow execution");

    store::append_events(
        conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted");

    let mut params = EnqueueParams::new("default", TaskType::Workflow, input);
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    queue::enqueue(conn, &params)
        .await
        .expect("enqueue workflow task");

    exec_id
}

async fn load_execution(url: &str, exec_id: ExecutionId) -> WorkflowExecution {
    let mut conn = connect(url).await;
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("reload workflow execution")
}

async fn load_history(url: &str, exec_id: ExecutionId) -> Vec<WorkflowEvent> {
    let mut conn = connect(url).await;
    store::load_history(&mut conn, exec_id)
        .await
        .expect("load_history")
        .events
}

async fn wait_for_state(
    url: &str,
    exec_id: ExecutionId,
    want: &str,
    timeout: Duration,
) -> WorkflowExecution {
    tokio::time::timeout(timeout, async {
        loop {
            let e = load_execution(url, exec_id).await;
            if e.state == want {
                break e;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("execution did not reach state {want} within {timeout:?}"))
}

// ---------------------------------------------------------------------------
// Handlers.
// ---------------------------------------------------------------------------

type BoxFut<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>>;

/// Workflow that calls a single long-running activity and returns its output.
fn workflow_calls_long_activity(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_activity_raw("long_auto_hb_activity", input, "default")
            .await
            .map_err(|e| e.to_string())
    })
}

/// Long-running activity that installs an auto-heartbeat guard and then runs
/// (checking cancellation) for well over 3× its `heartbeat_timeout` WITHOUT
/// ever manually heartbeating. Only the auto-heartbeat keeps it alive.
fn long_auto_hb_activity(
    ctx: &autumn_harvest::ActivityContext,
    _input: serde_json::Value,
) -> BoxFut<'_> {
    Box::pin(async move {
        let _guard = ctx
            .start_auto_heartbeat_default()
            .map_err(|e| e.to_string())?;
        // heartbeat_timeout is 6s (see ActivityInfo); auto interval is 2s, so
        // the freshness margin is ~3s of leeway against the 6s window even on a
        // loaded CI runner. Run for ~19s (> 3× the window).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(19);
        while tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(150)).await;
            ctx.check_cancellation().await.map_err(|e| e.to_string())?;
        }
        Ok(serde_json::json!({"done": true}))
    })
}

/// Workflow that calls the wedging auto-heartbeat activity (TEST C).
fn workflow_calls_wedge_activity(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_activity_raw("wedge_auto_hb_activity", input, "default")
            .await
            .map_err(|e| e.to_string())
    })
}

/// Activity that installs an auto-heartbeat guard (keeping `last_heartbeat_at`
/// fresh) and then *wedges* by sleeping well past its tight `start_to_close`
/// ceiling without cooperating. Proves the full composition: auto-heartbeat
/// defeats only the heartbeat timeout, NEVER the independent `start_to_close`
/// ceiling — the wedge is still reclaimed.
fn wedge_auto_hb_activity(
    ctx: &autumn_harvest::ActivityContext,
    _input: serde_json::Value,
) -> BoxFut<'_> {
    Box::pin(async move {
        let _guard = ctx
            .start_auto_heartbeat_default()
            .map_err(|e| e.to_string())?;
        // heartbeat_timeout is 3s (auto interval 1s keeps last_heartbeat_at
        // fresh); start_to_close is 2s. Sleep ~8s so the scanner reclaims via
        // start_to_close while the activity is still "running" and pinging.
        tokio::time::sleep(Duration::from_secs(8)).await;
        Ok(serde_json::json!({"done": true}))
    })
}

fn workflow_info(
    name: &'static str,
    handler: autumn_harvest::info::WorkflowHandlerFn,
) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "auto_heartbeat_tests",
        handler,
        execution_timeout: None,
        chain_execution_timeout: None,
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
    }
}

fn activity_info(
    name: &'static str,
    handler: autumn_harvest::info::ActivityHandlerFn,
    heartbeat_timeout: Option<Duration>,
    start_to_close: Option<Duration>,
) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "auto_heartbeat_tests",
        default_retry_policy: None,
        default_start_to_close: start_to_close,
        default_heartbeat_timeout: heartbeat_timeout,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some("default"),
        max_concurrent: None,
        concurrency_key: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        requires: None,
        handler,
    }
}

// ---------------------------------------------------------------------------
// TEST A — auto-heartbeat prevents spurious heartbeat reclaim (AC#8).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_heartbeat_prevents_spurious_heartbeat_reclaim() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;

    let exec_id = seed_workflow(&mut conn, "long_wf", serde_json::json!({"n": 1})).await;

    let registry = build_registry(
        vec![workflow_info("long_wf", workflow_calls_long_activity)],
        // heartbeat_timeout = 6s, NO start_to_close (so only heartbeat
        // enforcement is active). Default auto interval = 6s / 3 = 2s, giving a
        // ~3s freshness margin against the 6s window (widened for CI headroom).
        vec![activity_info(
            "long_auto_hb_activity",
            long_auto_hb_activity,
            Some(Duration::from_secs(6)),
            None,
        )],
    );
    let worker = build_worker("worker-auto-hb", Arc::clone(&registry));
    let pool = build_pool(&url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let run_handle = tokio::spawn(async move { runner.run(&pool_for_run).await });

    // Background timeout scanner: sweep every 500ms for the whole test. Without
    // auto-heartbeat, this would reclaim the activity (Heartbeat) once it has
    // been RUNNING for > heartbeat_timeout.
    let scanner_pool = pool.clone();
    let scanner_stop = Arc::new(tokio::sync::Notify::new());
    let scanner_stop_for_task = Arc::clone(&scanner_stop);
    let scanner_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                () = scanner_stop_for_task.notified() => break,
                () = tokio::time::sleep(Duration::from_millis(500)) => {
                    let mut c = scanner_pool.get().await.expect("scanner conn");
                    let _ = timeout::enforce_timeouts_once(
                        &mut c,
                        &NoOpMetrics,
                        Duration::from_secs(60),
                        &None,
                        &[ShardId::new(0)],
                        None,
                        None,
                        60,
                        &autumn_harvest::payload_codec::PayloadCodecs::default(),
                        0,
                    )
                    .await;
                }
            }
        }
    });

    // The activity runs ~19s; give a generous ceiling for CI.
    let execution = wait_for_state(&url, exec_id, "COMPLETED", Duration::from_secs(60)).await;

    scanner_stop.notify_one();
    let _ = scanner_handle.await;
    worker.shutdown();
    let _ = run_handle.await;

    assert_eq!(execution.state, "COMPLETED");

    // No heartbeat ActivityTimedOut event was ever appended — the
    // auto-heartbeat kept `last_heartbeat_at` fresh throughout.
    let history = load_history(&url, exec_id).await;
    assert!(
        !history.iter().any(|e| matches!(
            e,
            WorkflowEvent::ActivityTimedOut {
                timeout_type: autumn_harvest::error::TimeoutType::Heartbeat,
                ..
            }
        )),
        "auto-heartbeat must prevent any Heartbeat reclaim; history={history:?}"
    );
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. })),
        "the long-running activity must complete normally; history={history:?}"
    );
}

// ---------------------------------------------------------------------------
// TEST B — a fresh heartbeat does not defeat start_to_close (wedge safety net).
// ---------------------------------------------------------------------------

/// Enqueue an activity task and drive it into `RUNNING` with explicit
/// `started_at` / `last_heartbeat_at`, mirroring the state auto-heartbeat leaves
/// behind (or, for the stale case, does not).
async fn seed_running_activity(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    heartbeat_timeout: Option<Duration>,
    start_to_close: Option<Duration>,
    started_secs_ago: i64,
    fresh_heartbeat: bool,
) -> Uuid {
    let mut params = EnqueueParams::new("default", TaskType::Activity, serde_json::json!(null));
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.activity_name = Some("wedge_activity".to_string());
    params.heartbeat_timeout =
        heartbeat_timeout.map(|d| chrono::Duration::from_std(d).expect("hb interval"));
    params.start_to_close =
        start_to_close.map(|d| chrono::Duration::from_std(d).expect("s2c interval"));
    let task_id = queue::enqueue(conn, &params)
        .await
        .expect("enqueue activity");

    let hb_sql = if fresh_heartbeat { "NOW()" } else { "NULL" };
    diesel::sql_query(format!(
        "UPDATE harvest_task_queue \
         SET state = 'RUNNING', worker_id = 'w-1', \
             started_at = NOW() - ($2 * INTERVAL '1 second'), \
             last_heartbeat_at = {hb_sql} \
         WHERE id = $1"
    ))
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<diesel::sql_types::BigInt, _>(started_secs_ago)
    .execute(conn)
    .await
    .expect("drive task into RUNNING");

    task_id
}

fn reason_for(tasks: &[(TaskQueueItem, TimeoutReason)], id: Uuid) -> Option<TimeoutReason> {
    tasks
        .iter()
        .find(|(t, _)| t.id == id)
        .map(|(_, r)| r.clone())
}

#[tokio::test]
async fn fresh_heartbeat_does_not_defeat_start_to_close() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;

    let exec_id = seed_workflow(&mut conn, "wedge_wf", serde_json::json!(null)).await;

    // Row 1 — FRESH heartbeat + start_to_close (started 10s ago, s2c 1s):
    // a fresh heartbeat (what auto-heartbeat produces) must NOT shield a wedged
    // activity from the independent start_to_close ceiling.
    let fresh_with_s2c = seed_running_activity(
        &mut conn,
        exec_id,
        Some(Duration::from_secs(1)), // heartbeat_timeout 1s
        Some(Duration::from_secs(1)), // start_to_close 1s
        10,                           // started 10s ago
        true,                         // fresh last_heartbeat_at = NOW()
    )
    .await;

    // Row 2 — FRESH heartbeat, short heartbeat_timeout, NO start_to_close:
    // the fresh heartbeat DOES protect from the heartbeat timeout — not reclaimed.
    let fresh_no_s2c = seed_running_activity(
        &mut conn,
        exec_id,
        Some(Duration::from_secs(1)),
        None,
        10,
        true,
    )
    .await;

    // Row 3 — STALE heartbeat (NULL), short heartbeat_timeout, NO start_to_close:
    // proves the protection is real — without a fresh heartbeat it IS reclaimed
    // as Heartbeat.
    let stale_no_s2c = seed_running_activity(
        &mut conn,
        exec_id,
        Some(Duration::from_secs(1)),
        None,
        10,
        false,
    )
    .await;

    let timed_out = find_timed_out_tasks(&mut conn)
        .await
        .expect("scanner should succeed");

    // Row 1: reclaimed, and by StartToClose (NOT Heartbeat).
    assert_eq!(
        reason_for(&timed_out, fresh_with_s2c),
        Some(TimeoutReason::StartToClose),
        "a fresh heartbeat must not shield a wedged activity from start_to_close"
    );

    // Row 2: fresh heartbeat + no start_to_close → not reclaimed at all.
    assert_eq!(
        reason_for(&timed_out, fresh_no_s2c),
        None,
        "a fresh heartbeat must protect from the heartbeat timeout"
    );

    // Row 3: stale heartbeat → reclaimed as Heartbeat (protection is not vacuous).
    assert_eq!(
        reason_for(&timed_out, stale_no_s2c),
        Some(TimeoutReason::Heartbeat),
        "a stale heartbeat with an elapsed heartbeat_timeout must be reclaimed"
    );
}

// ---------------------------------------------------------------------------
// TEST C — end-to-end: an auto-heartbeat activity is still reclaimed by
// start_to_close (the airtight full-composition wedge proof, AC).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_heartbeat_activity_still_reclaimed_by_start_to_close() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;

    let exec_id = seed_workflow(&mut conn, "wedge_wf_e2e", serde_json::json!({"n": 1})).await;

    let registry = build_registry(
        vec![workflow_info("wedge_wf_e2e", workflow_calls_wedge_activity)],
        // heartbeat_timeout = 3s (auto interval 1s keeps last_heartbeat_at
        // fresh), start_to_close = 2s (the tight ceiling the wedge cannot
        // escape). Default auto interval = 3s / 3 = 1s.
        vec![activity_info(
            "wedge_auto_hb_activity",
            wedge_auto_hb_activity,
            Some(Duration::from_secs(3)),
            Some(Duration::from_secs(2)),
        )],
    );
    let worker = build_worker("worker-wedge-hb", Arc::clone(&registry));
    let pool = build_pool(&url);
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let run_handle = tokio::spawn(async move { runner.run(&pool_for_run).await });

    // Background timeout scanner sweeping every 250ms — enforces start_to_close
    // even though the auto-heartbeat keeps `last_heartbeat_at` fresh.
    let scanner_pool = pool.clone();
    let scanner_stop = Arc::new(tokio::sync::Notify::new());
    let scanner_stop_for_task = Arc::clone(&scanner_stop);
    let scanner_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                () = scanner_stop_for_task.notified() => break,
                () = tokio::time::sleep(Duration::from_millis(250)) => {
                    let mut c = scanner_pool.get().await.expect("scanner conn");
                    let _ = timeout::enforce_timeouts_once(
                        &mut c,
                        &NoOpMetrics,
                        Duration::from_secs(60),
                        &None,
                        &[ShardId::new(0)],
                        None,
                        None,
                        60,
                        &autumn_harvest::payload_codec::PayloadCodecs::default(),
                        0,
                    )
                    .await;
                }
            }
        }
    });

    // The wedge is reclaimed by start_to_close (~2s) and the workflow fails; a
    // generous ceiling for CI.
    let execution = wait_for_state(&url, exec_id, "FAILED", Duration::from_secs(30)).await;

    scanner_stop.notify_one();
    let _ = scanner_handle.await;
    worker.shutdown();
    let _ = run_handle.await;

    assert_eq!(execution.state, "FAILED");

    // The wedge was reclaimed by StartToClose, NOT Heartbeat: the auto-heartbeat
    // kept `last_heartbeat_at` fresh, so the ONLY thing that could reclaim it is
    // the independent start_to_close ceiling. This is the airtight
    // full-composition proof requested by the coordinator.
    let history = load_history(&url, exec_id).await;
    assert!(
        history.iter().any(|e| matches!(
            e,
            WorkflowEvent::ActivityTimedOut {
                timeout_type: autumn_harvest::error::TimeoutType::StartToClose,
                ..
            }
        )),
        "the wedged auto-heartbeat activity must be reclaimed by start_to_close; history={history:?}"
    );
    assert!(
        !history.iter().any(|e| matches!(
            e,
            WorkflowEvent::ActivityTimedOut {
                timeout_type: autumn_harvest::error::TimeoutType::Heartbeat,
                ..
            }
        )),
        "auto-heartbeat must keep last_heartbeat_at fresh — no Heartbeat reclaim; history={history:?}"
    );
}
