#![cfg(feature = "db")]
//! Activity timeout retries and the open circuit (issue #1809).
//!
//! `docs/adr/0004-activity-timeout-retry-and-open-circuit.md` records the
//! decision. These tests hold the code to it:
//!
//! - A start-to-close or heartbeat timeout retries per the retry policy.
//!   Only the last attempt appends `ActivityTimedOut`.
//! - A timeout is not retried past `schedule_to_close`.
//! - A timeout feeds the circuit breaker only when the handler started.
//! - An open breaker defers work by default and does not fail it.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to use a migrated Postgres. Otherwise the
//! suite starts a testcontainers Postgres 16.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_harvest::circuit_breaker::CircuitBreakerRegistry;
use autumn_harvest::error::TimeoutType;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::{NewWorkflowExecution, TaskQueueItem, WorkflowExecution};
use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::policy::CircuitBreakerPolicy;
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::schema::{harvest_task_queue, harvest_workflow_executions};
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::timeout;
use autumn_harvest::types::{ActivityExecId, ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{RetryPolicy, WorkflowContext, store};

use chrono::Utc;
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// DB setup
// ---------------------------------------------------------------------------

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let mut conn = connect(&url).await;
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("migration");
    (url, Some(container))
}

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect")
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool build")
}

fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", Uuid::new_v4().simple())
}

// ---------------------------------------------------------------------------
// Seeding
// ---------------------------------------------------------------------------

/// Insert a running execution on `queue` with `WorkflowStarted` in history.
async fn seed_execution(
    conn: &mut AsyncPgConnection,
    queue: &str,
    input: serde_json::Value,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let row = NewWorkflowExecution {
        quota_key: None,
        id: exec_id.as_uuid(),
        workflow_name: WF_NAME,
        workflow_id: &format!("wf-{}", exec_id.as_uuid()),
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: input.clone().into(),
        parent_id: None,
        queue_name: queue,
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
            input,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted");
    exec_id
}

/// The timeouts that one seeded activity task carries.
#[derive(Clone, Copy, Default)]
struct Timeouts {
    start_to_close: Option<Duration>,
    heartbeat: Option<Duration>,
    schedule_to_close: Option<Duration>,
}

fn chrono_duration(duration: Duration) -> chrono::Duration {
    chrono::Duration::from_std(duration).expect("duration in range")
}

/// Schedule `activity` in the history of a new execution and enqueue its
/// task on `queue`. The task allows `max_attempts` attempts.
async fn seed_activity(
    conn: &mut AsyncPgConnection,
    queue: &str,
    activity: &str,
    max_attempts: u32,
    timeouts: Timeouts,
) -> (ExecutionId, Uuid) {
    let exec_id = seed_execution(conn, queue, serde_json::json!({})).await;
    let activity_id = ActivityExecId::new();
    store::append_events(
        conn,
        exec_id,
        &[WorkflowEvent::ActivityScheduled {
            activity_id,
            name: activity.to_string(),
            input: serde_json::json!({}),
            queue: queue.to_string(),
        }],
        1,
    )
    .await
    .expect("append ActivityScheduled");

    let policy = RetryPolicy::fixed(max_attempts, Duration::from_millis(1));
    let mut params = EnqueueParams::new(queue, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.activity_name = Some(activity.to_string());
    params.activity_id = Some(activity_id.as_uuid());
    params.max_attempts = i32::try_from(max_attempts).expect("small max_attempts");
    params.retry_policy = Some(serde_json::to_value(&policy).expect("policy json"));
    params.start_to_close = timeouts.start_to_close.map(chrono_duration);
    params.heartbeat_timeout = timeouts.heartbeat.map(chrono_duration);
    params.schedule_to_close_at = timeouts
        .schedule_to_close
        .map(|deadline| Utc::now() + chrono_duration(deadline));
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    let task_id = queue::enqueue(conn, &params)
        .await
        .expect("enqueue activity");
    (exec_id, task_id)
}

/// Claim the only task on `queue`, as a worker does.
async fn claim(conn: &mut AsyncPgConnection, queue: &str, worker_id: &str) -> TaskQueueItem {
    // A retry requeue sets `scheduled_at` a moment ahead of the clock.
    for _ in 0..100 {
        if let Some(task) =
            queue::claim_task(conn, &[queue.to_string()], worker_id, "", None, &[], &[])
                .await
                .expect("claim")
        {
            return task;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("no claimable task on {queue}");
}

/// Move the claim and the last heartbeat 10 minutes into the past.
async fn age_claim(conn: &mut AsyncPgConnection, task_id: Uuid) {
    diesel::sql_query(
        "UPDATE harvest_task_queue \
         SET started_at = NOW() - INTERVAL '10 minutes', \
             last_heartbeat_at = CASE WHEN last_heartbeat_at IS NULL THEN NULL \
                 ELSE NOW() - INTERVAL '10 minutes' END \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(conn)
    .await
    .expect("age the claim");
}

async fn enforce(conn: &mut AsyncPgConnection, breakers: Option<&CircuitBreakerRegistry>) {
    timeout::enforce_timeouts_once(
        conn,
        &NoOpMetrics,
        Duration::from_secs(60),
        &None,
        &[],
        breakers,
        None,
        60,
        &PayloadCodecs::default(),
        0,
    )
    .await
    .expect("enforce_timeouts_once");
}

async fn task_row(conn: &mut AsyncPgConnection, task_id: Uuid) -> TaskQueueItem {
    harvest_task_queue::table
        .find(task_id)
        .select(TaskQueueItem::as_select())
        .first(conn)
        .await
        .expect("load task row")
}

async fn history(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> Vec<WorkflowEvent> {
    store::load_history(conn, exec_id)
        .await
        .expect("load history")
        .events
}

fn timed_out(history: &[WorkflowEvent]) -> Vec<TimeoutType> {
    history
        .iter()
        .filter_map(|event| match event {
            WorkflowEvent::ActivityTimedOut { timeout_type, .. } => Some(timeout_type.clone()),
            _ => None,
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Timeout retries
// ---------------------------------------------------------------------------

/// ADR 0004 §1: a start-to-close timeout retries per the retry policy. With
/// `max_attempts = 3`, attempts 1 and 2 go back to `PENDING` and append no
/// event. Attempt 3 appends `ActivityTimedOut { StartToClose }` and fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_to_close_timeout_retries_until_max_attempts() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique("t1809-s2c");
    let timeouts = Timeouts {
        start_to_close: Some(Duration::from_secs(1)),
        ..Timeouts::default()
    };
    let (exec_id, task_id) = seed_activity(&mut conn, &queue, "t1809_s2c", 3, timeouts).await;

    for attempt in 1..=3 {
        let claimed = claim(&mut conn, &queue, "w-s2c").await;
        assert_eq!(claimed.id, task_id);
        assert_eq!(claimed.attempt, attempt);
        age_claim(&mut conn, task_id).await;
        enforce(&mut conn, None).await;

        let row = task_row(&mut conn, task_id).await;
        let events = history(&mut conn, exec_id).await;
        if attempt < 3 {
            assert_eq!(row.state, "PENDING", "attempt {attempt} retries");
            assert_eq!(row.attempt, attempt, "a retry keeps the attempt number");
            assert!(row.worker_id.is_none(), "a retry releases the claim");
            assert!(
                row.error.as_deref().is_some_and(|e| e.contains("StartToClose")),
                "the retry records the timeout as the previous failure: {:?}",
                row.error
            );
            assert!(
                timed_out(&events).is_empty(),
                "a retried attempt appends no ActivityTimedOut"
            );
        } else {
            assert_eq!(row.state, "FAILED", "the last attempt fails");
            assert_eq!(timed_out(&events), vec![TimeoutType::StartToClose]);
        }
    }
}

/// ADR 0004 §1: a heartbeat timeout retries like a start-to-close timeout.
/// The retry keeps the heartbeat details for the next attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn heartbeat_timeout_retries_and_keeps_heartbeat_details() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique("t1809-hb");
    let timeouts = Timeouts {
        heartbeat: Some(Duration::from_secs(1)),
        ..Timeouts::default()
    };
    let (exec_id, task_id) = seed_activity(&mut conn, &queue, "t1809_hb", 2, timeouts).await;

    claim(&mut conn, &queue, "w-hb").await;
    diesel::sql_query(
        "UPDATE harvest_task_queue \
         SET heartbeat_details = '{\"cursor\": 7}'::jsonb, last_heartbeat_at = NOW() \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(&mut conn)
    .await
    .expect("record a heartbeat");
    age_claim(&mut conn, task_id).await;
    enforce(&mut conn, None).await;

    let row = task_row(&mut conn, task_id).await;
    assert_eq!(row.state, "PENDING", "attempt 1 of 2 retries");
    assert_eq!(
        row.heartbeat_details,
        Some(serde_json::json!({ "cursor": 7 })),
        "the retry keeps the heartbeat checkpoint"
    );
    assert!(timed_out(&history(&mut conn, exec_id).await).is_empty());

    claim(&mut conn, &queue, "w-hb").await;
    age_claim(&mut conn, task_id).await;
    enforce(&mut conn, None).await;

    let row = task_row(&mut conn, task_id).await;
    assert_eq!(row.state, "FAILED");
    assert_eq!(
        timed_out(&history(&mut conn, exec_id).await),
        vec![TimeoutType::Heartbeat]
    );
}

/// ADR 0004 §1: no retry starts after `schedule_to_close`. The task fails
/// with its own timeout type.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_retry_stops_at_the_schedule_to_close_deadline() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique("t1809-deadline");
    let timeouts = Timeouts {
        start_to_close: Some(Duration::from_secs(1)),
        schedule_to_close: Some(Duration::from_secs(3600)),
        ..Timeouts::default()
    };
    let (exec_id, task_id) =
        seed_activity(&mut conn, &queue, "t1809_deadline", 3, timeouts).await;

    claim(&mut conn, &queue, "w-deadline").await;
    age_claim(&mut conn, task_id).await;
    // The next attempt cannot start before this deadline passes.
    diesel::sql_query(
        "UPDATE harvest_task_queue SET schedule_to_close_at = clock_timestamp() + INTERVAL '50 milliseconds' \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(&mut conn)
    .await
    .expect("move the deadline close");
    diesel::sql_query(
        "UPDATE harvest_task_queue SET retry_policy = $2 WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<diesel::sql_types::Jsonb, _>(
        serde_json::to_value(RetryPolicy::fixed(3, Duration::from_secs(60))).expect("json"),
    )
    .execute(&mut conn)
    .await
    .expect("use a long backoff");
    enforce(&mut conn, None).await;

    let row = task_row(&mut conn, task_id).await;
    assert_eq!(row.state, "FAILED", "no retry past the deadline");
    assert_eq!(
        timed_out(&history(&mut conn, exec_id).await),
        vec![TimeoutType::StartToClose]
    );
}

/// ADR 0004 §1: schedule-to-start is not retried.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schedule_to_start_timeout_stays_terminal() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique("t1809-s2s");
    let (exec_id, task_id) =
        seed_activity(&mut conn, &queue, "t1809_s2s", 3, Timeouts::default()).await;
    diesel::sql_query(
        "UPDATE harvest_task_queue \
         SET schedule_to_start = INTERVAL '1 second', scheduled_at = NOW() - INTERVAL '10 minutes' \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(&mut conn)
    .await
    .expect("expire schedule-to-start");
    enforce(&mut conn, None).await;

    let row = task_row(&mut conn, task_id).await;
    assert_eq!(row.state, "FAILED");
    assert_eq!(
        timed_out(&history(&mut conn, exec_id).await),
        vec![TimeoutType::ScheduleToStart]
    );
}

// ---------------------------------------------------------------------------
// Breaker feed
// ---------------------------------------------------------------------------

/// A breaker that trips on the first counted failure.
fn trip_on_first(activity: &str) -> CircuitBreakerRegistry {
    let policy = CircuitBreakerPolicy::new(1, Duration::from_secs(60), Duration::from_secs(60));
    CircuitBreakerRegistry::new(HashMap::from([(activity.to_string(), policy)]))
}

fn breaker_state(breakers: &CircuitBreakerRegistry, activity: &str) -> (&'static str, u32) {
    let snapshot = breakers
        .snapshot(activity, Instant::now())
        .expect("activity has a policy");
    (snapshot.state, snapshot.rolling_failure_count)
}

/// ADR 0004 §2: a timeout of a claimed task whose handler never started
/// does not change breaker state.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unstarted_timeout_leaves_the_breaker_unchanged() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique("t1809-unstarted");
    let activity = "t1809_unstarted";
    let timeouts = Timeouts {
        start_to_close: Some(Duration::from_secs(1)),
        ..Timeouts::default()
    };
    let (_exec_id, task_id) = seed_activity(&mut conn, &queue, activity, 1, timeouts).await;
    let breakers = trip_on_first(activity);

    // Claimed, so RUNNING, but ActivityStarted was never appended.
    claim(&mut conn, &queue, "w-unstarted").await;
    age_claim(&mut conn, task_id).await;
    enforce(&mut conn, Some(&breakers)).await;

    assert_eq!(task_row(&mut conn, task_id).await.state, "FAILED");
    assert_eq!(
        breaker_state(&breakers, activity),
        ("closed", 0),
        "a handler that never started says nothing about the downstream"
    );
}

/// ADR 0004 §2: a timeout of an attempt whose handler started still counts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn started_timeout_feeds_the_breaker() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique("t1809-started");
    let activity = "t1809_started";
    let timeouts = Timeouts {
        start_to_close: Some(Duration::from_secs(1)),
        ..Timeouts::default()
    };
    let (exec_id, task_id) = seed_activity(&mut conn, &queue, activity, 1, timeouts).await;
    let breakers = trip_on_first(activity);

    let claimed = claim(&mut conn, &queue, "w-started").await;
    let started = autumn_harvest::worker::append_activity_started_for_test(
        &mut conn,
        &claimed,
        exec_id,
        activity,
        "w-started",
        &PayloadCodecs::default(),
    )
    .await
    .expect("start the attempt");
    assert!(started.is_some(), "the attempt starts");
    age_claim(&mut conn, task_id).await;
    enforce(&mut conn, Some(&breakers)).await;

    assert_eq!(breaker_state(&breakers, activity).0, "open");
}

/// ADR 0004 §2: a retried claim is a new attempt. A start of an earlier
/// attempt does not mark the new claim as started.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn earlier_start_does_not_mark_a_later_claim_started() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique("t1809-later");
    let activity = "t1809_later";
    let timeouts = Timeouts {
        start_to_close: Some(Duration::from_secs(1)),
        ..Timeouts::default()
    };
    let (exec_id, task_id) = seed_activity(&mut conn, &queue, activity, 2, timeouts).await;

    // Attempt 1 starts and times out. Without a breaker, it only retries.
    let first = claim(&mut conn, &queue, "w-later").await;
    autumn_harvest::worker::append_activity_started_for_test(
        &mut conn,
        &first,
        exec_id,
        activity,
        "w-later",
        &PayloadCodecs::default(),
    )
    .await
    .expect("start attempt 1");
    age_claim(&mut conn, task_id).await;
    enforce(&mut conn, None).await;
    assert_eq!(task_row(&mut conn, task_id).await.state, "PENDING");

    // Attempt 2 is claimed by the same worker but never starts.
    let breakers = trip_on_first(activity);
    claim(&mut conn, &queue, "w-later").await;
    age_claim(&mut conn, task_id).await;
    enforce(&mut conn, Some(&breakers)).await;

    assert_eq!(task_row(&mut conn, task_id).await.state, "FAILED");
    assert_eq!(breaker_state(&breakers, activity), ("closed", 0));
}

// ---------------------------------------------------------------------------
// Open breaker: defer mode
// ---------------------------------------------------------------------------

const WF_NAME: &str = "wf_t1809_call";

type BoxFut<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>>;

fn echo(_ctx: &autumn_harvest::ActivityContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move { Ok(input) })
}

/// Calls the activity named in the input once, with no retries.
fn wf_call(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        let activity = input["activity"].as_str().unwrap_or_default().to_owned();
        let queue = ctx.queue_name().to_string();
        ctx.execute_activity_raw_with_opts(
            &activity,
            input,
            &queue,
            Some(RetryPolicy::fixed(1, Duration::from_millis(10))),
            None,
        )
        .await
        .map_err(|e| e.to_string())
    })
}

fn wf_info() -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name: WF_NAME,
        module: "activity_timeout_retry_tests",
        handler: wf_call,
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

fn act_info(name: &'static str, policy: CircuitBreakerPolicy) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "activity_timeout_retry_tests",
        default_retry_policy: None,
        default_start_to_close: Some(Duration::from_secs(30)),
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: None,
        max_concurrent: None,
        concurrency_key: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: Some(policy),
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        requires: None,
        handler: echo,
    }
}

fn build_worker(queue: &str, registry: Arc<HandlerRegistry>) -> Arc<Worker> {
    Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                codec_rotation_batch_size: 0,
                dr: autumn_harvest::replication::DrConfig::default(),
                worker_id: format!("{queue}-worker"),
                queues: vec![queue.to_string()],
                notification_database_url: None,
                max_concurrent_workflows: 4,
                max_concurrent_activities: 4,
                poll_interval: Duration::from_millis(10),
                shutdown_timeout: Duration::from_secs(5),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::ZERO,
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![ShardId::new(0)],
                worker_heartbeat_interval: Duration::from_secs(5),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 1000,
                resident_workflows: true,
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

/// Start one workflow that calls `activity` on `queue`.
async fn seed_workflow(conn: &mut AsyncPgConnection, queue: &str, activity: &str) -> ExecutionId {
    let input = serde_json::json!({ "activity": activity });
    let exec_id = seed_execution(conn, queue, input.clone()).await;
    let mut params = EnqueueParams::new(queue, TaskType::Workflow, input);
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    queue::enqueue(conn, &params)
        .await
        .expect("enqueue workflow task");
    exec_id
}

async fn execution_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> String {
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .expect("load execution")
        .state
}

/// Poll `cond` every 20 ms until it holds, or panic after `timeout`.
async fn wait_until<F, Fut>(what: &str, timeout: Duration, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    tokio::time::timeout(timeout, async {
        while !cond().await {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out after {timeout:?} waiting for {what}"));
}

/// The activity task rows of `exec_id`.
async fn activity_rows(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> Vec<TaskQueueItem> {
    harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .filter(harvest_task_queue::task_type.eq("activity"))
        .select(TaskQueueItem::as_select())
        .load(conn)
        .await
        .expect("load activity rows")
}

/// ADR 0004 §3: an open breaker defers work and does not fail it. When the
/// breaker closes, the deferred activity runs and the workflow completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn open_breaker_defers_work_by_default() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique("t1809-defer");
    let activity = "t1809_defer";
    let policy = CircuitBreakerPolicy::new(1, Duration::from_secs(60), Duration::from_secs(1));
    let registry = Arc::new(HandlerRegistry::new(
        vec![wf_info()],
        vec![act_info(activity, policy)],
    ));
    let breakers = registry.circuit_breakers();
    breakers.force_open(activity, Instant::now());

    let exec_id = seed_workflow(&mut conn, &queue, activity).await;
    let worker = build_worker(&queue, Arc::clone(&registry));
    let pool = build_pool(&url);
    let runner = Arc::clone(&worker);
    let handle = tokio::spawn(async move { runner.run(&pool).await });

    // Wait until the activity task exists and the worker has seen it.
    wait_until("a deferred activity", Duration::from_secs(20), || {
        let url = url.clone();
        async move {
            let mut conn = connect(&url).await;
            activity_rows(&mut conn, exec_id)
                .await
                .iter()
                .any(|row| row.state == "PENDING" && row.scheduled_at > Utc::now())
        }
    })
    .await;
    // Give a fail-fast worker time to fail the run.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let events = history(&mut conn, exec_id).await;
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityFailed { .. })),
        "an open breaker must not fail the activity: {events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityStarted { .. })),
        "a deferral appends no ActivityStarted"
    );
    assert_eq!(execution_state(&mut conn, exec_id).await, "RUNNING");
    let rows = activity_rows(&mut conn, exec_id).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].attempt, 0, "a deferral uses no attempt");

    breakers.force_close(activity);
    wait_until("the workflow completes", Duration::from_secs(20), || {
        let url = url.clone();
        async move {
            let mut conn = connect(&url).await;
            execution_state(&mut conn, exec_id).await == "COMPLETED"
        }
    })
    .await;

    worker.shutdown();
    handle.await.expect("worker joins");
}
