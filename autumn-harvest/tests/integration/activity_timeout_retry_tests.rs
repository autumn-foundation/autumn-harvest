#![cfg(feature = "db")]
//! Activity timeout retries and the open circuit (issue #1809).
//!
//! `docs/adr/0005-activity-timeout-retry-and-open-circuit.md` records the
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
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use autumn_harvest::circuit_breaker::CircuitBreakerRegistry;
use autumn_harvest::error::TimeoutType;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::{NewWorkflowExecution, TaskQueueItem, WorkflowExecution};
use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::policy::{CircuitBreakerPolicy, CircuitOpenMode, JitterPolicy};
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::schema::{harvest_task_queue, harvest_workflow_executions};
use autumn_harvest::telemetry::{ActivityStatus, MetricsRecorder, NoOpMetrics, TelemetryConfig};
use autumn_harvest::timeout::{self, TimeoutReason};
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
    enforce_with(conn, breakers, &NoOpMetrics).await;
}

async fn enforce_with(
    conn: &mut AsyncPgConnection,
    breakers: Option<&CircuitBreakerRegistry>,
    metrics: &(dyn MetricsRecorder + Send + Sync),
) {
    timeout::enforce_timeouts_once(
        conn,
        metrics,
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

/// Records `harvest.activity.attempts` outcomes (issue #1809).
#[derive(Default)]
struct AttemptLog(Mutex<Vec<ActivityStatus>>);

impl AttemptLog {
    fn outcomes(&self) -> Vec<ActivityStatus> {
        self.0.lock().unwrap().clone()
    }
}

impl MetricsRecorder for AttemptLog {
    fn record_activity_attempt(&self, _activity: &str, _queue: &str, outcome: ActivityStatus) {
        self.0.lock().unwrap().push(outcome);
    }
}

/// A registry whose worker records its attempt outcomes in `attempts`.
fn recording_registry(activity: ActivityInfo, attempts: Arc<AttemptLog>) -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![wf_info()],
        vec![activity],
        autumn_harvest::context::empty_shared_state(),
        Arc::new(
            TelemetryConfig::builder()
                .metrics(attempts as Arc<dyn MetricsRecorder>)
                .build(),
        ),
    ))
}

/// Counts `harvest.activity.retries` for each activity.
#[derive(Default)]
struct RetryCounter(Mutex<HashMap<String, u32>>);

impl RetryCounter {
    fn count(&self, activity: &str) -> u32 {
        self.0.lock().unwrap().get(activity).copied().unwrap_or(0)
    }
}

impl MetricsRecorder for RetryCounter {
    fn record_activity_retried(&self, activity_name: &str, _queue: &str) {
        *self
            .0
            .lock()
            .unwrap()
            .entry(activity_name.to_owned())
            .or_default() += 1;
    }
}

/// Set a column of one task row from SQL.
async fn set_task(conn: &mut AsyncPgConnection, task_id: Uuid, assignment: &str) {
    diesel::sql_query(format!(
        "UPDATE harvest_task_queue SET {assignment} WHERE id = $1"
    ))
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(conn)
    .await
    .expect("update the task row");
}

/// A retry policy with a fixed backoff and no jitter.
fn fixed_policy(max_attempts: u32, interval: Duration) -> serde_json::Value {
    let mut policy = RetryPolicy::fixed(max_attempts, interval);
    policy.jitter = JitterPolicy::None;
    serde_json::to_value(policy).expect("policy json")
}

async fn set_policy(conn: &mut AsyncPgConnection, task_id: Uuid, policy: serde_json::Value) {
    diesel::sql_query("UPDATE harvest_task_queue SET retry_policy = $2 WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .bind::<diesel::sql_types::Jsonb, _>(policy)
        .execute(conn)
        .await
        .expect("set the retry policy");
}

/// Seconds from the database clock to the row's `scheduled_at`.
async fn secs_until_scheduled(conn: &mut AsyncPgConnection, task_id: Uuid) -> f64 {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Double)]
        secs: f64,
    }
    diesel::sql_query(
        "SELECT EXTRACT(EPOCH FROM scheduled_at - clock_timestamp())::float8 AS secs \
         FROM harvest_task_queue WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .get_result::<Row>(conn)
    .await
    .expect("read scheduled_at")
    .secs
}

/// The `timed_out_claims` entry that names `claim`.
fn record(claim: &TaskQueueItem) -> Option<String> {
    claim
        .started_at
        .map(|started_at| queue::timed_out_claim_key(claim.attempt, started_at))
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

/// Start the attempt that `claimed` holds, as the worker does.
async fn start(
    conn: &mut AsyncPgConnection,
    claimed: &TaskQueueItem,
    exec_id: ExecutionId,
    activity: &str,
) {
    let started = autumn_harvest::worker::append_activity_started_for_test(
        conn,
        claimed,
        exec_id,
        activity,
        claimed.worker_id.as_deref().expect("a claimed task"),
        &PayloadCodecs::default(),
    )
    .await
    .expect("start the attempt");
    assert!(started.is_some(), "the attempt starts");
}

/// The number of workflow tasks of `exec_id`. A retried timeout must not
/// wake the workflow.
async fn workflow_task_count(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> usize {
    harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .filter(harvest_task_queue::task_type.eq("workflow"))
        .filter(harvest_task_queue::state.eq("PENDING"))
        .count()
        .get_result::<i64>(conn)
        .await
        .map(|n| usize::try_from(n).expect("count fits"))
        .expect("count workflow tasks")
}

// ---------------------------------------------------------------------------
// Timeout retries
// ---------------------------------------------------------------------------

/// ADR 0005 §1: a start-to-close timeout retries per the retry policy. With
/// `max_attempts = 3`, attempts 1 and 2 go back to `PENDING` and append no
/// event. Attempt 3 appends `ActivityTimedOut { StartToClose }` and fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_to_close_timeout_retries_until_max_attempts() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique("t1809-s2c");
    let activity = "t1809_s2c";
    let timeouts = Timeouts {
        start_to_close: Some(Duration::from_secs(1)),
        ..Timeouts::default()
    };
    let (exec_id, task_id) = seed_activity(&mut conn, &queue, activity, 3, timeouts).await;
    set_policy(&mut conn, task_id, fixed_policy(3, Duration::from_secs(2))).await;
    let metrics = RetryCounter::default();

    for attempt in 1..=3 {
        let claimed = claim(&mut conn, &queue, "w-s2c").await;
        assert_eq!(claimed.id, task_id);
        assert_eq!(claimed.attempt, attempt);
        age_claim(&mut conn, task_id).await;
        enforce_with(&mut conn, None, &metrics).await;

        let row = task_row(&mut conn, task_id).await;
        let events = history(&mut conn, exec_id).await;
        if attempt < 3 {
            assert_eq!(row.state, "PENDING", "attempt {attempt} retries");
            assert_eq!(row.attempt, attempt, "a retry keeps the attempt number");
            assert!(row.worker_id.is_none(), "a retry releases the claim");
            assert!(
                row.error
                    .as_deref()
                    .is_some_and(|e| e.contains("StartToClose")),
                "the retry records the timeout as the previous failure: {:?}",
                row.error
            );
            assert!(
                timed_out(&events).is_empty(),
                "a retried attempt appends no ActivityTimedOut"
            );
            let wait = secs_until_scheduled(&mut conn, task_id).await;
            assert!(wait > 1.0, "the retry waits for the backoff: {wait}s");
            assert_eq!(
                workflow_task_count(&mut conn, exec_id).await,
                0,
                "a retried timeout does not wake the workflow"
            );
            assert_eq!(metrics.count(activity), u32::try_from(attempt).unwrap());
            // Skip the backoff, so the next claim is due.
            set_task(
                &mut conn,
                task_id,
                "scheduled_at = NOW() - INTERVAL '1 second'",
            )
            .await;
        } else {
            assert_eq!(row.state, "FAILED", "the last attempt fails");
            assert_eq!(timed_out(&events), vec![TimeoutType::StartToClose]);
            assert_eq!(
                metrics.count(activity),
                2,
                "a terminal timeout is not a retry"
            );
        }
    }
}

/// ADR 0005 §1: a heartbeat timeout retries like a start-to-close timeout.
/// The retry keeps the heartbeat details and the crash strikes.
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
    set_task(
        &mut conn,
        task_id,
        "heartbeat_details = '{\"cursor\": 7}'::jsonb, last_heartbeat_at = NOW(), \
         crash_strikes = 2",
    )
    .await;
    age_claim(&mut conn, task_id).await;
    enforce(&mut conn, None).await;

    let row = task_row(&mut conn, task_id).await;
    assert_eq!(row.state, "PENDING", "attempt 1 of 2 retries");
    assert_eq!(
        row.heartbeat_details,
        Some(serde_json::json!({ "cursor": 7 })),
        "the retry keeps the heartbeat checkpoint"
    );
    assert!(
        row.last_heartbeat_at.is_none() && row.started_at.is_none(),
        "the next attempt starts a fresh heartbeat clock"
    );
    assert_eq!(
        row.crash_strikes, 2,
        "a timeout does not prove the attempt ended without a crash"
    );
    assert_eq!(
        timed_out(&history(&mut conn, exec_id).await),
        Vec::<TimeoutType>::new()
    );

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

/// A heartbeat that lands after the scan, before the row lock, cancels the
/// heartbeat timeout. The sweeper re-checks the deadline under the lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn heartbeat_after_the_scan_cancels_the_timeout() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique("t1809-hb-late");
    let timeouts = Timeouts {
        heartbeat: Some(Duration::from_secs(60)),
        ..Timeouts::default()
    };
    let (_exec_id, task_id) = seed_activity(&mut conn, &queue, "t1809_hb_late", 2, timeouts).await;
    claim(&mut conn, &queue, "w-hb-late").await;
    set_task(
        &mut conn,
        task_id,
        "last_heartbeat_at = NOW() - INTERVAL '10 minutes'",
    )
    .await;
    let scanned = task_row(&mut conn, task_id).await;

    // The activity heartbeats after the scan.
    set_task(&mut conn, task_id, "last_heartbeat_at = NOW()").await;
    timeout::enforce_activity_timeout_for_test(
        &mut conn,
        &scanned,
        &TimeoutReason::Heartbeat,
        None,
        &PayloadCodecs::default(),
    )
    .await
    .expect("enforce");

    let row = task_row(&mut conn, task_id).await;
    assert_eq!(row.state, "RUNNING", "a live heartbeat keeps the attempt");
    assert_eq!(row.attempt, scanned.attempt);
}

/// A stale scan snapshot must not act on a later claim of the row. A
/// deferral before the handler lowers `attempt`, so the later claim reuses
/// the `(worker_id, attempt)` pair. Only `started_at` tells them apart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_scan_snapshot_leaves_a_later_claim_alone() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique("t1809-stale");
    let activity = "t1809_stale";
    let timeouts = Timeouts {
        start_to_close: Some(Duration::from_secs(1)),
        ..Timeouts::default()
    };
    let (exec_id, task_id) = seed_activity(&mut conn, &queue, activity, 3, timeouts).await;
    let breakers = trip_on_first(activity);

    let first = claim(&mut conn, &queue, "w-stale").await;
    age_claim(&mut conn, task_id).await;
    let stale = task_row(&mut conn, task_id).await;

    // The owner defers before its handler starts, then claims again.
    let deferred = queue::defer_claimed_retry_for_budget(
        &mut conn,
        &queue::TaskClaim::of(&first).expect("claimed"),
        chrono::Duration::zero(),
    )
    .await
    .expect("defer");
    assert_eq!(deferred, queue::ClaimWrite::Applied);
    let second = claim(&mut conn, &queue, "w-stale").await;
    assert_eq!(
        (second.worker_id.as_deref(), second.attempt),
        (stale.worker_id.as_deref(), stale.attempt),
        "the later claim reuses the claim pair"
    );
    start(&mut conn, &second, exec_id, activity).await;

    timeout::enforce_activity_timeout_for_test(
        &mut conn,
        &stale,
        &TimeoutReason::StartToClose,
        Some(&breakers),
        &PayloadCodecs::default(),
    )
    .await
    .expect("enforce");

    let row = task_row(&mut conn, task_id).await;
    assert_eq!(row.state, "RUNNING", "the later claim keeps running");
    assert_eq!(row.started_at, second.started_at);
    assert_eq!(
        timed_out(&history(&mut conn, exec_id).await),
        Vec::<TimeoutType>::new()
    );
    assert_eq!(breaker_state(&breakers, activity), ("closed", 0));
}

/// Claim A, defer it, then claim and start B. Returns the task, A's scan
/// snapshot and B (issue #1809).
async fn schedule_to_close_after_a_new_claim(
    conn: &mut AsyncPgConnection,
    queue: &str,
    activity: &str,
) -> (Uuid, TaskQueueItem, TaskQueueItem) {
    let timeouts = Timeouts {
        start_to_close: Some(Duration::from_secs(60)),
        schedule_to_close: Some(Duration::from_secs(3600)),
        ..Timeouts::default()
    };
    let (exec_id, task_id) = seed_activity(conn, queue, activity, 3, timeouts).await;
    let first = claim(conn, queue, "w-stc-current").await;
    let stale = task_row(conn, task_id).await;
    let deferred = queue::defer_claimed_retry_for_budget(
        conn,
        &queue::TaskClaim::of(&first).expect("claimed"),
        chrono::Duration::zero(),
    )
    .await
    .expect("defer");
    assert_eq!(deferred, queue::ClaimWrite::Applied);
    let second = claim(conn, queue, "w-stc-current").await;
    start(conn, &second, exec_id, activity).await;
    (task_id, stale, second)
}

async fn enforce_schedule_to_close(
    conn: &mut AsyncPgConnection,
    stale: &TaskQueueItem,
    breakers: &CircuitBreakerRegistry,
) {
    timeout::enforce_activity_timeout_for_test(
        conn,
        stale,
        &TimeoutReason::ScheduleToClose,
        Some(breakers),
        &PayloadCodecs::default(),
    )
    .await
    .expect("enforce");
}

/// A schedule-to-close timeout ends whichever claim holds the row (issue
/// #1809). When a later claim started its handler, that claim is the one
/// timed out. The enforcing process counts it once, and an owner in the
/// same process does not count it again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schedule_to_close_counts_the_current_claim_once_in_process() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique("t1809-stc-local");
    let activity = "t1809_stc_local";
    let (task_id, stale, second) =
        schedule_to_close_after_a_new_claim(&mut conn, &queue, activity).await;
    let breakers = trip_on(activity, 2);

    // The owner of the current claim runs in this process.
    let autumn_harvest::circuit_breaker::DispatchDecision::Allow { token } =
        breakers.on_dispatch(activity, Instant::now())
    else {
        panic!("a closed breaker admits the dispatch");
    };
    let key = autumn_harvest::circuit_breaker::ClaimKey {
        task_id,
        attempt: second.attempt,
        started_at: second.started_at,
    };
    breakers.begin_claim(activity, key, token);

    enforce_schedule_to_close(&mut conn, &stale, &breakers).await;
    let row = task_row(&mut conn, task_id).await;
    assert_eq!(row.state, "FAILED");
    assert_eq!(
        row.timed_out_claims,
        Some(vec![record(&second)]),
        "the record names the claim that was timed out"
    );
    assert_eq!(
        breaker_state(&breakers, activity),
        ("closed", 1),
        "the enforcing process counts the current claim"
    );

    breakers.on_claim_lost(activity, token, key, true, Instant::now());
    assert_eq!(
        breaker_state(&breakers, activity),
        ("closed", 1),
        "the owner in the same process does not count it again"
    );
}

/// The same timeout, with the owner of the current claim in another
/// process. The enforcing process still counts it, and the owner's process
/// counts it from the record (issue #1809).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schedule_to_close_counts_a_remote_current_claim() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique("t1809-stc-remote");
    let activity = "t1809_stc_remote";
    let (task_id, stale, second) =
        schedule_to_close_after_a_new_claim(&mut conn, &queue, activity).await;
    let enforcer = trip_on(activity, 2);

    enforce_schedule_to_close(&mut conn, &stale, &enforcer).await;
    assert_eq!(breaker_state(&enforcer, activity), ("closed", 1));
    let row = task_row(&mut conn, task_id).await;
    assert_eq!(row.timed_out_claims, Some(vec![record(&second)]));

    // The owner's process counts it from the record.
    let owner = trip_on(activity, 2);
    let autumn_harvest::circuit_breaker::DispatchDecision::Allow { token } =
        owner.on_dispatch(activity, Instant::now())
    else {
        panic!("a closed breaker admits the dispatch");
    };
    let key = autumn_harvest::circuit_breaker::ClaimKey {
        task_id,
        attempt: second.attempt,
        started_at: second.started_at,
    };
    owner.begin_claim(activity, key, token);
    owner.on_claim_lost(activity, token, key, true, Instant::now());
    assert_eq!(breaker_state(&owner, activity), ("closed", 1));
}

/// ADR 0005 §1: no retry starts after `schedule_to_close`. The task fails
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
    let (exec_id, task_id) = seed_activity(&mut conn, &queue, "t1809_deadline", 3, timeouts).await;

    claim(&mut conn, &queue, "w-deadline").await;
    age_claim(&mut conn, task_id).await;
    // The deadline is ahead, but the 60 s backoff would end after it.
    set_task(
        &mut conn,
        task_id,
        "schedule_to_close_at = clock_timestamp() + INTERVAL '30 seconds'",
    )
    .await;
    set_policy(&mut conn, task_id, fixed_policy(3, Duration::from_secs(60))).await;
    enforce(&mut conn, None).await;

    let row = task_row(&mut conn, task_id).await;
    assert_eq!(row.state, "FAILED", "no retry past the deadline");
    assert_eq!(
        timed_out(&history(&mut conn, exec_id).await),
        vec![TimeoutType::StartToClose]
    );
}

/// ADR 0005 §1: a pause stops the `schedule_to_close` clock, so the deadline
/// does not stop the retry of a paused execution.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn paused_execution_retries_past_the_deadline() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique("t1809-paused");
    let timeouts = Timeouts {
        start_to_close: Some(Duration::from_secs(1)),
        schedule_to_close: Some(Duration::from_secs(3600)),
        ..Timeouts::default()
    };
    let (exec_id, task_id) = seed_activity(&mut conn, &queue, "t1809_paused", 3, timeouts).await;

    claim(&mut conn, &queue, "w-paused").await;
    age_claim(&mut conn, task_id).await;
    set_task(
        &mut conn,
        task_id,
        "schedule_to_close_at = clock_timestamp() + INTERVAL '30 seconds'",
    )
    .await;
    set_policy(&mut conn, task_id, fixed_policy(3, Duration::from_secs(60))).await;
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set(harvest_workflow_executions::state.eq("PAUSED"))
        .execute(&mut conn)
        .await
        .expect("pause the execution");
    enforce(&mut conn, None).await;

    let row = task_row(&mut conn, task_id).await;
    assert_eq!(row.state, "PENDING", "a paused run keeps its retry");
    assert_eq!(
        timed_out(&history(&mut conn, exec_id).await),
        Vec::<TimeoutType>::new()
    );
}

/// A timeout after the run has ended starts no new attempt (issue #1870).
///
/// A workflow can fail while one of its activities still runs. A retry would
/// then run the handler again for a sealed run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_after_the_run_ends_starts_no_new_attempt() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique("t1870-sealed");
    let timeouts = Timeouts {
        start_to_close: Some(Duration::from_secs(1)),
        ..Timeouts::default()
    };
    let (exec_id, task_id) = seed_activity(&mut conn, &queue, "t1870_sealed", 3, timeouts).await;

    claim(&mut conn, &queue, "w-sealed").await;
    age_claim(&mut conn, task_id).await;
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set(harvest_workflow_executions::state.eq("FAILED"))
        .execute(&mut conn)
        .await
        .expect("end the run");
    enforce(&mut conn, None).await;

    let row = task_row(&mut conn, task_id).await;
    assert_eq!(
        (row.state.as_str(), row.attempt),
        ("FAILED", 1),
        "a sealed run gets no new attempt"
    );
}

/// ADR 0005 §1: schedule-to-start is not retried, and a task that never left
/// the queue never feeds the breaker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schedule_to_start_timeout_stays_terminal() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique("t1809-s2s");
    let activity = "t1809_s2s";
    let (exec_id, task_id) =
        seed_activity(&mut conn, &queue, activity, 3, Timeouts::default()).await;
    set_task(
        &mut conn,
        task_id,
        "schedule_to_start = INTERVAL '1 second', scheduled_at = NOW() - INTERVAL '10 minutes'",
    )
    .await;
    let breakers = trip_on_first(activity);
    enforce(&mut conn, Some(&breakers)).await;

    let row = task_row(&mut conn, task_id).await;
    assert_eq!(row.state, "FAILED");
    assert_eq!(
        timed_out(&history(&mut conn, exec_id).await),
        vec![TimeoutType::ScheduleToStart]
    );
    assert_eq!(breaker_state(&breakers, activity), ("closed", 0));
}

/// ADR 0005 §2: a `PENDING` task that passes `schedule_to_close` in the queue
/// never feeds the breaker, even after an earlier attempt started.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_schedule_to_close_timeout_does_not_feed_the_breaker() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique("t1809-s2c-pending");
    let activity = "t1809_s2c_pending";
    let timeouts = Timeouts {
        start_to_close: Some(Duration::from_secs(1)),
        ..Timeouts::default()
    };
    let (exec_id, task_id) = seed_activity(&mut conn, &queue, activity, 3, timeouts).await;

    // Attempt 1 starts and times out, so the row is PENDING with a marker.
    let first = claim(&mut conn, &queue, "w-s2c-pending").await;
    start(&mut conn, &first, exec_id, activity).await;
    age_claim(&mut conn, task_id).await;
    enforce(&mut conn, None).await;
    assert_eq!(task_row(&mut conn, task_id).await.state, "PENDING");

    set_task(
        &mut conn,
        task_id,
        "schedule_to_close_at = NOW() - INTERVAL '1 second'",
    )
    .await;
    let breakers = trip_on_first(activity);
    enforce(&mut conn, Some(&breakers)).await;

    assert_eq!(task_row(&mut conn, task_id).await.state, "FAILED");
    assert_eq!(
        timed_out(&history(&mut conn, exec_id).await),
        vec![TimeoutType::ScheduleToClose]
    );
    assert_eq!(breaker_state(&breakers, activity), ("closed", 0));
}

// ---------------------------------------------------------------------------
// Breaker feed
// ---------------------------------------------------------------------------

/// A breaker that trips on the first counted failure.
fn trip_on_first(activity: &str) -> CircuitBreakerRegistry {
    trip_on(activity, 1)
}

/// A breaker that trips on the `threshold`-th counted failure.
fn trip_on(activity: &str, threshold: u32) -> CircuitBreakerRegistry {
    let policy =
        CircuitBreakerPolicy::new(threshold, Duration::from_secs(60), Duration::from_secs(60));
    CircuitBreakerRegistry::new(HashMap::from([(activity.to_string(), policy)]))
}

fn breaker_state(breakers: &CircuitBreakerRegistry, activity: &str) -> (&'static str, u32) {
    let snapshot = breakers
        .snapshot(activity, Instant::now())
        .expect("activity has a policy");
    (snapshot.state, snapshot.rolling_failure_count)
}

/// ADR 0005 §2: a timeout of a claimed task whose handler never started
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

    let row = task_row(&mut conn, task_id).await;
    assert_eq!(row.state, "FAILED");
    assert_eq!(
        breaker_state(&breakers, activity),
        ("closed", 0),
        "a handler that never started says nothing about the downstream"
    );
    assert_eq!(
        row.timed_out_claims, None,
        "the owner of an unstarted claim must not count its timeout either"
    );
}

/// ADR 0005 §2: a timeout of an attempt whose handler started still counts.
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
    start(&mut conn, &claimed, exec_id, activity).await;
    age_claim(&mut conn, task_id).await;
    enforce(&mut conn, Some(&breakers)).await;

    assert_eq!(breaker_state(&breakers, activity).0, "open");
    let row = task_row(&mut conn, task_id).await;
    assert_eq!(
        row.timed_out_claims,
        Some(vec![record(&row)]),
        "the owner of the claim reads this record to count the timeout"
    );
}

/// Records the `harvest.activity.duration` samples of one activity's failed
/// attempts. The enforcer scans every task, so a shared database can hold
/// expired tasks of other tests.
struct DurationLog {
    activity: &'static str,
    samples: Mutex<Vec<f64>>,
}

impl MetricsRecorder for DurationLog {
    fn record_activity_completed_with_error_type(
        &self,
        activity_name: &str,
        _queue: &str,
        duration_secs: f64,
        status: ActivityStatus,
        _error_type: Option<&str>,
    ) {
        if activity_name == self.activity && status == ActivityStatus::Failed {
            self.samples.lock().unwrap().push(duration_secs);
        }
    }
}

/// The enforcer measures a timed-out attempt from its handler start, as the
/// worker does. The wait between the claim and the handler start is local
/// setup, so it stays out of the activity latency.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_duration_counts_from_the_handler_start() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique("t1809-duration");
    let activity = "t1809_duration";
    let timeouts = Timeouts {
        start_to_close: Some(Duration::from_secs(1)),
        ..Timeouts::default()
    };
    let (exec_id, task_id) = seed_activity(&mut conn, &queue, activity, 1, timeouts).await;

    let claimed = claim(&mut conn, &queue, "w-duration").await;
    age_claim(&mut conn, task_id).await;
    start(&mut conn, &claimed, exec_id, activity).await;
    let durations = DurationLog {
        activity,
        samples: Mutex::default(),
    };
    enforce_with(&mut conn, None, &durations).await;

    let samples = durations.samples.lock().unwrap().clone();
    assert_eq!(samples.len(), 1, "one failed attempt: {samples:?}");
    assert!(
        samples[0] < 60.0,
        "the sample covers the handler run, not the 10 minutes since the claim: {samples:?}"
    );
}

/// Seed a one-token bucket that never refills, and point `task_id` at it.
/// A bucket that refills would replace a missing refund on its own.
async fn one_token_bucket(conn: &mut AsyncPgConnection, task_id: Uuid) -> String {
    bucket(conn, task_id, 1.0).await
}

/// Seed a bucket with one token and room for `burst`, and point `task_id` at
/// it. It never refills.
async fn bucket(conn: &mut AsyncPgConnection, task_id: Uuid, burst: f64) -> String {
    let key = format!("t1809-bucket-{task_id}");
    diesel::sql_query(
        "INSERT INTO harvest_rate_limit_buckets (key, refill_rate, burst, tokens, last_refilled_at) \
         VALUES ($1, 0.0, $2, 1.0, NOW())",
    )
    .bind::<diesel::sql_types::Text, _>(key.as_str())
    .bind::<diesel::sql_types::Double, _>(burst)
    .execute(conn)
    .await
    .expect("seed the rate-limit bucket");
    set_task(conn, task_id, &format!("rate_limit_key = '{key}'")).await;
    key
}

/// The tokens left in the bucket `key`.
async fn bucket_tokens(conn: &mut AsyncPgConnection, key: &str) -> f64 {
    #[derive(diesel::QueryableByName)]
    struct Tokens {
        #[diesel(sql_type = diesel::sql_types::Double)]
        tokens: f64,
    }
    diesel::sql_query("SELECT tokens FROM harvest_rate_limit_buckets WHERE key = $1")
        .bind::<diesel::sql_types::Text, _>(key)
        .get_result::<Tokens>(conn)
        .await
        .expect("read the rate-limit bucket")
        .tokens
}

/// A retried timeout of an attempt whose handler never started leaves the
/// claim's rate-limit token to the claim's owner. Only the dispatch that made
/// a debit refunds it, so the enforcer and the owner never both credit it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unstarted_timeout_retry_leaves_the_refund_to_the_owner() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique("t1809-refund");
    let timeouts = Timeouts {
        start_to_close: Some(Duration::from_secs(1)),
        ..Timeouts::default()
    };
    let (_exec_id, task_id) = seed_activity(&mut conn, &queue, "t1809_refund", 3, timeouts).await;
    let key = one_token_bucket(&mut conn, task_id).await;

    claim(&mut conn, &queue, "w-refund").await;
    assert!(
        bucket_tokens(&mut conn, &key).await < 0.01,
        "the claim takes the token"
    );
    age_claim(&mut conn, task_id).await;
    enforce(&mut conn, None).await;

    assert_eq!(
        task_row(&mut conn, task_id).await.state,
        "PENDING",
        "the timeout retries"
    );
    let tokens = bucket_tokens(&mut conn, &key).await;
    assert!(
        tokens < 0.01,
        "the enforcer refunds nothing; the owner refunds its own debit: {tokens}"
    );
}

/// A retried timeout of a started attempt keeps its debit: the handler ran.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn started_timeout_retry_keeps_its_rate_limit_debit() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique("t1809-debit");
    let activity = "t1809_debit";
    let timeouts = Timeouts {
        start_to_close: Some(Duration::from_secs(1)),
        ..Timeouts::default()
    };
    let (exec_id, task_id) = seed_activity(&mut conn, &queue, activity, 3, timeouts).await;
    let key = one_token_bucket(&mut conn, task_id).await;

    let claimed = claim(&mut conn, &queue, "w-debit").await;
    start(&mut conn, &claimed, exec_id, activity).await;
    age_claim(&mut conn, task_id).await;
    enforce(&mut conn, None).await;

    assert_eq!(
        task_row(&mut conn, task_id).await.state,
        "PENDING",
        "the timeout retries"
    );
    let tokens = bucket_tokens(&mut conn, &key).await;
    assert!(
        tokens < 0.01,
        "the started attempt keeps its debit: {tokens}"
    );
}

/// A terminal timeout of a claim whose handler never started also leaves the
/// claim's rate-limit token to its owner.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unstarted_terminal_timeout_leaves_the_refund_to_the_owner() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique("t1809-refund-final");
    let timeouts = Timeouts {
        start_to_close: Some(Duration::from_secs(1)),
        ..Timeouts::default()
    };
    let (_exec_id, task_id) =
        seed_activity(&mut conn, &queue, "t1809_refund_final", 1, timeouts).await;
    let key = one_token_bucket(&mut conn, task_id).await;

    claim(&mut conn, &queue, "w-refund-final").await;
    age_claim(&mut conn, task_id).await;
    enforce(&mut conn, None).await;

    assert_eq!(
        task_row(&mut conn, task_id).await.state,
        "FAILED",
        "the last attempt times out for good"
    );
    let tokens = bucket_tokens(&mut conn, &key).await;
    assert!(
        tokens < 0.01,
        "the enforcer refunds nothing; the owner refunds its own debit: {tokens}"
    );
}

/// A circuit breaker that tracks an activity moves its debit to dispatch, so
/// its claim debits nothing. The enforcer refunds nothing for it either.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unstarted_timeout_of_a_tracked_activity_refunds_nothing() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique("t1809-tracked");
    let activity = "t1809_tracked";
    let timeouts = Timeouts {
        start_to_close: Some(Duration::from_secs(1)),
        ..Timeouts::default()
    };
    let (_exec_id, task_id) = seed_activity(&mut conn, &queue, activity, 3, timeouts).await;
    // Room for two tokens, so an extra refund shows.
    let key = bucket(&mut conn, task_id, 2.0).await;

    let claimed = queue::claim_task(
        &mut conn,
        std::slice::from_ref(&queue),
        "w-tracked",
        "",
        None,
        &[activity.to_string()],
        &[],
    )
    .await
    .expect("claim")
    .expect("a claimable task");
    assert_eq!(claimed.id, task_id);
    age_claim(&mut conn, task_id).await;
    enforce(&mut conn, Some(&trip_on(activity, 3))).await;

    assert_eq!(
        task_row(&mut conn, task_id).await.state,
        "PENDING",
        "the timeout retries"
    );
    let tokens = bucket_tokens(&mut conn, &key).await;
    assert!(
        (tokens - 1.0).abs() < 0.01,
        "the tracked claim took no token, so none comes back: {tokens}"
    );
}

/// A result write that fails before it knows its outcome reads the claim's
/// timeout record. A record means the enforcer settled and counted the
/// attempt, so the worker records nothing more and the record goes away.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_result_write_honours_the_timeout_record() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);
    let queue = unique("t1809-settle");
    let activity = "t1809_settle";
    let timeouts = Timeouts {
        start_to_close: Some(Duration::from_secs(1)),
        ..Timeouts::default()
    };
    let (exec_id, task_id) = seed_activity(&mut conn, &queue, activity, 3, timeouts).await;

    let claimed = claim(&mut conn, &queue, "w-settle").await;
    start(&mut conn, &claimed, exec_id, activity).await;
    age_claim(&mut conn, task_id).await;
    // The worker holds the claim as it saw it: aged, and started.
    let held = task_row(&mut conn, task_id).await;
    enforce(&mut conn, None).await;
    assert_eq!(
        task_row(&mut conn, task_id).await.timed_out_claims,
        Some(vec![record(&held)]),
        "the enforcer records the timed-out claim"
    );

    let settled = autumn_harvest::worker::settle_result_write_for_test(&pool, &held, None).await;
    assert_eq!(
        settled,
        (Some(false), true),
        "a timeout took the claim, so the failed write did not apply"
    );
    assert_eq!(
        task_row(&mut conn, task_id).await.timed_out_claims,
        Some(vec![]),
        "the owner takes its record"
    );

    let unrelated = autumn_harvest::worker::settle_result_write_for_test(&pool, &held, None).await;
    assert_eq!(
        unrelated,
        (None, false),
        "without a record, the write outcome stays unknown"
    );
}

/// A timeout record names its claim by `attempt` and `started_at`. A claim
/// stamped with `NOW()` takes the start time of its transaction, so two
/// claims of one task can share `started_at`. The owner of a later attempt
/// with the same `started_at` must not take the record of an earlier one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_timeout_record_names_the_attempt_as_well_as_the_start() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);
    let queue = unique("t1809-key");
    let activity = "t1809_key";
    let timeouts = Timeouts {
        start_to_close: Some(Duration::from_secs(1)),
        ..Timeouts::default()
    };
    let (exec_id, task_id) = seed_activity(&mut conn, &queue, activity, 3, timeouts).await;

    let claimed = claim(&mut conn, &queue, "w-key").await;
    start(&mut conn, &claimed, exec_id, activity).await;
    age_claim(&mut conn, task_id).await;
    let held = task_row(&mut conn, task_id).await;
    enforce(&mut conn, None).await;
    let records = |row: &TaskQueueItem| row.timed_out_claims.as_ref().map(Vec::len);
    assert_eq!(records(&task_row(&mut conn, task_id).await), Some(1));

    // A later claim with the same start time loses its claim to something
    // that is not a timeout.
    let mut later = held.clone();
    later.attempt = held.attempt + 1;
    let settled = autumn_harvest::worker::settle_result_write_for_test(&pool, &later, None).await;
    assert_eq!(
        settled,
        (None, false),
        "the record of another attempt says nothing about this claim"
    );
    assert_eq!(
        records(&task_row(&mut conn, task_id).await),
        Some(1),
        "the record stays for its own owner"
    );

    let settled = autumn_harvest::worker::settle_result_write_for_test(&pool, &held, None).await;
    assert_eq!(settled, (Some(false), true), "the owner finds its record");
    assert_eq!(records(&task_row(&mut conn, task_id).await), Some(0));
}

/// A result write that fails before it knows its outcome can leave the claim
/// held. The timeout enforcer then times the claim out later and counts the
/// attempt metrics. So the worker leaves the count to the enforcer, and the
/// attempt counts once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_write_on_a_held_claim_leaves_the_count_to_the_enforcer() {
    use autumn_harvest::worker::{SettledAttempt, settle_attempt_for_test};
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);
    let queue = unique("t1809-held");
    let activity = "t1809_held";
    let timeouts = Timeouts {
        start_to_close: Some(Duration::from_secs(1)),
        ..Timeouts::default()
    };
    let (exec_id, task_id) = seed_activity(&mut conn, &queue, activity, 3, timeouts).await;
    let claimed = claim(&mut conn, &queue, "w-held").await;
    start(&mut conn, &claimed, exec_id, activity).await;
    let held = task_row(&mut conn, task_id).await;

    assert_eq!(
        settle_attempt_for_test(&pool, &held, None).await,
        SettledAttempt {
            applied: None,
            lost_to_timeout: false,
            enforcer_counts: true,
        },
        "the claim is still held, so the enforcer ends it and counts it"
    );

    // A claim of another attempt is not held. Without a record, the worker
    // counts its own attempt.
    let mut stale = held.clone();
    stale.attempt = held.attempt + 1;
    assert_eq!(
        settle_attempt_for_test(&pool, &stale, None).await,
        SettledAttempt {
            applied: None,
            lost_to_timeout: false,
            enforcer_counts: false,
        }
    );

    // Aging moves `started_at`, so read the claim as the worker holds it.
    age_claim(&mut conn, task_id).await;
    let held = task_row(&mut conn, task_id).await;
    enforce(&mut conn, None).await;
    assert_eq!(
        settle_attempt_for_test(&pool, &held, None).await,
        SettledAttempt {
            applied: Some(false),
            lost_to_timeout: true,
            enforcer_counts: true,
        },
        "the enforcer took the claim and counted the attempt"
    );
}

/// A WASM start marker can commit just before its connection drops. The
/// worker then reads the marker on a new connection. A committed marker means
/// the handler started, so the guest must run. A later timeout would
/// otherwise count a guest that never ran.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lost_start_marker_is_read_back() {
    use autumn_harvest::worker::reconcile_lost_marker_for_test;
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = build_pool(&url);
    let queue = unique("t1809-marker");
    let activity = "t1809_marker";
    let (exec_id, task_id) =
        seed_activity(&mut conn, &queue, activity, 3, Timeouts::default()).await;
    let claimed = claim(&mut conn, &queue, "w-marker").await;
    assert!(
        !reconcile_lost_marker_for_test(&pool, &claimed)
            .await
            .expect("read"),
        "no marker committed, so the guest does not start"
    );

    start(&mut conn, &claimed, exec_id, activity).await;
    let held = task_row(&mut conn, task_id).await;
    assert!(
        reconcile_lost_marker_for_test(&pool, &held)
            .await
            .expect("read"),
        "the marker committed, so the guest runs"
    );

    let mut stale = held.clone();
    stale.attempt = held.attempt + 1;
    assert!(
        !reconcile_lost_marker_for_test(&pool, &stale)
            .await
            .expect("read"),
        "a marker of another claim does not start this one"
    );
}

/// The terminal-task janitor keeps a row whose timed-out-claim record its
/// owner has not taken yet (issue #1809). The owner reads the record after
/// its cancellation grace, which can outlast the janitor's shortest window.
/// Without the record, it would miss a timeout that another process enforced.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_task_gc_keeps_a_row_with_an_outstanding_timeout_record() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique("t1809-gc");
    let activity = "t1809_gc";
    let timeouts = Timeouts {
        start_to_close: Some(Duration::from_secs(1)),
        ..Timeouts::default()
    };
    let (exec_id, task_id) = seed_activity(&mut conn, &queue, activity, 1, timeouts).await;
    let claimed = claim(&mut conn, &queue, "w-gc").await;
    start(&mut conn, &claimed, exec_id, activity).await;
    age_claim(&mut conn, task_id).await;
    enforce(&mut conn, None).await;
    let row = task_row(&mut conn, task_id).await;
    assert_eq!(row.state, "FAILED", "the only attempt times out for good");
    assert_eq!(row.timed_out_claims, Some(vec![record(&row)]));

    set_task(
        &mut conn,
        task_id,
        "completed_at = NOW() - INTERVAL '2 hours'",
    )
    .await;
    let cutoff = Utc::now() - chrono::Duration::hours(1);
    queue::sweep_terminal_tasks(&mut conn, cutoff, 1000, false)
        .await
        .expect("sweep");
    let kept: i64 = harvest_task_queue::table
        .filter(harvest_task_queue::id.eq(task_id))
        .count()
        .get_result(&mut conn)
        .await
        .expect("count");
    assert_eq!(kept, 1, "the owner has not taken its record yet");

    set_task(&mut conn, task_id, "timed_out_claims = '{}'").await;
    queue::sweep_terminal_tasks(&mut conn, cutoff, 1000, false)
        .await
        .expect("sweep");
    let kept: i64 = harvest_task_queue::table
        .filter(harvest_task_queue::id.eq(task_id))
        .count()
        .get_result(&mut conn)
        .await
        .expect("count");
    assert_eq!(kept, 0, "a settled row ages out as before");
}

/// ADR 0005 §2, on the retry path: a retried timeout of a started attempt
/// feeds the breaker. A later claim is a new attempt, so the start of an
/// earlier attempt does not mark it started.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_started_attempts_feed_the_breaker_across_retries() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique("t1809-later");
    let activity = "t1809_later";
    let timeouts = Timeouts {
        start_to_close: Some(Duration::from_secs(1)),
        ..Timeouts::default()
    };
    let (exec_id, task_id) = seed_activity(&mut conn, &queue, activity, 3, timeouts).await;
    let breakers = trip_on(activity, 2);

    // Attempt 1 starts and times out: it retries and counts once.
    let first = claim(&mut conn, &queue, "w-later").await;
    start(&mut conn, &first, exec_id, activity).await;
    age_claim(&mut conn, task_id).await;
    enforce(&mut conn, Some(&breakers)).await;
    assert_eq!(task_row(&mut conn, task_id).await.state, "PENDING");
    assert_eq!(breaker_state(&breakers, activity), ("closed", 1));

    // Attempts 2 and 3 are claimed by the same worker but never start.
    for expected in ["PENDING", "FAILED"] {
        claim(&mut conn, &queue, "w-later").await;
        age_claim(&mut conn, task_id).await;
        enforce(&mut conn, Some(&breakers)).await;
        assert_eq!(task_row(&mut conn, task_id).await.state, expected);
        assert_eq!(breaker_state(&breakers, activity), ("closed", 1));
    }
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

/// Opens the gate of [`gated`].
static GATE: std::sync::LazyLock<tokio::sync::Notify> =
    std::sync::LazyLock::new(tokio::sync::Notify::new);
/// Set when [`gated`] returns.
static GATED_DONE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Succeeds once the test opens [`GATE`].
fn gated(_ctx: &autumn_harvest::ActivityContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        GATE.notified().await;
        GATED_DONE.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(input)
    })
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
    act_info_with(name, policy, echo)
}

fn act_info_with(
    name: &'static str,
    policy: CircuitBreakerPolicy,
    handler: autumn_harvest::info::ActivityHandlerFn,
) -> ActivityInfo {
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
        handler,
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
                scanner: autumn_harvest::scanner_lease::ScannerConfig::default(),
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

/// The activity task row of `exec_id`, read on the database clock.
struct ActivityRowState {
    state: String,
    attempt: i32,
    /// `PENDING` with `scheduled_at` ahead of the database clock.
    deferred: bool,
}

async fn activity_row_state(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> Option<ActivityRowState> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
        #[diesel(sql_type = diesel::sql_types::Integer)]
        attempt: i32,
        #[diesel(sql_type = diesel::sql_types::Bool)]
        deferred: bool,
    }
    diesel::sql_query(
        "SELECT state, attempt, \
             (state = 'PENDING' AND scheduled_at > clock_timestamp()) AS deferred \
         FROM harvest_task_queue \
         WHERE workflow_exec_id = $1 AND task_type = 'activity'",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result::<Row>(conn)
    .await
    .optional()
    .expect("read the activity row")
    .map(|row| ActivityRowState {
        state: row.state,
        attempt: row.attempt,
        deferred: row.deferred,
    })
}

/// ADR 0005 §3: an open breaker defers work and does not fail it. When the
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

    // Wait until the worker has deferred the activity task at least once.
    wait_until("a deferred activity", Duration::from_secs(20), || {
        let url = url.clone();
        async move {
            let mut conn = connect(&url).await;
            activity_row_state(&mut conn, exec_id)
                .await
                .is_some_and(|row| row.deferred)
        }
    })
    .await;
    // Let the worker run several deferral cycles. A fail-fast worker would
    // fail the run in this window.
    for _ in 0..30 {
        let row = activity_row_state(&mut conn, exec_id)
            .await
            .expect("the activity task exists");
        assert!(
            row.attempt <= 1,
            "a deferral uses no attempt: attempt {}",
            row.attempt
        );
        assert!(
            row.state != "PENDING" || row.attempt == 0,
            "a deferred row keeps attempt 0: {} at attempt {}",
            row.state,
            row.attempt
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

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

/// ADR 0005 §3: a breaker that tripped on its own defers work until its
/// cooldown admits a probe. The deferred task runs as that probe, succeeds
/// and closes the breaker. No operator action is needed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn organically_open_breaker_defers_until_the_probe_closes_it() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique("t1809-organic");
    let activity = "t1809_organic";
    let policy = CircuitBreakerPolicy::new(1, Duration::from_secs(60), Duration::from_secs(1));
    let registry = Arc::new(HandlerRegistry::new(
        vec![wf_info()],
        vec![act_info(activity, policy)],
    ));
    let breakers = registry.circuit_breakers();
    let _ = breakers.on_external_failure(activity, Instant::now());
    assert_eq!(breaker_state(&breakers, activity).0, "open");

    let exec_id = seed_workflow(&mut conn, &queue, activity).await;
    let worker = build_worker(&queue, Arc::clone(&registry));
    let pool = build_pool(&url);
    let runner = Arc::clone(&worker);
    let handle = tokio::spawn(async move { runner.run(&pool).await });

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

    let events = history(&mut conn, exec_id).await;
    let started = events
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ActivityStarted { .. }))
        .count();
    assert_eq!(started, 1, "only the probe runs: {events:?}");
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityFailed { .. })),
        "no attempt fails: {events:?}"
    );
    assert_eq!(breaker_state(&breakers, activity).0, "closed");
}

/// A late result of a timed-out attempt must not move the breaker (issue
/// #1809). The timeout already counted. Before the fence, the late success
/// cleared the failure window and erased the timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_result_of_a_timed_out_attempt_leaves_the_breaker_alone() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique("t1809-late");
    let activity = "t1809_late_result";
    let policy = CircuitBreakerPolicy::new(2, Duration::from_secs(60), Duration::from_secs(60));
    let worker_attempts = Arc::new(AttemptLog::default());
    let registry = recording_registry(
        act_info_with(activity, policy, gated),
        Arc::clone(&worker_attempts),
    );
    let breakers = registry.circuit_breakers();
    let enforcer_attempts = AttemptLog::default();

    let exec_id = seed_workflow(&mut conn, &queue, activity).await;
    let worker = build_worker(&queue, Arc::clone(&registry));
    let pool = build_pool(&url);
    let runner = Arc::clone(&worker);
    let handle = tokio::spawn(async move { runner.run(&pool).await });

    wait_until("the activity starts", Duration::from_secs(20), || {
        let url = url.clone();
        async move {
            let mut conn = connect(&url).await;
            history(&mut conn, exec_id)
                .await
                .iter()
                .any(|e| matches!(e, WorkflowEvent::ActivityStarted { .. }))
        }
    })
    .await;
    let task_id: Uuid = harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .filter(harvest_task_queue::task_type.eq("activity"))
        .select(harvest_task_queue::id)
        .first(&mut conn)
        .await
        .expect("the activity task");
    // Shorten the deadline, and keep `started_at`. The owner finds its
    // record by that epoch.
    set_task(
        &mut conn,
        task_id,
        "start_to_close = INTERVAL '1 millisecond'",
    )
    .await;
    enforce_with(&mut conn, Some(&breakers), &enforcer_attempts).await;
    assert_eq!(breaker_state(&breakers, activity), ("closed", 1));
    assert_eq!(
        task_row(&mut conn, task_id)
            .await
            .timed_out_claims
            .map(|claims| claims.len()),
        Some(1),
        "the enforcer records the claim it timed out"
    );
    // The worker's own scanner can win the race. Either enforcer counts the
    // timed-out attempt as failed, once.
    wait_until(
        "the timed-out attempt counts as failed once",
        Duration::from_secs(5),
        || async {
            [enforcer_attempts.outcomes(), worker_attempts.outcomes()].concat()
                == vec![ActivityStatus::Failed]
        },
    )
    .await;

    // The hung attempt now returns a success, after its claim is gone.
    GATE.notify_one();
    wait_until(
        "the late handler returns",
        Duration::from_secs(5),
        || async {
            std::sync::atomic::AtomicBool::load(&GATED_DONE, std::sync::atomic::Ordering::SeqCst)
        },
    )
    .await;
    // The owner takes its own record, so the row holds only unsettled
    // owners.
    wait_until(
        "the owner takes its timeout record",
        Duration::from_secs(5),
        || {
            let url = url.clone();
            async move {
                let mut conn = connect(&url).await;
                task_row(&mut conn, task_id)
                    .await
                    .timed_out_claims
                    .is_some_and(|claims| claims.is_empty())
            }
        },
    )
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    worker.shutdown();
    handle.await.expect("worker joins");

    assert_eq!(
        breaker_state(&breakers, activity),
        ("closed", 1),
        "a late success of a lost claim must not clear the counted timeout"
    );
    assert!(
        !worker_attempts
            .outcomes()
            .contains(&ActivityStatus::Completed),
        "the late success of the timed-out attempt is not a completed attempt: {:?}",
        worker_attempts.outcomes()
    );
}

/// ADR 0005 §3: `FailFast` keeps the old behaviour. The open breaker fails
/// the attempt with a non-retryable `CircuitOpen`, so the workflow fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fail_fast_mode_fails_the_attempt() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique("t1809-failfast");
    let activity = "t1809_failfast";
    let policy = CircuitBreakerPolicy::new(1, Duration::from_secs(60), Duration::from_secs(60))
        .with_open_mode(CircuitOpenMode::FailFast);
    let registry = Arc::new(HandlerRegistry::new(
        vec![wf_info()],
        vec![act_info(activity, policy)],
    ));
    registry
        .circuit_breakers()
        .force_open(activity, Instant::now());

    let exec_id = seed_workflow(&mut conn, &queue, activity).await;
    let worker = build_worker(&queue, Arc::clone(&registry));
    let pool = build_pool(&url);
    let runner = Arc::clone(&worker);
    let handle = tokio::spawn(async move { runner.run(&pool).await });

    wait_until("the workflow fails", Duration::from_secs(20), || {
        let url = url.clone();
        async move {
            let mut conn = connect(&url).await;
            execution_state(&mut conn, exec_id).await == "FAILED"
        }
    })
    .await;
    let failed = history(&mut conn, exec_id).await.into_iter().any(|e| {
        matches!(
            e,
            WorkflowEvent::ActivityFailed { error_type, non_retryable: true, .. }
                if error_type == "CircuitOpen"
        )
    });
    assert!(failed, "the activity fails with CircuitOpen");
    // A short circuit calls no handler, so its timeout must not feed the
    // breaker (issue #1809).
    let markers: Vec<Option<i32>> = harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .filter(harvest_task_queue::task_type.eq("activity"))
        .select(harvest_task_queue::handler_started_attempt)
        .load(&mut conn)
        .await
        .expect("the activity task");
    assert_eq!(markers, vec![None], "a short circuit starts no handler");

    worker.shutdown();
    handle.await.expect("worker joins");
}
