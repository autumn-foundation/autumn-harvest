#![cfg(feature = "db")]
#![allow(clippy::unused_async)]
//! Drain release of activity claims (issue #1813).
//!
//! A draining worker gives back each claim whose task never started. One
//! join window before the drain deadline, it cancels its running activities.
//! It releases the claim of each handler that returns a retryable error, so a
//! peer retries it at once. It keeps the claim of a handler that ignores the
//! cancel. A peer must never run that activity at the same time.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to use a migrated Postgres. Otherwise the
//! suite starts a testcontainers Postgres 16.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::prelude::*;
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::schema::{harvest_task_queue, harvest_workers, harvest_workflow_executions};
use autumn_harvest::store;
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker};
use autumn_harvest::{ExecutionId, ShardId};

use chrono::Utc;
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use uuid::Uuid;

use crate::integration_e2e::{
    build_test_pool, runtime_config, setup_test_database_url_or_env, spawn_test_worker,
    wait_for_execution_state_with_timeout,
};

/// The drain budget of the worker under test.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(4);

// ---------------------------------------------------------------------------
// Handlers.
// ---------------------------------------------------------------------------

/// Calls the activity named in `input["activity"]` on its own queue.
#[workflow]
async fn drain_wf(
    ctx: &WorkflowContext,
    input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let name = input["activity"]
        .as_str()
        .ok_or("missing activity")?
        .to_string();
    let queue = ctx.queue_name().to_string();
    ctx.execute_activity_raw(&name, serde_json::json!({}), &queue)
        .await
        .map_err(|e| e.to_string())
}

static COOPERATIVE_STARTS: AtomicU32 = AtomicU32::new(0);

/// Attempt 1 runs until cancelled, then fails. A later attempt succeeds.
#[activity(start_to_close = "600s")]
async fn drain_cooperative(
    ctx: &ActivityContext,
    input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let _ = input;
    COOPERATIVE_STARTS.fetch_add(1, Ordering::SeqCst);
    let attempt = ctx.info().attempt;
    if attempt > 1 {
        return Ok(serde_json::json!({ "attempt": attempt }));
    }
    while !ctx.is_cancelled() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Err("cancelled".to_string())
}

static STUBBORN_STARTS: AtomicU32 = AtomicU32::new(0);
static STUBBORN_GO: tokio::sync::Notify = tokio::sync::Notify::const_new();

/// Ignores cancellation. Returns only when the test lets it go.
///
/// Its auto-heartbeat stops at the drain cancel. The 2 s heartbeat timeout
/// then runs out while the handler still runs.
#[activity(start_to_close = "600s", heartbeat_timeout = "2s")]
async fn drain_stubborn(
    ctx: &ActivityContext,
    input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let _ = input;
    STUBBORN_STARTS.fetch_add(1, Ordering::SeqCst);
    let _beat = ctx
        .start_auto_heartbeat_default()
        .map_err(|e| e.to_string())?;
    STUBBORN_GO.notified().await;
    Ok(serde_json::json!("done"))
}

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

/// Counts the retries that a worker enqueues for `drain_cooperative`.
#[derive(Default)]
struct RetryCounter(AtomicU32);

impl autumn_harvest::telemetry::MetricsRecorder for RetryCounter {
    fn record_activity_retried(&self, activity_name: &str, _queue: &str) {
        if activity_name == "drain_cooperative" {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
}

static COOPERATIVE_RETRIES: std::sync::LazyLock<Arc<RetryCounter>> =
    std::sync::LazyLock::new(Arc::default);

fn registry() -> Arc<HandlerRegistry> {
    let telemetry = Arc::new(autumn_harvest::telemetry::TelemetryConfig {
        metrics: Arc::clone(&*COOPERATIVE_RETRIES) as _,
        ..Default::default()
    });
    Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![drain_wf_info()],
        activities![drain_cooperative, drain_stubborn],
        autumn_harvest::context::empty_shared_state(),
        telemetry,
    ))
}

/// A running worker and the task that runs it.
struct Running {
    worker: Arc<Worker>,
    handle: tokio::task::JoinHandle<()>,
}

impl Running {
    fn start(worker_id: &str, queue: &str, pool: &DbPool) -> Self {
        // A slow liveness heartbeat keeps orphan reclaim out of the test
        // window. Only the drain may move a claim here. This holds when each
        // test owns its database or runs alone, as in CI.
        Self::start_with_heartbeat(worker_id, queue, pool, Duration::from_secs(15))
    }

    fn start_with_heartbeat(
        worker_id: &str,
        queue: &str,
        pool: &DbPool,
        heartbeat: Duration,
    ) -> Self {
        let mut config = runtime_config(worker_id, 2, 2, Duration::from_secs(10));
        config.queues = vec![queue.to_string()];
        config.shutdown_timeout = SHUTDOWN_TIMEOUT;
        config.cancellation_grace_period = Duration::from_secs(1);
        config.sticky_timeout = Duration::ZERO;
        config.worker_heartbeat_interval = heartbeat;
        let worker = Arc::new(Worker::new(config, registry()).expect("worker builds"));
        let handle = spawn_test_worker(Arc::clone(&worker), pool.clone());
        Self { worker, handle }
    }

    /// Request shutdown and return how long the drain took.
    async fn stop(self) -> Duration {
        let started = Instant::now();
        self.worker.shutdown();
        tokio::time::timeout(SHUTDOWN_TIMEOUT * 3, self.handle)
            .await
            .expect("the worker must stop")
            .expect("the worker task must not panic");
        started.elapsed()
    }
}

/// A queue name no earlier run used. A shared database can keep old rows.
fn unique(label: &str) -> String {
    format!("q1813-{label}-{}", Uuid::new_v4().simple())
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .expect("connect to Postgres")
}

/// Start a `drain_wf` run on `queue` that calls `activity`.
async fn seed_workflow(conn: &mut AsyncPgConnection, queue: &str, activity: &str) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let input = serde_json::json!({ "activity": activity });
    let row = NewWorkflowExecution {
        quota_key: None,
        id: exec_id.as_uuid(),
        workflow_name: "drain_wf",
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
    let mut params = EnqueueParams::new(queue, TaskType::Workflow, input);
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    queue::enqueue(conn, &params)
        .await
        .expect("enqueue workflow task");
    exec_id
}

/// The activity task row of `exec_id`.
#[derive(Debug)]
struct ActivityRow {
    state: String,
    worker_id: Option<String>,
    attempt: i32,
    error: Option<String>,
    scheduled_at: chrono::DateTime<Utc>,
}

async fn activity_row(url: &str, exec_id: ExecutionId) -> Option<ActivityRow> {
    let mut conn = connect(url).await;
    harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .filter(harvest_task_queue::task_type.eq("activity"))
        .select((
            harvest_task_queue::state,
            harvest_task_queue::worker_id,
            harvest_task_queue::attempt,
            harvest_task_queue::error,
            harvest_task_queue::scheduled_at,
        ))
        .first::<(
            String,
            Option<String>,
            i32,
            Option<String>,
            chrono::DateTime<Utc>,
        )>(&mut conn)
        .await
        .optional()
        .expect("load activity row")
        .map(
            |(state, worker_id, attempt, error, scheduled_at)| ActivityRow {
                state,
                worker_id,
                attempt,
                error,
                scheduled_at,
            },
        )
}

/// Enqueue an activity task on `queue` and mark it claimed by `worker_id`.
///
/// No dispatch body holds this claim.
async fn abandon_claim(conn: &mut AsyncPgConnection, queue: &str, worker_id: &str) -> Uuid {
    let params = EnqueueParams::new(queue, TaskType::Activity, serde_json::json!({}));
    let task_id = queue::enqueue(conn, &params)
        .await
        .expect("enqueue activity task");
    diesel::update(harvest_task_queue::table.find(task_id))
        .set((
            harvest_task_queue::state.eq("RUNNING"),
            harvest_task_queue::worker_id.eq(Some(worker_id)),
            harvest_task_queue::attempt.eq(1),
            harvest_task_queue::started_at.eq(Some(Utc::now())),
        ))
        .execute(conn)
        .await
        .expect("mark the task claimed");
    task_id
}

/// The `state` and `worker_id` of task `task_id`.
async fn task_state(url: &str, task_id: Uuid) -> (String, Option<String>) {
    let mut conn = connect(url).await;
    harvest_task_queue::table
        .find(task_id)
        .select((harvest_task_queue::state, harvest_task_queue::worker_id))
        .first(&mut conn)
        .await
        .expect("load task")
}

/// Wait until `worker_id` runs the activity handler.
async fn wait_for_start(url: &str, exec_id: ExecutionId, worker_id: &str, starts: &AtomicU32) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let row = activity_row(url, exec_id).await;
            if row
                .is_some_and(|r| r.state == "RUNNING" && r.worker_id.as_deref() == Some(worker_id))
                && AtomicU32::load(starts, Ordering::SeqCst) == 1
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the worker must start the activity within 30 s");
}

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

/// `release_unstarted_claim` gives back only the current claim.
///
/// It restores `attempt`, keeps `scheduled_at` and clears a sticky pin. A
/// stale claim and a row that is not `RUNNING` change nothing. The chaos
/// suite drives the same write through a real drain.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_unstarted_claim_restores_the_claim_and_is_fenced() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let queue = unique("unstarted");
    let mut conn = connect(&url).await;
    seed_workflow(&mut conn, &queue, "drain_cooperative").await;
    let worker = format!("{queue}-a");
    let task = queue::claim_task(
        &mut conn,
        std::slice::from_ref(&queue),
        &worker,
        "",
        None,
        &[],
        &[],
    )
    .await
    .expect("claim")
    .expect("the workflow task is claimable");
    diesel::sql_query(
        "UPDATE harvest_task_queue \
         SET sticky_worker_id = $2, sticky_until = NOW() + INTERVAL '1 hour' \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task.id)
    .bind::<diesel::sql_types::Text, _>(&worker)
    .execute(&mut conn)
    .await
    .expect("pin the row");

    let load = async |conn: &mut AsyncPgConnection| {
        harvest_task_queue::table
            .find(task.id)
            .select(autumn_harvest::models::TaskQueueItem::as_select())
            .first::<autumn_harvest::models::TaskQueueItem>(conn)
            .await
            .expect("load the row")
    };

    for stale in [
        queue::TaskClaim::new(task.id, "another-worker", task.attempt),
        queue::TaskClaim::new(task.id, &worker, task.attempt + 1),
    ] {
        let write = queue::release_unstarted_claim(&mut conn, &stale)
            .await
            .expect("release");
        assert_eq!(write, queue::ClaimWrite::LeaseLost, "{stale:?}");
        assert_eq!(load(&mut conn).await.state, "RUNNING", "{stale:?}");
    }

    let claim = queue::TaskClaim::of(&task).expect("a claimed row has a claim");
    let write = queue::release_unstarted_claim(&mut conn, &claim)
        .await
        .expect("release");
    assert_eq!(write, queue::ClaimWrite::Applied);
    let row = load(&mut conn).await;
    assert_eq!(row.state, "PENDING");
    assert!(row.worker_id.is_none());
    assert!(row.started_at.is_none());
    assert_eq!(row.attempt, task.attempt - 1, "no attempt is used");
    assert_eq!(row.scheduled_at, task.scheduled_at, "the row stays due");
    assert!(row.sticky_worker_id.is_none(), "the pin is cleared");
    assert!(row.sticky_until.is_none(), "the pin is cleared");

    let write = queue::release_unstarted_claim(&mut conn, &claim)
        .await
        .expect("release");
    assert_eq!(
        write,
        queue::ClaimWrite::LeaseLost,
        "a row that is not RUNNING is a no-op"
    );
}

/// A running activity that honours the cancel is joined and released. A peer
/// then retries it at once, not after its 600 s `start_to_close`.
///
/// RED: before the fix the drain never cancels the handler. The row stays
/// `RUNNING` under the stopped worker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drain_joins_a_cooperative_activity_and_a_peer_retries_it() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let queue = unique("coop");
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(&mut conn, &queue, "drain_cooperative").await;
    let pool = build_test_pool(&url);

    let worker_a = format!("{queue}-a");
    let a = Running::start(&worker_a, &queue, &pool);
    wait_for_start(&url, exec_id, &worker_a, &COOPERATIVE_STARTS).await;

    let drain = a.stop().await;
    assert!(
        drain < SHUTDOWN_TIMEOUT + Duration::from_secs(1),
        "the drain must end at its deadline: took {drain:?}"
    );
    let row = activity_row(&url, exec_id).await.expect("activity row");
    assert_eq!(
        row.state, "PENDING",
        "the joined claim is released: {row:?}"
    );
    assert!(row.worker_id.is_none(), "{row:?}");
    assert_eq!(row.attempt, 1, "the cancelled attempt counts: {row:?}");
    assert!(
        row.error
            .as_deref()
            .is_some_and(|e| e.contains("worker shutdown")),
        "the release names a retryable shutdown reason: {row:?}"
    );

    let worker_b = format!("{queue}-b");
    let b = Running::start(&worker_b, &queue, &pool);
    let exec =
        wait_for_execution_state_with_timeout(&url, exec_id, "COMPLETED", Duration::from_secs(30))
            .await;
    assert_eq!(
        exec.output,
        Some(serde_json::json!({ "attempt": 2 })),
        "the peer runs attempt 2"
    );
    assert_eq!(AtomicU32::load(&COOPERATIVE_STARTS, Ordering::SeqCst), 2);
    assert_eq!(
        AtomicU32::load(&COOPERATIVE_RETRIES.0, Ordering::SeqCst),
        1,
        "the drain release counts as one enqueued retry"
    );
    let row = activity_row(&url, exec_id).await.expect("activity row");
    assert_eq!(row.state, "COMPLETED", "{row:?}");
    b.stop().await;
}

/// A running activity that ignores the cancel keeps its claim.
///
/// The drain ends at its deadline. The row stays `RUNNING` under the drained
/// worker, so a live peer never starts a second copy. The handler's late
/// result still lands through the claim fence.
///
/// The host process outlives `run`, as an embedded runtime does. Both workers
/// use a 1 s heartbeat, so orphan reclaim judges the drained worker stale
/// after 2 s. The activity has a 2 s heartbeat timeout. The test waits longer
/// than both. The drained worker must keep its lease and the task heartbeat
/// while the handler runs. The kept lease must not hide a claim that no
/// handler holds, so the keeper gives such a claim back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drain_keeps_the_claim_of_an_activity_that_ignores_the_cancel() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let queue = unique("stubborn");
    let mut conn = connect(&url).await;
    let exec_id = seed_workflow(&mut conn, &queue, "drain_stubborn").await;
    let pool = build_test_pool(&url);

    let heartbeat = Duration::from_secs(1);
    let worker_a = format!("{queue}-a");
    let a = Running::start_with_heartbeat(&worker_a, &queue, &pool, heartbeat);
    wait_for_start(&url, exec_id, &worker_a, &STUBBORN_STARTS).await;
    let worker_b = format!("{queue}-b");
    let b = Running::start_with_heartbeat(&worker_b, &queue, &pool, heartbeat);

    // A claim that no dispatch body holds, as a failed release leaves one.
    // No worker polls its queue, so a release leaves it `PENDING`.
    let abandoned = abandon_claim(&mut conn, &format!("{queue}-abandoned"), &worker_a).await;

    let drain = a.stop().await;
    assert!(
        drain + Duration::from_millis(500) >= SHUTDOWN_TIMEOUT,
        "the drain waits for its deadline: took {drain:?}"
    );
    assert!(
        drain < SHUTDOWN_TIMEOUT + Duration::from_secs(3),
        "the drain must end at its deadline: took {drain:?}"
    );
    // A shutdown heartbeat does not re-register a missing row. The keeper
    // must restore it, or orphan reclaim sees no worker.
    diesel::delete(harvest_workers::table.find(&worker_a))
        .execute(&mut conn)
        .await
        .expect("delete the drained worker row");

    // Outlast the stale window (2 s), the heartbeat timeout (2 s) and a
    // reclaimer tick (1 s) on the live peer. It must not take the claim.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let row = activity_row(&url, exec_id).await.expect("activity row");
    assert_eq!(row.state, "RUNNING", "the claim stays held: {row:?}");
    assert_eq!(row.worker_id.as_deref(), Some(worker_a.as_str()), "{row:?}");
    assert_eq!(row.attempt, 1, "{row:?}");
    assert_eq!(
        AtomicU32::load(&STUBBORN_STARTS, Ordering::SeqCst),
        1,
        "no peer may run the activity while its handler runs"
    );
    let status: String = harvest_workers::table
        .find(&worker_a)
        .select(harvest_workers::status)
        .first(&mut conn)
        .await
        .expect("the keeper restores the worker row");
    assert_eq!(status, "Stopped", "the restored row claims no coverage");
    // The kept lease must not hide a claim that no handler holds.
    let (state, worker_id) = task_state(&url, abandoned).await;
    assert_eq!(state, "PENDING", "the keeper gives back an abandoned claim");
    assert_eq!(worker_id, None);

    STUBBORN_GO.notify_one();
    wait_for_execution_state_with_timeout(&url, exec_id, "COMPLETED", Duration::from_secs(30))
        .await;
    assert_eq!(AtomicU32::load(&STUBBORN_STARTS, Ordering::SeqCst), 1);
    b.stop().await;
}
