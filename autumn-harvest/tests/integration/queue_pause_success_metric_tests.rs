//! The issue #619 **success metric**, driven end to end by a real worker.
//!
//! > During a pause window **zero** activity tasks transition to `FAILED`/DLQ,
//! > and **100%** of held tasks resume to a successful terminal outcome after
//! > thaw (verified by an integration test that pauses, induces downstream
//! > failures, holds, then resumes).
//!
//! The story this test tells is the one the feature exists for:
//!
//! 1. A downstream dependency goes **down**. Real activity attempts fail and
//!    burn retry budget — proving the retry machinery is genuinely live and
//!    that, left alone, this backlog *would* march to `FAILED`/DLQ.
//! 2. The operator **pauses** the queue. From this instant nothing further is
//!    claimed: no attempt burns, nothing fails, nothing dead-letters.
//! 3. The hold lasts several worker poll cycles with the worker still running
//!    — the assertion window that would catch a leaked claim path.
//! 4. The downstream recovers and the operator **resumes**.
//! 5. **100%** of the held workflows reach `COMPLETED`, and the DLQ is empty.
//!
//! Step 1 is what makes the test falsifiable: without it, a queue that simply
//! never ran anything would pass trivially.

// Drives a real worker against Postgres via the `integration_e2e` helpers,
// which are themselves `db`-gated. Without this the no-default-features CI leg
// fails to compile on an unresolved import.
#![cfg(feature = "db")]
#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::queue_pause;
use autumn_harvest::types::{
    ExecutionId, Priority, WorkflowIdConflictPolicy, WorkflowIdReusePolicy,
};
use autumn_harvest::worker::HandlerRegistry;
use autumn_harvest::{StartSource, StartWorkflowParams, start_or_load_workflow_execution};
use diesel_async::SimpleAsyncConnection;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::json;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use crate::integration_e2e::{build_test_pool, runtime_config, spawn_test_worker};

/// The simulated downstream dependency. `false` = the outage is in effect and
/// every activity attempt fails.
static DOWNSTREAM_UP: AtomicBool = AtomicBool::new(false);
/// Every activity attempt that actually **ran** (i.e. was claimed and
/// dispatched). The load-bearing counter: it must not move while the queue is
/// held.
static ATTEMPTS_RUN: AtomicUsize = AtomicUsize::new(0);

/// The queue every workflow in this test uses. It is a fixed constant because
/// the worker binds its queues at construction time; cross-run isolation on a
/// shared env-provided database comes from the scrub in [`setup_db_url`]
/// instead (CI gets a fresh testcontainer and needs neither).
const TEST_QUEUE: &str = "qp-success-metric";

const WORKFLOWS: usize = 6;

// ── handlers ─────────────────────────────────────────────────────────────────

fn call_downstream_handler(
    _ctx: &autumn_harvest::context::ActivityContext,
    input: serde_json::Value,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + '_>,
> {
    Box::pin(async move {
        ATTEMPTS_RUN.fetch_add(1, Ordering::SeqCst);
        if AtomicBool::load(&DOWNSTREAM_UP, Ordering::SeqCst) {
            Ok(input)
        } else {
            Err("downstream unavailable".to_string())
        }
    })
}

fn held_workflow_handler(
    ctx: &autumn_harvest::context::WorkflowContext,
    input: serde_json::Value,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + '_>,
> {
    Box::pin(async move {
        let out = ctx
            .execute_activity_raw("call_downstream", input, TEST_QUEUE)
            .await
            .map_err(|e| e.to_string())?;
        Ok(out)
    })
}

// ── registry ─────────────────────────────────────────────────────────────────

fn registry() -> Arc<HandlerRegistry> {
    let wf = WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name: "held_workflow",
        module: "queue_pause_success_metric_tests",
        handler: held_workflow_handler,
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
    };
    let act = ActivityInfo {
        name: "call_downstream",
        module: "queue_pause_success_metric_tests",
        // Generous budget with a short backoff: the pre-pause outage must burn
        // SOME attempts (proving retries are live) without exhausting them, so
        // the post-thaw run has budget left to succeed.
        default_retry_policy: Some(RetryPolicy::fixed(20, Duration::from_millis(200))),
        default_start_to_close: None,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some(TEST_QUEUE),
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
        handler: call_downstream_handler,
    };
    Arc::new(HandlerRegistry::new(vec![wf], vec![act]))
}

// ── DB setup ─────────────────────────────────────────────────────────────────

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect")
}

async fn setup_db_url() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let mut conn = connect(&url).await;
        // Every fixture this test owns is keyed by a fixed queue/workflow name,
        // so a shared env-provided database would carry a previous run's rows
        // into this one's assertions ("zero tasks may transition to FAILED"
        // would count a *prior* run's failures). CI gets a fresh testcontainer
        // and never needs this; scrubbing keeps the env-DB path re-runnable.
        conn.batch_execute(&format!(
            "DELETE FROM harvest_queue_pauses; \
             DELETE FROM harvest_dead_letters WHERE queue_name = '{TEST_QUEUE}'; \
             DELETE FROM harvest_task_queue WHERE queue_name = '{TEST_QUEUE}'; \
             DELETE FROM harvest_events WHERE workflow_exec_id IN \
                 (SELECT id FROM harvest_workflow_executions \
                  WHERE workflow_name = 'held_workflow'); \
             DELETE FROM harvest_workflow_executions WHERE workflow_name = 'held_workflow';"
        ))
        .await
        .expect("scrub this test's fixtures");
        return (url, None);
    }
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
    let mut conn = connect(&url).await;
    conn.batch_execute(autumn_harvest::full_migrations_sql())
        .await
        .expect("migrations");
    (url, Some(container))
}

// ── counters ─────────────────────────────────────────────────────────────────

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

async fn scalar(conn: &mut AsyncPgConnection, sql: &str, queue: &str) -> i64 {
    diesel::sql_query(sql)
        .bind::<diesel::sql_types::Text, _>(queue)
        .get_result::<CountRow>(conn)
        .await
        .expect("count query")
        .count
}

async fn tasks_in_state(conn: &mut AsyncPgConnection, queue: &str, state: &str) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*) AS count FROM harvest_task_queue WHERE queue_name = $1 AND state = $2",
    )
    .bind::<diesel::sql_types::Text, _>(queue)
    .bind::<diesel::sql_types::Text, _>(state)
    .get_result::<CountRow>(conn)
    .await
    .expect("count query")
    .count
}

async fn dlq_entries(conn: &mut AsyncPgConnection, queue: &str) -> i64 {
    scalar(
        conn,
        "SELECT COUNT(*) AS count FROM harvest_dead_letters WHERE queue_name = $1",
        queue,
    )
    .await
}

async fn executions_in_state(conn: &mut AsyncPgConnection, state: &str) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*) AS count FROM harvest_workflow_executions \
         WHERE workflow_name = 'held_workflow' AND state = $1",
    )
    .bind::<diesel::sql_types::Text, _>(state)
    .get_result::<CountRow>(conn)
    .await
    .expect("count query")
    .count
}

async fn start_one(url: &str, n: usize) -> ExecutionId {
    let exec_id = ExecutionId::new();
    let mut conn = connect(url).await;
    let workflow_id = format!("qp-success-{n}-{}", uuid::Uuid::new_v4().simple());
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "held_workflow",
            workflow_id: &workflow_id,
            exec_id,
            input: json!({ "n": n }),
            parent_id: None,
            queue_name: TEST_QUEUE,
            execution_timeout: None,
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            inherited_chain_deadline_at: None,
            memo: None,
            search_attrs: None,
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            conflict_policy: WorkflowIdConflictPolicy::Unspecified,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            concurrency_key: None,
            concurrency_limit: None,
            concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
            priority: Priority::default(),
            max_workflow_input_bytes: 0,
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
            start_source: StartSource::Api,
            start_source_ref: None,
            started_by: None,
        },
        None,
    )
    .await
    .expect("start workflow");
    exec_id
}

// ── the success metric ───────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_holds_a_failing_backlog_and_every_task_succeeds_after_thaw() {
    DOWNSTREAM_UP.store(false, Ordering::SeqCst);
    ATTEMPTS_RUN.store(0, Ordering::SeqCst);

    let (url, _container) = setup_db_url().await;
    let pool = build_test_pool(&url);
    let mut conn = connect(&url).await;

    // A live worker polling the queue for the whole test.
    let worker = build_runtime_worker_on_queue("qp-success-worker", registry());
    let handle = spawn_test_worker(worker, pool.clone());

    // ── 1. Downstream is DOWN. Start the backlog and let it genuinely fail. ──
    for n in 0..WORKFLOWS {
        start_one(&url, n).await;
    }

    // Wait until real attempts have run and failed — this is what proves the
    // retry machinery is live and that this backlog would otherwise march to
    // FAILED/DLQ if left alone.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while AtomicUsize::load(&ATTEMPTS_RUN, Ordering::SeqCst) < WORKFLOWS
        && std::time::Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let attempts_before_pause = AtomicUsize::load(&ATTEMPTS_RUN, Ordering::SeqCst);
    assert!(
        attempts_before_pause >= WORKFLOWS,
        "the outage must produce real, failing attempts before the pause -- \
         otherwise the test would pass trivially on a queue that never ran \
         anything (ran {attempts_before_pause}, wanted >= {WORKFLOWS})"
    );

    // ── 2. The operator pauses the queue. ────────────────────────────────────
    queue_pause::pause_queue(
        &mut conn,
        TEST_QUEUE,
        "downstream provider outage",
        "alice@ops",
        None,
    )
    .await
    .expect("pause");
    // Read the counter AFTER the pause commits. An attempt already in flight
    // when the pause landed may still finish (AC2: a pause holds dispatch, it
    // does not abort running work), so this is the honest baseline for
    // "nothing NEW was claimed".
    tokio::time::sleep(Duration::from_millis(500)).await;
    let attempts_at_hold = AtomicUsize::load(&ATTEMPTS_RUN, Ordering::SeqCst);

    // ── 3. Hold across several worker poll cycles. ───────────────────────────
    tokio::time::sleep(Duration::from_secs(3)).await;

    // AC3 + the success metric's "zero FAILED/DLQ during the pause window".
    assert_eq!(
        AtomicUsize::load(&ATTEMPTS_RUN, Ordering::SeqCst),
        attempts_at_hold,
        "NO activity attempt may be claimed while the queue is held -- a leaked \
         claim path would burn retry budget during exactly the outage the hold \
         exists to ride out"
    );
    assert_eq!(
        dlq_entries(&mut conn, TEST_QUEUE).await,
        0,
        "zero tasks may reach the DLQ during the pause window"
    );
    assert_eq!(
        tasks_in_state(&mut conn, TEST_QUEUE, "FAILED").await,
        0,
        "zero tasks may transition to FAILED during the pause window"
    );
    assert_eq!(
        executions_in_state(&mut conn, "FAILED").await,
        0,
        "zero workflows may fail during the pause window"
    );
    let held = tasks_in_state(&mut conn, TEST_QUEUE, "PENDING").await;
    assert!(
        held > 0,
        "the backlog must still be waiting as PENDING while held (found {held})"
    );

    // The read model agrees with the claim gate.
    let listed = queue_pause::list_paused_queues(&mut conn)
        .await
        .expect("list");
    let entry = listed
        .iter()
        .find(|q| q.queue_name == TEST_QUEUE)
        .expect("the held queue must be listed");
    assert_eq!(entry.reason, "downstream provider outage");
    assert_eq!(entry.held_task_count, held);

    // ── 4. Downstream recovers; the operator resumes. ────────────────────────
    DOWNSTREAM_UP.store(true, Ordering::SeqCst);
    let outcome = queue_pause::resume_queue(&mut conn, TEST_QUEUE, "alice@ops")
        .await
        .expect("resume");
    assert!(outcome.newly_resumed);

    // ── 5. 100% of the held work reaches a SUCCESSFUL terminal outcome. ──────
    let want_completed = i64::try_from(WORKFLOWS).expect("fixture size fits in i64");
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let completed = executions_in_state(&mut conn, "COMPLETED").await;
        if completed == want_completed {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "after the thaw every held workflow must reach COMPLETED: \
             {completed}/{WORKFLOWS} completed, \
             {} failed, {} still pending, {} in the DLQ",
            executions_in_state(&mut conn, "FAILED").await,
            tasks_in_state(&mut conn, TEST_QUEUE, "PENDING").await,
            dlq_entries(&mut conn, TEST_QUEUE).await,
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    assert_eq!(
        executions_in_state(&mut conn, "FAILED").await,
        0,
        "no workflow may end FAILED -- 100% of held tasks resume to a \
         successful terminal outcome"
    );
    assert_eq!(
        dlq_entries(&mut conn, TEST_QUEUE).await,
        0,
        "the DLQ must be empty: a pause never dead-letters work"
    );

    handle.abort();
}

/// The shared `build_runtime_worker` helper binds the `"default"` queue; this
/// test needs the worker to poll its own uniquely-named queue so a sibling
/// suite sharing an env-provided database cannot claim its tasks (and so the
/// pause under test is scoped to work this test owns).
fn build_runtime_worker_on_queue(
    worker_id: &str,
    registry: Arc<HandlerRegistry>,
) -> Arc<autumn_harvest::worker::Worker> {
    let mut config = runtime_config(worker_id, 4, 4, Duration::from_secs(30));
    config.queues = vec![TEST_QUEUE.to_string()];
    Arc::new(autumn_harvest::worker::Worker::new(config, registry).expect("worker should build"))
}
