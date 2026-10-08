#![cfg(feature = "db")]
// Test-code style lints (consistent with the other integration test files).
#![allow(
    clippy::doc_markdown,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::items_after_statements,
    clippy::default_trait_access,
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::unused_async
)]
//! Post-claim workflow-execution-pause re-check — issue #1640.
//!
//! `apply_post_claim_rechecks` (`queue.rs`) already re-checks queue pause
//! (#619) and activity pause (#807) in a fresh statement after a claim's own
//! `UPDATE` commits its snapshot. It did not re-check workflow-execution
//! pause. The claim query's own anti-join excludes a `PAUSED` execution's
//! workflow task, but only against *that statement's* snapshot.
//!
//! A `pause_workflow_execution` commit can land in the window between that
//! snapshot and the post-claim re-check. Both stayed blind to it. The task
//! then still dispatched into a pause already acknowledged to the operator.
//!
//! These tests prove the gap (would fail before the fix) and the fix.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly; otherwise a fresh testcontainers Postgres is booted
//! with the full migration bundle.

use autumn_harvest::execution::{self, release_claim_if_workflow_paused};
use autumn_harvest::queue::{self, EnqueueParams, TaskType, claim_task};
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::types::ExecutionId;
use diesel_async::AsyncPgConnection;
use diesel_async::SimpleAsyncConnection;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// ── DB setup ──────────────────────────────────────────────────────────────────

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as diesel_async::AsyncConnection>::establish(url)
        .await
        .expect("connect")
}

/// A migrated Postgres 16 — the env URL when set, else a fresh testcontainer.
async fn setup_db_url() -> (String, Option<ContainerAsync<Postgres>>) {
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
    let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
    let mut conn = connect(&url).await;
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("migrations");
    (url, Some(container))
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn unique_queue(prefix: &str) -> String {
    format!("{prefix}-{}", Uuid::new_v4().simple())
}

async fn insert_execution(conn: &mut AsyncPgConnection) -> Uuid {
    use diesel_async::RunQueryDsl;
    let id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, shard_id, input) \
         VALUES ($1, 'wp', $2, 0, '{}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .bind::<diesel::sql_types::Text, _>(id.to_string())
    .execute(conn)
    .await
    .expect("insert execution");
    id
}

/// Enqueue a workflow-type task owned by `exec_id`.
async fn enqueue_workflow_task(
    conn: &mut AsyncPgConnection,
    queue: &str,
    exec_id: Uuid,
    rate_limit_key: Option<&str>,
) -> Uuid {
    let mut params = EnqueueParams::new(queue, TaskType::Workflow, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.rate_limit_key = rate_limit_key.map(ToString::to_string);
    queue::enqueue(conn, &params).await.expect("enqueue")
}

/// Enqueue an activity task owned by `exec_id` (for the scope-guard test).
async fn enqueue_activity_task(conn: &mut AsyncPgConnection, queue: &str, exec_id: Uuid) -> Uuid {
    let mut params = EnqueueParams::new(queue, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = Some("noop".to_string());
    params.activity_id = Some(Uuid::new_v4());
    queue::enqueue(conn, &params).await.expect("enqueue")
}

async fn claim_one(conn: &mut AsyncPgConnection, queue: &str) -> Option<Uuid> {
    claim_task(conn, &[queue.to_string()], "w1", "", None, &[], &[])
        .await
        .expect("claim")
        .map(|t| t.id)
}

async fn pause(conn: &mut AsyncPgConnection, exec_id: Uuid) {
    execution::pause_workflow_execution(
        conn,
        ExecutionId::from_uuid(exec_id),
        Some("outage"),
        "alice",
        &NoOpMetrics,
    )
    .await
    .expect("pause");
}

struct TaskRow {
    state: String,
    attempt: i32,
    worker_id: Option<String>,
    started_at: Option<chrono::DateTime<chrono::Utc>>,
}

async fn load_task(conn: &mut AsyncPgConnection, id: Uuid) -> TaskRow {
    use diesel_async::RunQueryDsl;
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
        #[diesel(sql_type = diesel::sql_types::Integer)]
        attempt: i32,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        worker_id: Option<String>,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
        started_at: Option<chrono::DateTime<chrono::Utc>>,
    }
    let row: Row = diesel::sql_query(
        "SELECT state, attempt, worker_id, started_at FROM harvest_task_queue WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .get_result(conn)
    .await
    .expect("load task");
    TaskRow {
        state: row.state,
        attempt: row.attempt,
        worker_id: row.worker_id,
        started_at: row.started_at,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// A claim that beat the pause must be released once a fresh statement sees
/// the pause. Its `attempt` must be restored: no retry budget is consumed.
#[tokio::test]
async fn a_claim_that_beat_the_workflow_pause_is_released_with_its_attempt_restored() {
    let (url, _c) = setup_db_url().await;
    let mut conn = connect(&url).await;
    let q = unique_queue("wp-release");

    let exec_id = insert_execution(&mut conn).await;
    let task_id = enqueue_workflow_task(&mut conn, &q, exec_id, None).await;
    assert_eq!(
        claim_one(&mut conn, &q).await,
        Some(task_id),
        "the unpaused claim must succeed"
    );

    // The pause commits after the claim -- exactly the state the post-claim
    // re-check is reached with when a claim's snapshot predates the pause.
    pause(&mut conn, exec_id).await;

    let released = release_claim_if_workflow_paused(&mut conn, task_id, "w1")
        .await
        .expect("release");
    assert!(
        released,
        "a claim on a now-paused workflow execution must be released"
    );

    let row = load_task(&mut conn, task_id).await;
    assert_eq!(
        row.state, "PENDING",
        "the task must be held, not dispatched"
    );
    assert_eq!(
        row.attempt, 0,
        "a hold must consume no retry budget -- the claim's attempt increment \
         has to be given back"
    );
    assert!(row.worker_id.is_none(), "the claim must be fully undone");
    assert!(row.started_at.is_none(), "the claim must be fully undone");
}

/// The re-check must not disturb an ordinary claim: with no pause in effect it
/// is a no-op and the task stays `RUNNING`.
#[tokio::test]
async fn an_ordinary_claim_is_not_released_when_the_workflow_is_not_paused() {
    let (url, _c) = setup_db_url().await;
    let mut conn = connect(&url).await;
    let q = unique_queue("wp-noop");

    let exec_id = insert_execution(&mut conn).await;
    let task_id = enqueue_workflow_task(&mut conn, &q, exec_id, None).await;
    assert_eq!(claim_one(&mut conn, &q).await, Some(task_id));

    let released = release_claim_if_workflow_paused(&mut conn, task_id, "w1")
        .await
        .expect("release");
    assert!(
        !released,
        "an unpaused workflow execution's claim must never be rolled back"
    );

    let row = load_task(&mut conn, task_id).await;
    assert_eq!(row.state, "RUNNING");
    assert_eq!(row.attempt, 1, "the ordinary claim's attempt must stand");
}

/// Scope guard: `pause_workflow_execution` holds new *workflow* dispatch only.
/// An already-`PENDING` activity task owned by the same (now paused) execution
/// must still be claimable. In-flight and not-yet-dispatched activities are
/// not what this hold blocks. See `pause_workflow_execution`'s doc comment.
#[tokio::test]
async fn an_activity_task_of_a_paused_workflow_is_not_held_by_this_recheck() {
    let (url, _c) = setup_db_url().await;
    let mut conn = connect(&url).await;
    let q = unique_queue("wp-act");

    let exec_id = insert_execution(&mut conn).await;
    let task_id = enqueue_activity_task(&mut conn, &q, exec_id).await;

    pause(&mut conn, exec_id).await;

    assert_eq!(
        claim_one(&mut conn, &q).await,
        Some(task_id),
        "a workflow pause must not collaterally hold the execution's activity tasks"
    );

    let released = release_claim_if_workflow_paused(&mut conn, task_id, "w1")
        .await
        .expect("release");
    assert!(
        !released,
        "the workflow-pause re-check must never touch an activity-type row"
    );
}

/// Issue #1640's exact race, reproduced deterministically: a pause committing
/// while a claim statement is already in flight must still hold the task.
///
/// `claim_task` is a single autocommit statement, so under `READ COMMITTED`
/// its anti-join runs against one snapshot taken at statement start.
///
/// Stalling the claim mid-statement on its rate-limit debit (the one part of
/// the CTE that can block) lets a pause commit inside that window. The
/// post-claim re-check is a fresh statement, so it must then catch it.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_workflow_pause_committed_mid_claim_still_holds_the_task() {
    use diesel_async::RunQueryDsl;

    let (url, _c) = setup_db_url().await;
    let mut conn = connect(&url).await;
    let q = unique_queue("wp-mid");
    let rl_key = format!("{q}-bucket");

    queue::ensure_rate_limit_bucket(&mut conn, &rl_key, 100.0, 100.0)
        .await
        .expect("bucket");

    let exec_id = insert_execution(&mut conn).await;
    let task_id = enqueue_workflow_task(&mut conn, &q, exec_id, Some(&rl_key)).await;

    // Hold the rate-limit bucket row so the claim's debit blocks mid-statement,
    // freezing its snapshot from before the pause.
    let mut bucket_holder = connect(&url).await;
    bucket_holder.batch_execute("BEGIN").await.expect("begin");
    diesel::sql_query(
        "UPDATE harvest_rate_limit_buckets SET last_refilled_at = last_refilled_at WHERE key = $1",
    )
    .bind::<diesel::sql_types::Text, _>(&rl_key)
    .execute(&mut bucket_holder)
    .await
    .expect("hold the bucket row");

    let url_for_claim = url.clone();
    let q_for_claim = q.clone();
    let claim_in_flight = tokio::spawn(async move {
        let mut conn = connect(&url_for_claim).await;
        claim_task(&mut conn, &[q_for_claim], "w1", "", None, &[], &[])
            .await
            .expect("claim")
            .map(|t| t.id)
    });

    // Let the claim reach (and block on) the bucket row.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    pause(&mut conn, exec_id).await;
    bucket_holder
        .batch_execute("COMMIT")
        .await
        .expect("release the bucket row");

    let claimed = claim_in_flight.await.expect("join");
    assert!(
        claimed.is_none(),
        "a claim whose snapshot predated the pause must NOT be handed to the \
         worker -- the post-claim re-check has to release it, or the task is \
         dispatched into exactly the pause it exists to ride out"
    );

    let row = load_task(&mut conn, task_id).await;
    assert_eq!(row.state, "PENDING", "the task must be held");
    assert_eq!(row.attempt, 0, "the hold must consume no retry budget");
}
