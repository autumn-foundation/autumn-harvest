#![cfg(feature = "db")]
//! Shared contract of the three workflow-task backoff requeues (issue #1751).
//!
//! `requeue_workflow_task_nd_blocked`, `requeue_workflow_task_after_panic`, and
//! `requeue_workflow_task_for_quota_retry` all re-pend a claimed workflow task.
//! This file pins what they share and the one thing that differs: ND-block and
//! panic release sticky affinity, quota retry keeps it.

use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use chrono::Duration;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as diesel_async::AsyncConnection>::establish(url)
        .await
        .expect("connect")
}

async fn setup_db() -> (AsyncPgConnection, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (connect(&url).await, None);
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
    (conn, Some(container))
}

/// The three requeue entry points under test.
#[derive(Clone, Copy, Debug)]
enum Path {
    NdBlocked,
    AfterPanic,
    QuotaRetry,
}

const ALL_PATHS: [Path; 3] = [Path::NdBlocked, Path::AfterPanic, Path::QuotaRetry];

impl Path {
    const fn releases_sticky(self) -> bool {
        !matches!(self, Self::QuotaRetry)
    }

    async fn run(self, conn: &mut AsyncPgConnection, task: Uuid, delay: Duration, reason: &str) {
        match self {
            Self::NdBlocked => queue::requeue_workflow_task_nd_blocked(conn, task, delay, reason)
                .await
                .expect("requeue nd-blocked"),
            Self::AfterPanic => {
                queue::requeue_workflow_task_after_panic(conn, task, delay, reason)
                    .await
                    .expect("requeue after panic");
            }
            Self::QuotaRetry => {
                queue::requeue_workflow_task_for_quota_retry(conn, task, delay, reason)
                    .await
                    .expect("requeue quota retry");
            }
        }
    }
}

async fn insert_execution(conn: &mut AsyncPgConnection) -> Uuid {
    let id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, shard_id, input) \
         VALUES ($1, 'backoff-requeue', $2, 0, '{}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .bind::<diesel::sql_types::Text, _>(id.to_string())
    .execute(conn)
    .await
    .expect("insert execution");
    id
}

/// Enqueue and claim a workflow task, then stamp every column the requeue
/// may clear.
async fn claimed_task_with_stale_markers(conn: &mut AsyncPgConnection) -> Uuid {
    let queue_name = format!("backoff-{}", Uuid::new_v4().simple());
    let exec_id = insert_execution(conn).await;
    let mut params = EnqueueParams::new(&queue_name, TaskType::Workflow, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    let task = queue::enqueue(conn, &params).await.expect("enqueue");
    let claimed = queue::claim_task(conn, &[queue_name], "w1", "", None, &[], &[])
        .await
        .expect("claim")
        .expect("a task must be claimable");
    assert_eq!(claimed.id, task);
    diesel::sql_query(
        "UPDATE harvest_task_queue SET sticky_worker_id = 'w1', \
         sticky_until = clock_timestamp() + interval '1 hour', \
         sticky_timeout = interval '30 seconds', wake_requested = TRUE, \
         activity_name = 'mixed_signal_suspension', \
         timer_fires_at = clock_timestamp(), crash_strikes = 2 WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task)
    .execute(conn)
    .await
    .expect("stamp markers");
    task
}

// One bool per column the requeue may clear.
#[allow(clippy::struct_excessive_bools)]
#[derive(diesel::QueryableByName, Debug)]
struct RowState {
    #[diesel(sql_type = diesel::sql_types::Text)]
    state: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    worker_id: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    error: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    crash_strikes: i32,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    wake_requested: bool,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    activity_name: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    timer_cleared: bool,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    sticky_worker_id: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    sticky_until_set: bool,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    sticky_timeout_set: bool,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    backoff_in_future: bool,
}

async fn read_row(conn: &mut AsyncPgConnection, task: Uuid) -> RowState {
    diesel::sql_query(
        "SELECT state, worker_id, error, crash_strikes, wake_requested, activity_name, \
         timer_fires_at IS NULL AS timer_cleared, sticky_worker_id, \
         sticky_until IS NOT NULL AS sticky_until_set, \
         sticky_timeout IS NOT NULL AS sticky_timeout_set, \
         scheduled_at > clock_timestamp() AS backoff_in_future \
         FROM harvest_task_queue WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task)
    .get_result::<RowState>(conn)
    .await
    .expect("read row")
}

/// Every path re-pends the row, resets the claim, and clears the stale
/// markers that could defeat the backoff.
#[tokio::test]
async fn every_backoff_requeue_repends_and_clears_the_shared_markers() {
    let (mut conn, _c) = setup_db().await;
    for path in ALL_PATHS {
        let task = claimed_task_with_stale_markers(&mut conn).await;

        path.run(&mut conn, task, Duration::seconds(60), "why")
            .await;

        let row = read_row(&mut conn, task).await;
        assert_eq!(row.state, "PENDING", "{path:?}");
        assert_eq!(row.worker_id, None, "{path:?}");
        assert_eq!(row.error.as_deref(), Some("why"), "{path:?}");
        assert_eq!(row.crash_strikes, 0, "{path:?}");
        assert!(!row.wake_requested, "{path:?}");
        assert_eq!(row.activity_name, None, "{path:?}");
        assert!(row.timer_cleared, "{path:?}");
        assert!(row.backoff_in_future, "{path:?}");
    }
}

/// ND-block and panic release sticky affinity. Quota retry keeps it, because
/// a quota rejection is not a worker failure.
#[tokio::test]
async fn only_quota_retry_keeps_sticky_affinity() {
    let (mut conn, _c) = setup_db().await;
    for path in ALL_PATHS {
        let task = claimed_task_with_stale_markers(&mut conn).await;

        path.run(&mut conn, task, Duration::seconds(60), "why")
            .await;

        let row = read_row(&mut conn, task).await;
        let released =
            row.sticky_worker_id.is_none() && !row.sticky_until_set && !row.sticky_timeout_set;
        let kept = row.sticky_worker_id.as_deref() == Some("w1")
            && row.sticky_until_set
            && row.sticky_timeout_set;
        if path.releases_sticky() {
            assert!(released, "{path:?} must release sticky: {row:?}");
        } else {
            assert!(kept, "{path:?} must keep sticky: {row:?}");
        }
    }
}

/// A task that is not a claimed workflow task reports `NotFound` on every path.
#[tokio::test]
async fn every_backoff_requeue_reports_not_found_for_an_unclaimed_task() {
    let (mut conn, _c) = setup_db().await;
    for path in ALL_PATHS {
        let result = match path {
            Path::NdBlocked => {
                queue::requeue_workflow_task_nd_blocked(
                    &mut conn,
                    Uuid::new_v4(),
                    Duration::seconds(1),
                    "x",
                )
                .await
            }
            Path::AfterPanic => {
                queue::requeue_workflow_task_after_panic(
                    &mut conn,
                    Uuid::new_v4(),
                    Duration::seconds(1),
                    "x",
                )
                .await
            }
            Path::QuotaRetry => {
                queue::requeue_workflow_task_for_quota_retry(
                    &mut conn,
                    Uuid::new_v4(),
                    Duration::seconds(1),
                    "x",
                )
                .await
            }
        };
        assert!(
            matches!(
                result,
                Err(autumn_harvest::error::HarvestError::NotFound(_))
            ),
            "{path:?}: {result:?}"
        );
    }
}

/// A `PENDING` task is not claimed, so every path reports `NotFound` and
/// leaves the row untouched.
#[tokio::test]
async fn every_backoff_requeue_rejects_a_pending_task() {
    let (mut conn, _c) = setup_db().await;
    for path in ALL_PATHS {
        let queue_name = format!("backoff-{}", Uuid::new_v4().simple());
        let exec_id = insert_execution(&mut conn).await;
        let mut params = EnqueueParams::new(&queue_name, TaskType::Workflow, serde_json::json!({}));
        params.workflow_exec_id = Some(exec_id);
        let task = queue::enqueue(&mut conn, &params).await.expect("enqueue");

        let result = match path {
            Path::NdBlocked => {
                queue::requeue_workflow_task_nd_blocked(&mut conn, task, Duration::seconds(60), "x")
                    .await
            }
            Path::AfterPanic => {
                queue::requeue_workflow_task_after_panic(
                    &mut conn,
                    task,
                    Duration::seconds(60),
                    "x",
                )
                .await
            }
            Path::QuotaRetry => {
                queue::requeue_workflow_task_for_quota_retry(
                    &mut conn,
                    task,
                    Duration::seconds(60),
                    "x",
                )
                .await
            }
        };

        assert!(
            matches!(
                result,
                Err(autumn_harvest::error::HarvestError::NotFound(_))
            ),
            "{path:?}: {result:?}"
        );
        assert!(
            !read_row(&mut conn, task).await.backoff_in_future,
            "{path:?} must not defer a task it rejected"
        );
    }
}

#[derive(diesel::QueryableByName)]
struct Attempt {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    attempt: i32,
}

/// Issue #1815: the claim-fenced panic requeue writes only under the current
/// claim. A stale dispatcher leaves a peer's claim alone and reports it.
#[tokio::test]
async fn the_fenced_panic_requeue_leaves_a_peer_claim_alone() {
    let (mut conn, _c) = setup_db().await;
    let task = claimed_task_with_stale_markers(&mut conn).await;
    let attempt = diesel::sql_query("SELECT attempt FROM harvest_task_queue WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task)
        .get_result::<Attempt>(&mut conn)
        .await
        .expect("read attempt")
        .attempt;

    let stale = queue::TaskClaim::new(task, "w0", attempt);
    let applied = queue::requeue_claimed_workflow_task_after_panic(
        &mut conn,
        &stale,
        Duration::seconds(60),
        "stale",
    )
    .await
    .expect("stale requeue");
    assert!(!applied, "a stale claim must not apply");
    let row = read_row(&mut conn, task).await;
    assert_eq!(row.state, "RUNNING");
    assert_eq!(row.worker_id.as_deref(), Some("w1"));
    assert!(
        !row.backoff_in_future,
        "the peer's claim must not be deferred"
    );

    let current = queue::TaskClaim::new(task, "w1", attempt);
    let applied = queue::requeue_claimed_workflow_task_after_panic(
        &mut conn,
        &current,
        Duration::seconds(60),
        "why",
    )
    .await
    .expect("current requeue");
    assert!(applied, "the current claim must apply");
    let row = read_row(&mut conn, task).await;
    assert_eq!(row.state, "PENDING");
    assert_eq!(row.worker_id, None);
    assert_eq!(row.error.as_deref(), Some("why"));
    assert_eq!(row.crash_strikes, 0);
    assert!(row.backoff_in_future);
    assert_eq!(
        row.sticky_worker_id, None,
        "a panic releases sticky affinity"
    );
}
