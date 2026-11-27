//! The batch form of the default claim.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run against it.
//! Otherwise a fresh testcontainers Postgres boots with the migrations.

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::prelude::*;
use autumn_harvest::queue::{self, ClaimBatch, EnqueueParams, SlottedBatch, TaskType};
use autumn_harvest::telemetry::{ClaimKind, MetricsRecorder, TelemetryConfig};
use autumn_harvest::types::ShardId;
use autumn_harvest::worker::{Worker, WorkerRuntimeConfig};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

use crate::decision_owner_claim_tests::OwnDb;
use crate::integration_e2e::{build_test_pool, wait_for_execution_state_with_timeout};
use crate::lean_claim_db_tests as lean;

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url).await.expect("connect")
}

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
    connect(&url)
        .await
        .batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("migrations");
    (url, Some(container))
}

fn unique_queue(prefix: &str) -> String {
    format!("{prefix}-{}", &Uuid::new_v4().simple().to_string()[..8])
}

struct Row {
    kind: TaskType,
    priority: i32,
    key: Option<(&'static str, u32)>,
}

const fn plain(priority: i32) -> Row {
    Row {
        kind: TaskType::Activity,
        priority,
        key: None,
    }
}

const fn keyed(priority: i32, key: &'static str, cap: u32) -> Row {
    Row {
        kind: TaskType::Activity,
        priority,
        key: Some((key, cap)),
    }
}

async fn enqueue(conn: &mut AsyncPgConnection, queue: &str, row: &Row) -> Uuid {
    let mut params = EnqueueParams::new(queue, row.kind, serde_json::json!({}));
    if row.kind == TaskType::Activity {
        params.activity_name = Some("noop".to_owned());
        params.activity_id = Some(Uuid::new_v4());
    }
    params.priority = row.priority;
    if let Some((key, cap)) = row.key {
        params.concurrency_key = Some(format!("{queue}-{key}"));
        params.max_concurrent = Some(cap);
    }
    queue::enqueue(conn, &params).await.expect("enqueue")
}

async fn claim(
    conn: &mut AsyncPgConnection,
    queue: &str,
    worker: &str,
    kind: Option<TaskType>,
    batch: ClaimBatch,
) -> Vec<(Uuid, i32)> {
    let claimed = queue::claim_task_batch_with_slots(
        conn,
        &[queue.to_owned()],
        worker,
        "",
        &[],
        &[],
        None,
        kind,
        None,
        batch,
    )
    .await
    .expect("batch claim");
    match claimed {
        SlottedBatch::Claimed(tasks) => {
            assert!(tasks.len() <= batch.rows(), "a batch stays at its size");
            let mut rows: Vec<_> = tasks
                .into_iter()
                .map(|task| {
                    assert_eq!(task.state, "RUNNING");
                    assert_eq!(task.worker_id.as_deref(), Some(worker));
                    assert_eq!(task.attempt, 1);
                    (task.id, task.priority)
                })
                .collect();
            rows.sort_by_key(|row| std::cmp::Reverse(row.1));
            rows
        }
        SlottedBatch::Empty => Vec::new(),
        SlottedBatch::NoSlot => panic!("no claim slots were asked for"),
    }
}

fn priorities(rows: &[(Uuid, i32)]) -> Vec<i32> {
    rows.iter().map(|row| row.1).collect()
}

async fn running_count(conn: &mut AsyncPgConnection, queue: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_task_queue WHERE queue_name = $1 AND state = 'RUNNING'",
    )
    .bind::<diesel::sql_types::Text, _>(queue)
    .get_result::<Count>(conn)
    .await
    .expect("count")
    .n
}

#[test]
fn a_batch_size_rounds_down_to_two_four_or_eight() {
    let sizes: Vec<_> = (0..=9).map(ClaimBatch::at_most).collect();
    assert_eq!(
        sizes,
        [
            None,
            None,
            Some(ClaimBatch::Two),
            Some(ClaimBatch::Two),
            Some(ClaimBatch::Four),
            Some(ClaimBatch::Four),
            Some(ClaimBatch::Four),
            Some(ClaimBatch::Four),
            Some(ClaimBatch::Eight),
            Some(ClaimBatch::Eight),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_batch_takes_every_free_row_up_to_its_size_then_none() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique_queue("batch-all");
    for priority in 0..5 {
        enqueue(&mut conn, &queue, &plain(priority)).await;
    }
    let first = claim(&mut conn, &queue, "w-all", None, ClaimBatch::Eight).await;
    assert_eq!(priorities(&first), [4, 3, 2, 1, 0]);
    assert_eq!(
        claim(&mut conn, &queue, "w-all", None, ClaimBatch::Eight).await,
        Vec::new()
    );
    assert_eq!(running_count(&mut conn, &queue).await, 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_batch_takes_the_rows_a_run_of_single_claims_takes() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique_queue("batch-order");
    for priority in [1, 5, 2, 9, 3, 7] {
        enqueue(&mut conn, &queue, &plain(priority)).await;
    }
    let first = claim(&mut conn, &queue, "w-order", None, ClaimBatch::Two).await;
    assert_eq!(priorities(&first), [9, 7]);
    let second = claim(&mut conn, &queue, "w-order", None, ClaimBatch::Two).await;
    assert_eq!(priorities(&second), [5, 3]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_batch_stops_before_its_second_keyed_row() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique_queue("batch-cut");
    for row in [plain(9), plain(8), keyed(7, "k", 5), plain(6)] {
        enqueue(&mut conn, &queue, &row).await;
    }
    let first = claim(&mut conn, &queue, "w-cut", None, ClaimBatch::Eight).await;
    assert_eq!(priorities(&first), [9, 8], "the keyed row is not first");
    let second = claim(&mut conn, &queue, "w-cut", None, ClaimBatch::Eight).await;
    assert_eq!(
        priorities(&second),
        [7, 6],
        "a keyed first row leads a batch"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_batch_never_takes_two_rows_of_one_capped_key() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique_queue("batch-cap");
    for row in [keyed(9, "k", 1), keyed(8, "k", 1), keyed(7, "k", 1)] {
        enqueue(&mut conn, &queue, &row).await;
    }
    let first = claim(&mut conn, &queue, "w-cap", None, ClaimBatch::Eight).await;
    assert_eq!(priorities(&first), [9]);
    assert_eq!(
        claim(&mut conn, &queue, "w-cap", None, ClaimBatch::Eight).await,
        Vec::new(),
        "the key is at its cap"
    );
    assert_eq!(running_count(&mut conn, &queue).await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_kind_batch_takes_only_rows_of_its_kind() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique_queue("batch-kind");
    for priority in 0..3 {
        enqueue(&mut conn, &queue, &plain(priority)).await;
        enqueue(
            &mut conn,
            &queue,
            &Row {
                kind: TaskType::Workflow,
                priority: 10 + priority,
                key: None,
            },
        )
        .await;
    }
    let activities = claim(
        &mut conn,
        &queue,
        "w-kind",
        Some(TaskType::Activity),
        ClaimBatch::Eight,
    )
    .await;
    assert_eq!(priorities(&activities), [2, 1, 0]);
    let workflows = claim(
        &mut conn,
        &queue,
        "w-kind",
        Some(TaskType::Workflow),
        ClaimBatch::Four,
    )
    .await;
    assert_eq!(priorities(&workflows), [12, 11, 10]);
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_batches_take_each_row_exactly_once() {
    const ROWS: usize = 60;
    const CLAIMERS: usize = 6;
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique_queue("batch-race");
    let mut ids = HashSet::new();
    for i in 0..ROWS {
        let row = if i % 10 == 0 {
            keyed(0, "race", 3)
        } else {
            plain(0)
        };
        ids.insert(enqueue(&mut conn, &queue, &row).await);
    }
    let mut claimers = Vec::new();
    for claimer in 0..CLAIMERS {
        let url = url.clone();
        let queue = queue.clone();
        claimers.push(tokio::spawn(async move {
            let mut conn = connect(&url).await;
            let worker = format!("w-race-{claimer}");
            let mut taken = Vec::new();
            let mut empty = 0;
            while empty < 3 {
                let rows = claim(&mut conn, &queue, &worker, None, ClaimBatch::Four).await;
                if rows.is_empty() {
                    empty += 1;
                }
                taken.extend(rows.into_iter().map(|row| row.0));
            }
            taken
        }));
    }
    let mut taken = Vec::new();
    for claimer in claimers {
        taken.extend(claimer.await.expect("claimer"));
    }
    loop {
        let rows = claim(&mut conn, &queue, "w-race-drain", None, ClaimBatch::Four).await;
        if rows.is_empty() {
            break;
        }
        taken.extend(rows.into_iter().map(|row| row.0));
    }
    let distinct: HashSet<_> = taken.iter().copied().collect();
    assert_eq!(distinct.len(), taken.len(), "no row is claimed twice");
    assert!(distinct.is_subset(&ids));
    assert_eq!(
        taken.len(),
        ROWS - 3,
        "every plain row and three keyed rows, the cap"
    );
}

/// A run whose deadline passes during the claim stops its own row only.
///
/// The first row spends a token from a bucket that a peer holds locked, so
/// the claim reads its clock after the run deadline. The rows behind it have
/// no run, and the claim still takes them.
#[tokio::test(flavor = "multi_thread")]
async fn a_run_that_expires_during_the_claim_stops_only_its_own_row() {
    #[derive(diesel::QueryableByName)]
    struct State {
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
        #[diesel(sql_type = diesel::sql_types::Integer)]
        attempt: i32,
    }
    #[derive(diesel::QueryableByName)]
    struct Tokens {
        #[diesel(sql_type = diesel::sql_types::Double)]
        tokens: f64,
    }
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique_queue("batch-expiry");
    let bucket = unique_queue("batch-expiry-bucket");
    queue::ensure_rate_limit_bucket(&mut conn, &bucket, 0.0, 1.0)
        .await
        .expect("bucket");

    let run = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, shard_id, input, state, deadline_at) \
         VALUES ($1, 'batch-expiry', $2, 0, '{}'::jsonb, 'RUNNING', \
                 NOW() + INTERVAL '2 seconds')",
    )
    .bind::<diesel::sql_types::Uuid, _>(run)
    .bind::<diesel::sql_types::Text, _>(run.to_string())
    .execute(&mut conn)
    .await
    .expect("insert run");
    let mut params = EnqueueParams::new(&queue, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(run);
    params.activity_name = Some("noop".to_owned());
    params.activity_id = Some(Uuid::new_v4());
    params.rate_limit_key = Some(bucket.clone());
    params.priority = 9;
    let expiring = queue::enqueue(&mut conn, &params).await.expect("enqueue");
    for priority in [3, 2, 1] {
        enqueue(&mut conn, &queue, &plain(priority)).await;
    }

    let mut holder = connect(&url).await;
    holder.batch_execute("BEGIN").await.expect("begin holder");
    diesel::sql_query("SELECT 1 FROM harvest_rate_limit_buckets WHERE key = $1 FOR UPDATE")
        .bind::<diesel::sql_types::Text, _>(&bucket)
        .execute(&mut holder)
        .await
        .expect("hold the bucket lock");

    let claim_url = url.clone();
    let claim_queue = queue.clone();
    let claimer = tokio::spawn(async move {
        let mut claim_conn = connect(&claim_url).await;
        claim(
            &mut claim_conn,
            &claim_queue,
            "w-expiry",
            None,
            ClaimBatch::Four,
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;
    assert!(!claimer.is_finished(), "the claim waits on the bucket lock");
    holder.batch_execute("COMMIT").await.expect("release");

    let taken = claimer.await.expect("claimer");
    assert_eq!(
        priorities(&taken),
        [3, 2, 1],
        "the other runs keep their rows"
    );
    assert!(taken.iter().all(|row| row.0 != expiring));

    let row: State =
        diesel::sql_query("SELECT state, attempt FROM harvest_task_queue WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(expiring)
            .get_result(&mut conn)
            .await
            .expect("expiring row");
    assert_eq!((row.state.as_str(), row.attempt), ("PENDING", 0));
    let tokens: Tokens =
        diesel::sql_query("SELECT tokens FROM harvest_rate_limit_buckets WHERE key = $1")
            .bind::<diesel::sql_types::Text, _>(&bucket)
            .get_result(&mut conn)
            .await
            .expect("bucket row");
    assert!(
        (tokens.tokens - 1.0).abs() < f64::EPSILON,
        "the expired row spends no token"
    );
}

/// One claim transaction, as a worker sends it, with the batch text run by
/// hand so the test can read the locks before the commit.
///
/// `NOW()` stays at the transaction start, so the statement-start gate sees
/// each run as live. The clock read after the bucket locks sees the runs that
/// the sleep expired. Row 1 carries a capped concurrency key of an expiring
/// run. Row 3 has no key and no bucket, so only the expiry check of
/// `claimed` stops it.
#[tokio::test(flavor = "multi_thread")]
async fn an_expired_row_takes_no_key_lock_and_its_peers_still_claim() {
    #[derive(diesel::QueryableByName)]
    struct Claimed {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Uuid>)]
        id: Option<Uuid>,
    }
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique_queue("batch-expiry-lock");
    conn.batch_execute("BEGIN").await.expect("begin");

    let run = |deadline: bool| {
        let id = Uuid::new_v4();
        let sql = format!(
            "INSERT INTO harvest_workflow_executions \
                 (id, workflow_name, workflow_id, shard_id, input, state, deadline_at) \
             VALUES ($1, 'batch-expiry', $2, 0, '{{}}'::jsonb, 'RUNNING', {})",
            if deadline {
                "NOW() + INTERVAL '1 second'"
            } else {
                "NULL"
            }
        );
        (id, sql)
    };
    let mut rows = Vec::new();
    for (priority, deadline, key) in [
        (9, true, true),
        (5, false, false),
        (4, true, false),
        (3, false, false),
    ] {
        let (exec, sql) = run(deadline);
        diesel::sql_query(sql)
            .bind::<diesel::sql_types::Uuid, _>(exec)
            .bind::<diesel::sql_types::Text, _>(exec.to_string())
            .execute(&mut conn)
            .await
            .expect("insert run");
        let mut params = EnqueueParams::new(&queue, TaskType::Activity, serde_json::json!({}));
        params.workflow_exec_id = Some(exec);
        params.activity_name = Some("noop".to_owned());
        params.activity_id = Some(Uuid::new_v4());
        params.priority = priority;
        if key {
            params.concurrency_key = Some(format!("{queue}-k"));
            params.max_concurrent = Some(1);
        }
        rows.push((
            priority,
            queue::enqueue(&mut conn, &params).await.expect("enqueue"),
        ));
    }
    diesel::sql_query("SELECT pg_sleep(1.5)")
        .execute(&mut conn)
        .await
        .expect("sleep");

    let claimed: Vec<Claimed> =
        diesel::sql_query(queue::batch_claim_query_for(None, false, ClaimBatch::Four))
            .bind::<diesel::sql_types::Text, _>("w-expiry-lock")
            .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(vec![queue.clone()])
            .bind::<diesel::sql_types::Text, _>("")
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::BigInt>, _>(None::<i64>)
            .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(Vec::<String>::new())
            .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(Vec::<String>::new())
            .load(&mut conn)
            .await
            .expect("batch claim");
    let taken: HashSet<Uuid> = claimed.into_iter().filter_map(|row| row.id).collect();
    let expect: HashSet<Uuid> = rows
        .iter()
        .filter(|(priority, _)| *priority == 5 || *priority == 3)
        .map(|(_, id)| *id)
        .collect();
    assert_eq!(taken, expect, "the rows of the expired runs stay PENDING");

    let locks: Count = diesel::sql_query(
        "SELECT COUNT(*) AS n FROM pg_locks \
         WHERE locktype = 'advisory' AND pid = pg_backend_pid()",
    )
    .get_result(&mut conn)
    .await
    .expect("locks");
    assert_eq!(locks.n, 0, "a skipped row holds no concurrency-key lock");
    conn.batch_execute("ROLLBACK").await.expect("rollback");
}

/// One row of the queue table, as the pause test reads it.
#[derive(diesel::QueryableByName)]
struct RowVersion {
    #[diesel(sql_type = diesel::sql_types::Text)]
    state: String,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    attempt: i32,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    worker_id: Option<String>,
    /// The transaction that wrote the visible version of the row.
    #[diesel(sql_type = diesel::sql_types::Text)]
    written_by: String,
}

async fn row_version(conn: &mut AsyncPgConnection, id: Uuid) -> RowVersion {
    diesel::sql_query(
        "SELECT state, attempt, worker_id, xmin::text AS written_by \
         FROM harvest_task_queue WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .get_result(conn)
    .await
    .expect("the row version")
}

/// The tokens of bucket `key`.
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
        .expect("bucket row")
        .tokens
}

/// The backend id of `conn`.
async fn backend_pid(conn: &mut AsyncPgConnection) -> i32 {
    #[derive(diesel::QueryableByName)]
    struct Pid {
        #[diesel(sql_type = diesel::sql_types::Integer)]
        pid: i32,
    }
    diesel::sql_query("SELECT pg_backend_pid() AS pid")
        .get_result::<Pid>(conn)
        .await
        .expect("read the backend pid")
        .pid
}

/// Waits until the statement of backend `pid` waits for a lock.
async fn wait_for_a_statement_at_a_lock(observer: &mut AsyncPgConnection, pid: i32) {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    for _ in 0..5_000 {
        let waiting = diesel::sql_query(
            "SELECT COUNT(*) AS n FROM pg_stat_activity \
             WHERE pid = $1 AND state = 'active' AND wait_event_type = 'Lock'",
        )
        .bind::<diesel::sql_types::Integer, _>(pid)
        .get_result::<Count>(observer)
        .await
        .expect("read the activity")
        .n;
        if waiting == 1 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("backend {pid} never waited for a lock");
}

/// Queues the four rows of the pause test, in claim order. The first row
/// spends a token of `bucket`. The second is the workflow task of a new
/// running run. Two plain rows follow.
///
/// Returns the run, the row of the run, and the three other rows.
async fn enqueue_a_batch_around_a_run(
    conn: &mut AsyncPgConnection,
    queue: &str,
    bucket: &str,
) -> (Uuid, Uuid, HashSet<Uuid>) {
    let run = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, shard_id, input, state) \
         VALUES ($1, 'batch-paused', $2, 0, '{}'::jsonb, 'RUNNING')",
    )
    .bind::<diesel::sql_types::Uuid, _>(run)
    .bind::<diesel::sql_types::Text, _>(run.to_string())
    .execute(conn)
    .await
    .expect("insert run");
    let mut first = EnqueueParams::new(queue, TaskType::Activity, serde_json::json!({}));
    first.activity_name = Some("noop".to_owned());
    first.activity_id = Some(Uuid::new_v4());
    first.rate_limit_key = Some(bucket.to_owned());
    first.priority = 9;
    let mut peers = HashSet::from([queue::enqueue(conn, &first).await.expect("enqueue")]);
    let mut of_the_run = EnqueueParams::new(queue, TaskType::Workflow, serde_json::json!({}));
    of_the_run.workflow_exec_id = Some(run);
    of_the_run.priority = 7;
    let of_the_run = queue::enqueue(conn, &of_the_run).await.expect("enqueue");
    for priority in [5, 3] {
        peers.insert(enqueue(conn, queue, &plain(priority)).await);
    }
    (run, of_the_run, peers)
}

/// A row that a post-claim recheck gives back leaves the rest of its batch
/// claimed.
///
/// The first row of the batch spends a token from a bucket that a peer
/// holds locked. The batch statement waits there, after it read its
/// snapshot. A pause of the run of the second row commits during the wait.
/// The statement does not see the pause, so it claims the workflow task of
/// the paused run. The recheck of that row reads a fresh snapshot and gives
/// the row back.
///
/// The row is `PENDING` again, with its attempt restored and no worker. Its
/// three peers stay claimed. No batch takes the row while the run is
/// paused. After a resume, the next batch takes it.
#[tokio::test(flavor = "multi_thread")]
async fn a_batch_gives_back_the_row_of_a_run_paused_during_the_claim() {
    use autumn_harvest::execution::{pause_workflow_execution, resume_workflow_execution};
    use autumn_harvest::telemetry::NoOpMetrics;
    use autumn_harvest::types::ExecutionId;

    const WORKER: &str = "w-paused";
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue = unique_queue("batch-paused");
    let bucket = unique_queue("batch-paused-bucket");
    queue::ensure_rate_limit_bucket(&mut conn, &bucket, 0.0, 1.0)
        .await
        .expect("bucket");
    let (run, held, peers) = enqueue_a_batch_around_a_run(&mut conn, &queue, &bucket).await;
    let run = ExecutionId::from_uuid(run);

    let mut holder = connect(&url).await;
    holder.batch_execute("BEGIN").await.expect("begin holder");
    diesel::sql_query("SELECT 1 FROM harvest_rate_limit_buckets WHERE key = $1 FOR UPDATE")
        .bind::<diesel::sql_types::Text, _>(&bucket)
        .execute(&mut holder)
        .await
        .expect("hold the bucket lock");

    let mut claim_conn = connect(&url).await;
    let pid = backend_pid(&mut claim_conn).await;
    let claim_queue = queue.clone();
    let claimer = tokio::spawn(async move {
        claim(
            &mut claim_conn,
            &claim_queue,
            WORKER,
            None,
            ClaimBatch::Four,
        )
        .await
    });
    wait_for_a_statement_at_a_lock(&mut conn, pid).await;
    let pause =
        pause_workflow_execution(&mut conn, run, Some("recheck"), "batch-test", &NoOpMetrics);
    tokio::time::timeout(Duration::from_secs(20), pause)
        .await
        .expect("the pause does not wait for the claim")
        .expect("pause");
    assert!(!claimer.is_finished(), "the claim waits on the bucket lock");
    let before_the_claim = row_version(&mut conn, held).await.written_by;
    holder.batch_execute("COMMIT").await.expect("release");

    let taken = claimer.await.expect("claimer");
    assert_eq!(
        priorities(&taken),
        [9, 5, 3],
        "the peers of the row that went back stay claimed"
    );
    assert_eq!(taken.iter().map(|row| row.0).collect::<HashSet<_>>(), peers);
    let row = row_version(&mut conn, held).await;
    // The pause wrote no version of the row. Only the claim transaction did,
    // so the statement claimed the row and the recheck gave it back.
    assert_ne!(
        row.written_by, before_the_claim,
        "the claim transaction wrote the row of the paused run"
    );
    assert_eq!(
        (row.state.as_str(), row.attempt, row.worker_id),
        ("PENDING", 0, None),
        "the recheck gives the row back, and the claim costs it no attempt"
    );
    assert_eq!(running_count(&mut conn, &queue).await, 3);
    let tokens = bucket_tokens(&mut conn, &bucket).await;
    assert!(
        tokens.abs() < f64::EPSILON,
        "the first row spent its token: {tokens}"
    );

    assert_eq!(
        claim(&mut conn, &queue, WORKER, None, ClaimBatch::Four).await,
        Vec::new(),
        "no batch takes the row of a paused run"
    );
    resume_workflow_execution(&mut conn, run, "batch-test", &NoOpMetrics)
        .await
        .expect("resume");
    let after = claim(&mut conn, &queue, WORKER, None, ClaimBatch::Four).await;
    assert!(
        after.iter().any(|row| row.0 == held),
        "a batch takes the row after the resume: {after:?}"
    );
}

/// A batch with no free row in its window falls to the full statement.
///
/// One key at its cap holds a full head of rows. The free rows sit under
/// that head, outside the window. The batch text then finds no row, and it
/// sets the fallback flag because the head is full. The claim sends the
/// full statement in the same transaction. That statement takes one row at
/// most, so the batch takes one row: the row that a single claim takes.
///
/// When the full statement finds no row either, the batch takes none.
#[tokio::test(flavor = "multi_thread")]
async fn a_batch_with_no_free_row_in_its_window_takes_one_row_by_the_full_statement() {
    const WORKER: &str = "w-fall";
    let (url, _container) = setup_db().await;
    // New connections, so each count of full statements starts at zero.
    let mut batch_conn = connect(&url).await;
    let mut single_conn = connect(&url).await;
    let batch_queue = unique_queue("batch-fall-a");
    let single_queue = unique_queue("batch-fall-b");
    for (conn, queue) in [
        (&mut batch_conn, &batch_queue),
        (&mut single_conn, &single_queue),
    ] {
        for _ in 0..=lean::HEAD_LIMIT {
            enqueue(conn, queue, &keyed(9, "held", 1)).await;
        }
        for priority in [3, 2, 1] {
            enqueue(conn, queue, &plain(priority)).await;
        }
        // One row takes the key to its cap. A full head of its rows stays.
        let first = claim(conn, queue, WORKER, None, ClaimBatch::Two).await;
        assert_eq!(priorities(&first), [9], "the key has a cap of one");
    }
    assert_eq!(
        lean::full_statements(&mut batch_conn).await,
        0,
        "the batch text took the first row"
    );

    let steps = [
        (None, ClaimBatch::Two, 3),
        (Some(TaskType::Activity), ClaimBatch::Four, 2),
        (None, ClaimBatch::Eight, 1),
    ];
    for (step, (kind, batch, priority)) in (1_i64..).zip(steps) {
        let taken = claim(&mut batch_conn, &batch_queue, WORKER, kind, batch).await;
        assert_eq!(
            priorities(&taken),
            [priority],
            "{batch:?} of kind {kind:?} takes one row by the full statement"
        );
        assert_eq!(
            lean::full_statements(&mut batch_conn).await,
            step,
            "{batch:?} of kind {kind:?} sends the full statement once"
        );
        let single = queue::claim_task(
            &mut single_conn,
            std::slice::from_ref(&single_queue),
            WORKER,
            "",
            None,
            &[],
            &[],
        )
        .await
        .expect("single claim")
        .map(|task| task.priority);
        assert_eq!(single, Some(priority), "a single claim takes the same row");
    }

    assert_eq!(
        claim(
            &mut batch_conn,
            &batch_queue,
            WORKER,
            None,
            ClaimBatch::Eight
        )
        .await,
        Vec::new(),
        "the full statement finds no free row"
    );
    assert_eq!(lean::full_statements(&mut batch_conn).await, 4);
    assert_eq!(running_count(&mut batch_conn, &batch_queue).await, 4);
    assert_eq!(running_count(&mut single_conn, &single_queue).await, 4);
}

// ── The plan on a deep backlog ──────────────────────────────────────────────

const BATCHES: [ClaimBatch; 3] = [ClaimBatch::Two, ClaimBatch::Four, ClaimBatch::Eight];
const KINDS: [Option<TaskType>; 3] = [None, Some(TaskType::Workflow), Some(TaskType::Activity)];

/// The `EXPLAIN (ANALYZE, BUFFERS)` plan of one batch claim, rolled back.
///
/// The statement is prepared under the planner settings of a claim
/// transaction, so the plan is the generic plan that a real claim reuses.
/// The fence binds match the generation row of `lean::write_shared_rows`.
async fn batch_plan(
    conn: &mut AsyncPgConnection,
    kind: Option<TaskType>,
    fenced: bool,
    batch: ClaimBatch,
    queue: &str,
) -> serde_json::Value {
    #[derive(diesel::QueryableByName)]
    struct Plan {
        #[diesel(sql_type = diesel::sql_types::Json, column_name = "QUERY PLAN")]
        plan: serde_json::Value,
    }
    let mut binds = vec![format!(
        "'{}', ARRAY['{queue}'], '', NULL, ARRAY[]::text[], ARRAY[]::text[]",
        lean::WORKER
    )];
    if fenced {
        binds.push(format!(
            "ARRAY[{}]::int4[], ARRAY[{}]::int8[]",
            lean::FENCE_SHARD,
            lean::FENCE_GENERATION
        ));
    }
    let binds = binds.join(", ");
    lean::exec(
        conn,
        &format!(
            "BEGIN; {}; \
             PREPARE batch_plan_probe(text, text[], text, bigint, text[], text[]) AS {}",
            queue::CLAIM_PLAN_SETTINGS_SQL,
            queue::batch_claim_query_for(kind, fenced, batch)
        ),
    )
    .await;
    let rows: Vec<Plan> = diesel::sql_query(format!(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE batch_plan_probe({binds})"
    ))
    .load(conn)
    .await
    .unwrap_or_else(|e| panic!("explain the batch claim of {kind:?}, {batch:?}: {e}"));
    lean::exec(conn, "ROLLBACK; DEALLOCATE batch_plan_probe;").await;
    let plan = rows.into_iter().next().expect("one plan row").plan;
    plan[0]["Plan"].clone()
}

/// Asserts the plan shape of one batch claim over the deep backlog.
fn assert_the_batch_plan_is_bounded(plan: &serde_json::Value, case: &str, batch: ClaimBatch) {
    let mut all = Vec::new();
    lean::plan_nodes(plan, None, false, &mut all);
    let on_the_queue_table =
        |node: &serde_json::Value| node["Relation Name"].as_str() == Some("harvest_task_queue");
    let node_type = |node: &serde_json::Value| -> String {
        node["Node Type"].as_str().unwrap_or_default().to_owned()
    };
    // Four queue heads and the pin head make the window.
    let window = 4 * lean::HEAD_LIMIT + lean::DEEP_PINNED_ROWS;

    let claimed = all
        .iter()
        .find(|(_, node)| node["Subplan Name"].as_str() == Some("CTE claimed"))
        .map(|(_, node)| lean::per_loop(node, "Actual Rows"));
    assert_eq!(
        claimed,
        i64::try_from(batch.rows()).ok(),
        "{case}: the batch claims a row for each of its slots"
    );

    // No scan reads the whole queue table, in a branch that ran or not.
    for (_, node) in &all {
        assert!(
            !(on_the_queue_table(node) && node_type(node) == "Seq Scan"),
            "{case}: a sequential scan of the queue table: {node}"
        );
    }

    // The ranking of the batch sorts its own rows only. No sort reads more
    // than the window, and no sort goes to disk.
    for (_, node) in &all {
        if !node_type(node).contains("Sort") {
            continue;
        }
        assert!(
            node["Sort Space Type"].as_str() != Some("Disk"),
            "{case}: a sort on disk: {node}"
        );
        assert!(
            lean::per_loop(node, "Actual Rows") <= window,
            "{case}: a sort reads more than the window: {node}"
        );
    }

    // Each head reads its ordered index, and reads one window of it.
    let mut queue_heads = 0;
    for (cte, head) in &all {
        if *cte != Some("seek_heads") || !on_the_queue_table(head) {
            continue;
        }
        let index = head["Index Name"].as_str().unwrap_or("none");
        assert!(
            node_type(head) == "Index Scan"
                && (index == "idx_harvest_tq_claim_seek" || index == "idx_harvest_tq_sticky_poll"),
            "{case}: a head scan reads {index}, not its ordered index: {head}"
        );
        if index == "idx_harvest_tq_claim_seek" {
            queue_heads += 1;
            let read = lean::per_loop(head, "Actual Rows")
                + lean::per_loop(head, "Rows Removed by Filter");
            assert!(
                lean::per_loop(head, "Actual Loops") > 0 && read <= lean::HEAD_LIMIT,
                "{case}: a queue head reads {read} rows for each loop: {head}"
            );
        }
    }
    assert_eq!(queue_heads, 1, "{case}: one scan serves the queue heads");

    // Each scan of the queue table that ran reads a window at most.
    for (cte, node) in &all {
        if !on_the_queue_table(node) || lean::per_loop(node, "Actual Loops") == 0 {
            continue;
        }
        let read =
            lean::per_loop(node, "Actual Rows") + lean::per_loop(node, "Rows Removed by Filter");
        assert!(
            read <= window,
            "{case}: a scan in {cte:?} reads {read} rows for each loop: {node}"
        );
    }

    let buffers =
        lean::per_loop(plan, "Shared Hit Blocks") + lean::per_loop(plan, "Shared Read Blocks");
    assert!(
        buffers <= lean::DEEP_ROWS / 20,
        "{case}: the claim reads {buffers} buffers of a backlog of {} rows",
        lean::DEEP_ROWS
    );
}

/// The batch form keeps a bounded plan on a deep backlog of one queue.
///
/// A default worker sends the batch text on most claims, so its plan is the
/// claim plan of a default fleet. The batch text adds a `row_number()` sort
/// to the window text, under planner settings that turn sorts off. This
/// test pins the plan of each of the eighteen batch texts:
///
/// - No scan of the queue table is sequential.
/// - Each head scan reads `idx_harvest_tq_claim_seek`, or the pin index,
///   and a queue head reads one window of rows.
/// - No sort reads more than the window, and no sort goes to disk.
/// - The claim takes a row for each slot of its batch.
///
/// It does so with fresh statistics, and with the statistics of an idle
/// system. There an `ANALYZE` sees terminal rows only, and then a backlog
/// arrives. Each plan is the generic plan under the claim settings.
#[tokio::test]
async fn each_batch_text_keeps_a_bounded_plan_on_a_deep_backlog_of_one_queue() {
    #[derive(diesel::QueryableByName)]
    struct Mcv {
        #[diesel(sql_type = diesel::sql_types::Text)]
        vals: String,
    }
    let _registry = lean::clear_fence_registry().await;
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    lean::write_shared_rows(&mut conn).await;
    let queue = "batch-deep";
    let terminal = lean::deep_rows_sql(queue, lean::DEEP_TERMINAL_ROWS, "COMPLETED");
    let backlog = format!(
        "{}; UPDATE harvest_task_queue SET sticky_worker_id = '{}', \
           sticky_until = NOW() + INTERVAL '1 hour' \
         WHERE id IN (SELECT id FROM harvest_task_queue WHERE state = 'PENDING' \
                      LIMIT {})",
        lean::deep_rows_sql(queue, lean::DEEP_ROWS, "PENDING"),
        lean::WORKER,
        lean::DEEP_PINNED_ROWS
    );
    lean::exec(
        &mut conn,
        "ALTER TABLE harvest_task_queue SET (autovacuum_enabled = off)",
    )
    .await;
    let mut plans = 0;
    for statistics in ["fresh", "idle"] {
        lean::exec(&mut conn, "TRUNCATE harvest_task_queue CASCADE").await;
        lean::exec(&mut conn, &terminal).await;
        if statistics == "fresh" {
            lean::exec(&mut conn, &backlog).await;
            lean::exec(&mut conn, "VACUUM ANALYZE harvest_task_queue").await;
        } else {
            lean::exec(&mut conn, "VACUUM ANALYZE harvest_task_queue").await;
            lean::exec(&mut conn, &backlog).await;
        }
        let mcv: Mcv = diesel::sql_query(
            "SELECT COALESCE(most_common_vals::text, '') AS vals FROM pg_stats \
             WHERE tablename = 'harvest_task_queue' AND attname = 'state'",
        )
        .get_result(&mut conn)
        .await
        .expect("read the state statistics");
        assert_eq!(
            mcv.vals.contains("PENDING"),
            statistics == "fresh",
            "{statistics} statistics show a PENDING row: {}",
            mcv.vals
        );
        for kind in KINDS {
            for fenced in [false, true] {
                for batch in BATCHES {
                    let case = format!(
                        "{statistics} statistics, kind {kind:?}, fenced {fenced}, {batch:?}"
                    );
                    let plan = batch_plan(&mut conn, kind, fenced, batch, queue).await;
                    assert_the_batch_plan_is_bounded(&plan, &case, batch);
                    plans += 1;
                }
            }
        }
    }
    assert_eq!(plans, 36, "eighteen texts under two sets of statistics");
    db.drop_db().await;
}

// ── Random states ───────────────────────────────────────────────────────────

/// The seeds of the batch comparison. The list is fixed, so each run builds
/// the same states.
const BATCH_SEEDS: std::ops::RangeInclusive<u64> = 1..=64;

/// The most steps of the comparison on one state. A state ends sooner when
/// a claim of each kind finds no row.
const BATCH_STEPS: usize = 45;

/// The fewest batches of two rows or more that each property must compare.
const BATCH_FLOOR: u32 = 150;

/// The fewest batches that must end before the rows of the single claims.
const CUT_FLOOR: u32 = 20;

/// The fewest batches of two rows or more that a keyed row must lead.
const KEYED_FIRST_FLOOR: u32 = 8;

/// The fewest states where a stale pin must refuse a batch that the live
/// pin then takes.
const FENCE_REFUSAL_FLOOR: u32 = 48;

/// True when row `n` of `spec` carries a concurrency key or a rate-limit
/// key. Only the first row of a batch may be such a row.
fn keyed_row(spec: &lean::StateSpec, n: i64) -> bool {
    let row = usize::try_from(n).expect("an ordinal is not negative");
    matches!(
        spec.rows[row].extra,
        lean::Extra::Key(_) | lean::Extra::Rate(_)
    )
}

/// One batch claim of the worker of the differential tests over a written
/// state. Returns the ordinals of its rows.
async fn state_batch(
    conn: &mut AsyncPgConnection,
    state: &lean::Written,
    kind: Option<TaskType>,
    batch: ClaimBatch,
) -> HashSet<i64> {
    let (breakers, ineligible) = lean::activity_binds();
    let claimed = queue::claim_task_batch_with_slots(
        conn,
        &state.queues,
        lean::WORKER,
        lean::BUILD,
        &breakers,
        &ineligible,
        None,
        kind,
        None,
        batch,
    )
    .await
    .expect("batch claim");
    match claimed {
        SlottedBatch::Claimed(tasks) => {
            let rows: HashSet<i64> = tasks
                .iter()
                .map(|task| task.input["n"].as_i64().expect("a row holds its ordinal"))
                .collect();
            assert_eq!(rows.len(), tasks.len(), "a batch holds each row once");
            assert!(rows.len() <= batch.rows(), "a batch stays at its size");
            rows
        }
        SlottedBatch::Empty => HashSet::new(),
        SlottedBatch::NoSlot => panic!("no claim slots were asked for"),
    }
}

/// What the comparison saw over the states of one property.
#[derive(Debug, Default, PartialEq, Eq)]
struct BatchTally {
    states: u32,
    /// The batches of two rows or more.
    batches: u32,
    /// The batches that took a row for each of their slots.
    full: u32,
    /// The batches that took fewer rows than the single claims took.
    shorter: u32,
    /// The batches of two rows or more whose first row carries a key.
    keyed_first: u32,
    /// The steps where neither side took a row.
    empty: u32,
    /// The states where a stale pin took no row and the live pin then took
    /// one. Only a fenced pass counts them.
    fence_refusals: u32,
}

impl BatchTally {
    /// Asserts the floors of the states with no keyed row.
    fn assert_plain_floors(&self, pass: &str) {
        assert!(
            self.batches >= BATCH_FLOOR && self.full >= BATCH_FLOOR && self.empty >= 1,
            "{pass}: the seeds give too few batches with no keyed row: {self:?}"
        );
        assert_eq!(self.shorter, 0, "{pass}: {self:?}");
    }

    /// Asserts the floors of the states with keyed rows.
    fn assert_keyed_floors(&self, pass: &str) {
        assert!(
            self.batches >= BATCH_FLOOR
                && self.shorter >= CUT_FLOOR
                && self.keyed_first >= KEYED_FIRST_FLOOR,
            "{pass}: the seeds give too few batches with keyed rows, or too few key cuts: \
             {self:?}"
        );
    }
}

/// Makes the pin of the process stale or live again. The generation row of
/// the database moves, because a pin never changes.
async fn move_the_generation(conn: &mut AsyncPgConnection, stale: bool) {
    let generation = lean::FENCE_GENERATION + i64::from(stale);
    lean::exec(
        conn,
        &format!(
            "UPDATE harvest_shard_generation SET generation = {generation} \
             WHERE shard_id = {}",
            lean::FENCE_SHARD
        ),
    )
    .await;
}

/// A batch claim of each kind under a stale pin takes no row and changes
/// nothing. `state` is a copy that no claim touched yet.
async fn assert_a_stale_pin_claims_nothing(
    conn: &mut AsyncPgConnection,
    state: &lean::Written,
    case: &str,
) {
    move_the_generation(conn, true).await;
    let before = lean::digest(conn, &state.queues, &state.buckets).await;
    for kind in KINDS {
        for batch in BATCHES {
            let taken = state_batch(conn, state, kind, batch).await;
            assert!(
                taken.is_empty(),
                "{case}: a stale pin took {taken:?} with kind {kind:?}, {batch:?}"
            );
        }
    }
    assert_eq!(
        lean::digest(conn, &state.queues, &state.buckets).await,
        before,
        "{case}: a refused batch changes no row and spends no token"
    );
    move_the_generation(conn, false).await;
}

/// The batch comparison over each seed, for one fence state.
///
/// With `fenced`, the process holds a pin at the generation of
/// `lean::write_shared_rows`. Each claim of both sides then sends its
/// fenced text. Each state first proves that a stale pin claims nothing.
///
/// Returns the tally of the states with no keyed row, then the tally of
/// the states with keyed rows.
async fn compare_batches_with_single_claims(
    batch_conn: &mut AsyncPgConnection,
    single_conn: &mut AsyncPgConnection,
    fenced: bool,
) -> (BatchTally, BatchTally) {
    let pass = if fenced { "fenced" } else { "unfenced" };
    let mut plain = BatchTally::default();
    let mut keyed = BatchTally::default();
    for seed in BATCH_SEEDS {
        let spec = lean::state_spec(seed);
        let has_keys = (0..spec.rows.len())
            .any(|n| keyed_row(&spec, i64::try_from(n).expect("a row count fits")));
        let tally = if has_keys { &mut keyed } else { &mut plain };
        tally.states += 1;
        let tag = u8::from(fenced);
        let batch_copy = lean::write_state(batch_conn, &spec, &format!("ba{tag}-{seed}")).await;
        let single_copy = lean::write_state(single_conn, &spec, &format!("bb{tag}-{seed}")).await;
        if fenced {
            let case = format!("{pass}, seed {seed}");
            assert_a_stale_pin_claims_nothing(batch_conn, &batch_copy, &case).await;
        }
        let mut refused_then_claimed = false;
        let mut empty_run = 0;
        for step in 0..BATCH_STEPS {
            // Three empty steps in a row are one of each kind.
            if empty_run == KINDS.len() {
                break;
            }
            let kind = KINDS[step % 3];
            let batch = BATCHES[(step / 3) % 3];
            let case = format!("{pass}, seed {seed}, step {step}, kind {kind:?}, {batch:?}");

            let mut singles = Vec::new();
            while singles.len() < batch.rows() {
                let Some(row) =
                    lean::state_claim(single_conn, &single_copy, kind, None, None).await
                else {
                    break;
                };
                singles.push(row);
            }
            let taken = state_batch(batch_conn, &batch_copy, kind, batch).await;

            assert!(
                taken.len() <= singles.len(),
                "{case}: the batch took {taken:?}, the single claims took {singles:?}"
            );
            let prefix: HashSet<i64> = singles[..taken.len()].iter().copied().collect();
            assert_eq!(
                taken, prefix,
                "{case}: the batch takes the first rows of the single claims {singles:?}"
            );
            assert!(
                !taken.is_empty() || singles.is_empty(),
                "{case}: the batch took no row, the single claims took {singles:?}"
            );
            if has_keys {
                let keyed_rows: Vec<i64> = taken
                    .iter()
                    .copied()
                    .filter(|n| keyed_row(&spec, *n))
                    .collect();
                assert!(
                    keyed_rows.is_empty() || keyed_rows == [singles[0]],
                    "{case}: only the first row of a batch may carry a key: {keyed_rows:?} of \
                     {singles:?}"
                );
                tally.keyed_first += u32::from(taken.len() >= 2 && !keyed_rows.is_empty());
            } else {
                assert_eq!(
                    taken.len(),
                    singles.len(),
                    "{case}: with no keyed row, the batch takes each row of the single claims \
                     {singles:?}"
                );
            }
            tally.batches += u32::from(taken.len() >= 2);
            tally.full += u32::from(taken.len() == batch.rows());
            tally.shorter += u32::from(taken.len() < singles.len());
            tally.empty += u32::from(singles.is_empty());
            if fenced && !refused_then_claimed && !taken.is_empty() {
                refused_then_claimed = true;
                tally.fence_refusals += 1;
            }
            empty_run = if singles.is_empty() { empty_run + 1 } else { 0 };

            // The batch side takes the rest by single claims, in the same order.
            for expected in &singles[taken.len()..] {
                let row = lean::state_claim(batch_conn, &batch_copy, kind, None, None).await;
                assert_eq!(row, Some(*expected), "{case}: the rest of {singles:?}");
            }
            assert_eq!(
                lean::digest(batch_conn, &batch_copy.queues, &batch_copy.buckets).await,
                lean::digest(single_conn, &single_copy.queues, &single_copy.buckets).await,
                "{case}: the copies hold the same state"
            );
        }
    }
    (plain, keyed)
}

/// A batch takes the rows that a run of single claims takes, over random
/// states.
///
/// Each seed writes two copies of its state. The single side sends up to
/// `n` window-first single claims over one copy. The batch side sends one
/// batch claim of `n` rows over the other copy. The claims change kind and
/// size as the permits of a worker do.
///
/// In each step the batch takes a prefix of the rows of the single claims:
/// its `k` rows are their first `k` rows. A batch takes no row only when a
/// single claim takes none.
///
/// A state with no keyed row holds plain rows with distinct claim keys. It
/// also holds paused queues and activities, pins, sessions and the parked
/// rows of paused and expired runs. There the batch takes each row that the
/// single claims take.
///
/// A state with keyed rows also holds concurrency keys and rate-limit keys.
/// There the batch holds one keyed row at most, and that row is its first
/// row. That is the key cut.
///
/// After each step the batch side takes the other rows by single claims.
/// Both copies must then hold the same state.
///
/// The second pass runs under a DR pin, so each claim sends its fenced
/// text. The fence is open, so each tally must equal that of the first
/// pass. Before its first claim, each state proves that a stale pin takes
/// no row.
#[tokio::test]
async fn random_states_give_a_batch_the_rows_of_a_run_of_single_claims() {
    use autumn_harvest::replication::{FenceRegistry, ShardGeneration};

    let _registry = lean::clear_fence_registry().await;
    let db = OwnDb::new().await;
    let mut batch_conn = db.conn().await;
    let mut single_conn = db.conn().await;
    lean::write_shared_rows(&mut batch_conn).await;

    let (plain, keyed) =
        compare_batches_with_single_claims(&mut batch_conn, &mut single_conn, false).await;
    // Each property must compare real batches, or it proves little.
    println!("unfenced, states with no keyed row: {plain:?}");
    println!("unfenced, states with keyed rows: {keyed:?}");
    plain.assert_plain_floors("unfenced");
    keyed.assert_keyed_floors("unfenced");

    let shard = ShardId::new(lean::FENCE_SHARD);
    FenceRegistry::register(shard, ShardGeneration::new(lean::FENCE_GENERATION))
        .expect("no pin before this one");
    FenceRegistry::set_default_shard(shard).expect("no default shard before this one");
    let (fenced_plain, fenced_keyed) =
        compare_batches_with_single_claims(&mut batch_conn, &mut single_conn, true).await;
    println!("fenced, states with no keyed row: {fenced_plain:?}");
    println!("fenced, states with keyed rows: {fenced_keyed:?}");
    let refusals = fenced_plain.fence_refusals + fenced_keyed.fence_refusals;
    assert!(
        refusals >= FENCE_REFUSAL_FLOOR,
        "too few states where a stale pin took no row and the live pin took one: {refusals}"
    );
    // An open fence changes no claim. Only the refusal count differs.
    for (fenced, unfenced) in [(fenced_plain, plain), (fenced_keyed, keyed)] {
        let fenced = BatchTally {
            fence_refusals: 0,
            ..fenced
        };
        assert_eq!(
            fenced, unfenced,
            "the fenced pass compares the same batches"
        );
    }
    db.drop_db().await;
}

// ── The default worker ──────────────────────────────────────────────────────

#[activity(start_to_close = "30s")]
async fn batch_default_step(_ctx: &ActivityContext, n: u64) -> Result<u64, String> {
    tokio::task::yield_now().await;
    Ok(n)
}

/// One activity. Two workflow tasks and one activity task for each run.
#[workflow]
async fn batch_default_flow(ctx: &WorkflowContext, queue: String) -> Result<String, String> {
    ctx.execute_activity_raw("batch_default_step", serde_json::json!(1), &queue)
        .await
        .map_err(|e| e.to_string())?;
    Ok("done".to_string())
}

/// The tasks that one run of [`batch_default_flow`] dispatches.
const TASKS_PER_RUN: u64 = 3;

/// Counts the claim statements of one worker that took a row, by kind.
#[derive(Default, Clone)]
struct RowClaims(Arc<Mutex<BTreeMap<&'static str, u64>>>);

impl RowClaims {
    fn with_a_row(&self) -> u64 {
        let counts = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        [ClaimKind::Workflow, ClaimKind::Activity]
            .iter()
            .map(|kind| counts.get(kind.as_str()).copied().unwrap_or(0))
            .sum()
    }
}

impl MetricsRecorder for RowClaims {
    fn record_claim_duration(&self, kind: ClaimKind, _shard: u16, _seconds: f64) {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(kind.as_str())
            .or_default() += 1;
    }
}

/// Queues `runs` runs of [`batch_default_flow`], then runs a worker with
/// `config` until each run is complete.
///
/// Returns the claim forms that the pool of the worker sent, and the claim
/// statements that took a row.
async fn drain_a_backlog(db: &OwnDb, config: WorkerConfig, runs: u64) -> (lean::Forms, u64) {
    let queue = unique_queue("batch-default");
    let pool = build_test_pool(&db.url);
    let mut conn = db.conn().await;
    let client = WorkflowHandleClient::single(pool.clone(), db.url.clone());
    let mut started = Vec::new();
    for _ in 0..runs {
        let exec = BatchDefaultFlowStub::start_with_options(
            &mut conn,
            &client,
            &unique_queue("batch-default-wf"),
            queue.clone(),
            TypedStartOptions {
                queue_name: Some(queue.clone()),
                ..Default::default()
            },
        )
        .await
        .expect("start")
        .exec_id();
        started.push(exec);
    }

    let claims = RowClaims::default();
    let built = HarvestBuilder::new()
        .workflows(workflows![batch_default_flow])
        .activities(activities![batch_default_step])
        .worker(config.with_queues([queue.as_str()]))
        .telemetry(
            TelemetryConfig::builder()
                .metrics(Arc::new(claims.clone()))
                .build(),
        )
        .build();
    let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
    let mut runtime: WorkerRuntimeConfig = worker_config.into();
    runtime.worker_id = unique_queue("batch-default-worker");
    runtime.poll_interval = Duration::from_millis(50);
    runtime.shutdown_timeout = Duration::from_secs(5);
    runtime.shard_assignments = vec![ShardId::new(0)];
    let worker = Arc::new(Worker::new(runtime, Arc::new(registry)).expect("the worker builds"));
    let runner = Arc::clone(&worker);
    let run_pool = pool.clone();
    let handle = tokio::spawn(async move { runner.run(&run_pool).await });
    for exec in started {
        wait_for_execution_state_with_timeout(&db.url, exec, "COMPLETED", Duration::from_secs(60))
            .await;
    }
    worker.shutdown();
    handle.await.expect("the worker joins");
    (lean::pool_forms(&pool).await, claims.with_a_row())
}

/// A worker with no knob set claims in batches, and it completes each task.
///
/// `WorkerConfig::default()` sets no claim knob. On a backlog its claim
/// loops send batch statements, and some of them take two rows or more. So
/// the statements that took a row are fewer than the tasks.
///
/// The same worker with `with_claim_batch_max(1)` sends no batch statement.
/// Each of its claim statements that took a row took one task.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_default_worker_claims_a_backlog_in_batches_and_one_row_turns_it_off() {
    const RUNS: u64 = 40;
    let _registry = lean::clear_fence_registry().await;
    let db = OwnDb::new().await;

    let (sent, with_a_row) = drain_a_backlog(&db, WorkerConfig::default(), RUNS).await;
    println!("default worker: {sent:?}, {with_a_row} claim statements took a row");
    assert!(
        sent.batch >= 5,
        "a default worker sends batch statements: {sent:?}"
    );
    assert!(
        with_a_row < TASKS_PER_RUN * RUNS,
        "some statement took two rows or more: {with_a_row} statements, {sent:?}"
    );

    let one_row = WorkerConfig::default().with_claim_batch_max(1);
    let (sent, with_a_row) = drain_a_backlog(&db, one_row, RUNS).await;
    println!("one row per claim: {sent:?}, {with_a_row} claim statements took a row");
    assert_eq!(sent.batch, 0, "one row per claim: {sent:?}");
    assert!(
        sent.lean + sent.window >= i64::try_from(TASKS_PER_RUN * RUNS).expect("fits"),
        "each task has its own claim statement: {sent:?}"
    );
    assert_eq!(
        with_a_row,
        TASKS_PER_RUN * RUNS,
        "each claim statement that took a row took one task"
    );
    db.drop_db().await;
}
