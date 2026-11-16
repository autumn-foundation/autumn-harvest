//! The batch form of the default claim.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run against it.
//! Otherwise a fresh testcontainers Postgres boots with the migrations.

use std::collections::HashSet;

use autumn_harvest::queue::{self, ClaimBatch, EnqueueParams, SlottedBatch, TaskType};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

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
/// the claim reads its clock after the run deadline. The rows behind it
/// belong to other runs, and the claim still takes them.
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
