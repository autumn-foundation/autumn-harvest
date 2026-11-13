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
    clippy::unused_async
)]
//! DB-backed correctness tests for the batched seek-and-refine claim path
//! (issue #1340).
//!
//! `queue::claim_task_batched` is not wired into the default claim path.
//! See the module doc above `queue::claim_task_batched_candidates_query`.
//! The single-session apparatus of
//! `docs/assays/0005-claim-batched-seek-and-refine.md` could not prove one
//! thing. These tests prove it: the batching mechanism keeps its
//! exactly-once, never-over-the-cap guarantee under REAL concurrent
//! claimers, not only in a serial walk.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres, to
//! run against it directly. Otherwise a fresh testcontainers Postgres
//! boots with the full migration bundle.

use autumn_harvest::build_routing;
use autumn_harvest::queue::{
    self, BatchedClaimConfig, EnqueueParams, TaskType, claim_task, claim_task_batched,
};
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

/// A migrated Postgres 16 -- the env URL when set, else a fresh testcontainer.
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

async fn setup_db() -> (String, AsyncPgConnection, Option<ContainerAsync<Postgres>>) {
    let (url, container) = setup_db_url().await;
    let conn = connect(&url).await;
    (url, conn, container)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// A short, unique queue name.
///
/// The NOTIFY channel `enqueue` derives from it (`harvest_queue_{name}`) is
/// a Postgres identifier capped at 63 bytes. This stays well under that,
/// even with the longest prefix this suite uses.
fn unique_queue(prefix: &str) -> String {
    format!("{prefix}-{}", &Uuid::new_v4().simple().to_string()[..8])
}

async fn insert_execution(conn: &mut AsyncPgConnection) -> Uuid {
    use diesel_async::RunQueryDsl;
    let id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, shard_id, input) \
         VALUES ($1, 'batched-claim', $2, 0, '{}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .bind::<diesel::sql_types::Text, _>(id.to_string())
    .execute(conn)
    .await
    .expect("insert execution");
    id
}

/// One knob this suite varies per row: priority and an optional saturated
/// concurrency key. A fixture can then rank "poison" ahead of the one
/// claimable row, exactly like `docs/assays/0005-...`'s own adversarial
/// fixtures.
struct RowSpec {
    priority: i32,
    concurrency_key: Option<String>,
    concurrency_cap: Option<u32>,
}

async fn enqueue_row(conn: &mut AsyncPgConnection, queue: &str, spec: &RowSpec) -> Uuid {
    let exec_id = insert_execution(conn).await;
    let mut params = EnqueueParams::new(queue, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = Some("noop".to_string());
    params.activity_id = Some(Uuid::new_v4());
    params.priority = spec.priority;
    params.concurrency_key = spec.concurrency_key.clone();
    params.max_concurrent = spec.concurrency_cap;
    queue::enqueue(conn, &params).await.expect("enqueue")
}

async fn task_state(conn: &mut AsyncPgConnection, id: Uuid) -> String {
    use diesel_async::RunQueryDsl;
    #[derive(diesel::QueryableByName)]
    struct S {
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
    }
    diesel::sql_query("SELECT state FROM harvest_task_queue WHERE id=$1")
        .bind::<diesel::sql_types::Uuid, _>(id)
        .get_result::<S>(conn)
        .await
        .expect("state")
        .state
}

/// Directly mark a row `RUNNING` under a worker, bypassing claim. A
/// fixture can then seed an already-saturated concurrency key, without
/// spending a real claim on it.
async fn force_running(conn: &mut AsyncPgConnection, id: Uuid, worker_id: &str) {
    use diesel_async::RunQueryDsl;
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = $2 WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .bind::<diesel::sql_types::Text, _>(worker_id)
    .execute(conn)
    .await
    .expect("force running");
}

async fn batched_claim_one(
    conn: &mut AsyncPgConnection,
    queue: &str,
    worker_id: &str,
    config: BatchedClaimConfig,
) -> Option<Uuid> {
    claim_task_batched(
        conn,
        &[queue.to_string()],
        worker_id,
        "",
        None,
        &[],
        &[],
        config,
    )
    .await
    .expect("batched claim")
    .map(|t| t.id)
}

// ── Equivalence with the single-row claim path ──────────────────────────────

/// On a plain backlog with no residual-gate predicates in play, the
/// batched path must pick the same row the single-row path would. Both
/// must pick the highest priority PENDING row.
#[tokio::test(flavor = "multi_thread")]
async fn batched_claim_picks_the_same_row_the_single_row_path_would() {
    let (_url, mut conn, _container) = setup_db().await;

    let queue_a = unique_queue("batched-equiv-single");
    let queue_b = unique_queue("batched-equiv-batched");

    // Identical priority shape seeded into two separate queues, so each
    // path claims from its own backlog and there is no shared-row race.
    for queue in [&queue_a, &queue_b] {
        for priority in [1, 5, 2, 9, 3] {
            enqueue_row(
                &mut conn,
                queue,
                &RowSpec {
                    priority,
                    concurrency_key: None,
                    concurrency_cap: None,
                },
            )
            .await;
        }
    }

    let single_claimed = claim_task(
        &mut conn,
        std::slice::from_ref(&queue_a),
        "w1",
        "",
        None,
        &[],
        &[],
    )
    .await
    .expect("single claim")
    .expect("a row was claimable");
    let batched_claimed_id =
        batched_claim_one(&mut conn, &queue_b, "w1", BatchedClaimConfig::default())
            .await
            .expect("a row was claimable");

    // Both paths claim the row of priority 9. The row ids of the two
    // backlogs differ, so the priority is the fact to compare.
    assert_eq!(single_claimed.priority, 9);
    let batched_priority: i32 = {
        use diesel_async::RunQueryDsl;
        #[derive(diesel::QueryableByName)]
        struct P {
            #[diesel(sql_type = diesel::sql_types::Integer)]
            priority: i32,
        }
        diesel::sql_query("SELECT priority FROM harvest_task_queue WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(batched_claimed_id)
            .get_result::<P>(&mut conn)
            .await
            .expect("priority")
            .priority
    };
    assert_eq!(batched_priority, 9);
}

/// An empty backlog must return `None`, not an empty-batch panic or loop.
#[tokio::test(flavor = "multi_thread")]
async fn batched_claim_returns_none_on_an_empty_backlog() {
    let (_url, mut conn, _container) = setup_db().await;
    let queue = unique_queue("batched-empty");
    let claimed = batched_claim_one(&mut conn, &queue, "w1", BatchedClaimConfig::default()).await;
    assert_eq!(claimed, None);
}

// ── Adversarial saturated-key fixtures (ledger #4 / #5 style) ──────────────

/// A saturated concurrency key ranked ahead of the one claimable row must
/// not block the claim. That is why the gate is a per-candidate recheck,
/// not a single-row candidate-side filter.
#[tokio::test(flavor = "multi_thread")]
async fn batched_claim_finds_the_claimable_row_behind_a_saturated_key() {
    let (_url, mut conn, _container) = setup_db().await;
    let queue = unique_queue("batched-adversarial-small");
    let saturated_key = format!("key-{}", Uuid::new_v4().simple());

    // One RUNNING row already holds the cap-1 key.
    let holder = enqueue_row(
        &mut conn,
        &queue,
        &RowSpec {
            priority: 0,
            concurrency_key: Some(saturated_key.clone()),
            concurrency_cap: Some(1),
        },
    )
    .await;
    force_running(&mut conn, holder, "holder").await;

    // Five more PENDING rows on the same saturated key, all ranked ahead
    // (higher priority) of the one claimable row.
    let mut poisoned = Vec::new();
    for _ in 0..5 {
        poisoned.push(
            enqueue_row(
                &mut conn,
                &queue,
                &RowSpec {
                    priority: 10,
                    concurrency_key: Some(saturated_key.clone()),
                    concurrency_cap: Some(1),
                },
            )
            .await,
        );
    }
    let claimable = enqueue_row(
        &mut conn,
        &queue,
        &RowSpec {
            priority: 1,
            concurrency_key: None,
            concurrency_cap: None,
        },
    )
    .await;

    let claimed = batched_claim_one(&mut conn, &queue, "w2", BatchedClaimConfig::default())
        .await
        .expect("the claimable row must be found despite the saturated key ahead of it");
    assert_eq!(claimed, claimable);

    for id in poisoned {
        assert_eq!(
            task_state(&mut conn, id).await,
            "PENDING",
            "a saturated key's row must never be claimed over its cap"
        );
    }
}

/// Enough poisoned rows to span more than one batch, WITH ties in the
/// poisoned rows' sort key. This exercises the `id` tiebreak of the keyset
/// cursor across a batch boundary. Ledger #5 found and fixed that bug.
#[tokio::test(flavor = "multi_thread")]
async fn batched_claim_walks_multiple_batches_across_tied_sort_keys() {
    let (_url, mut conn, _container) = setup_db().await;
    let queue = unique_queue("batched-adversarial-multibatch");
    let saturated_key = format!("key-{}", Uuid::new_v4().simple());

    let holder = enqueue_row(
        &mut conn,
        &queue,
        &RowSpec {
            priority: 0,
            concurrency_key: Some(saturated_key.clone()),
            concurrency_cap: Some(1),
        },
    )
    .await;
    force_running(&mut conn, holder, "holder").await;

    // 12 poisoned rows, all the SAME priority -- a real tie, resolved only
    // by scheduled_at/id. A small batch_size (5) forces several batch
    // fetches to walk past all of them.
    let mut poisoned = Vec::new();
    for _ in 0..12 {
        poisoned.push(
            enqueue_row(
                &mut conn,
                &queue,
                &RowSpec {
                    priority: 10,
                    concurrency_key: Some(saturated_key.clone()),
                    concurrency_cap: Some(1),
                },
            )
            .await,
        );
    }
    let claimable = enqueue_row(
        &mut conn,
        &queue,
        &RowSpec {
            priority: 1,
            concurrency_key: None,
            concurrency_cap: None,
        },
    )
    .await;

    let config = BatchedClaimConfig {
        batch_size: 5,
        max_batches: 10,
    };
    let claimed = batched_claim_one(&mut conn, &queue, "w3", config)
        .await
        .expect("the claimable row must be found across multiple batches");
    assert_eq!(claimed, claimable);

    for id in poisoned {
        assert_eq!(task_state(&mut conn, id).await, "PENDING");
    }
}

/// `max_batches` exhaustion must give up cleanly -- `None`, no poisoned row
/// touched -- rather than looping forever or claiming past the cap.
#[tokio::test(flavor = "multi_thread")]
async fn batched_claim_gives_up_after_max_batches_without_claiming_poisoned_rows() {
    let (_url, mut conn, _container) = setup_db().await;
    let queue = unique_queue("batched-exhaustion");
    let saturated_key = format!("key-{}", Uuid::new_v4().simple());

    let holder = enqueue_row(
        &mut conn,
        &queue,
        &RowSpec {
            priority: 0,
            concurrency_key: Some(saturated_key.clone()),
            concurrency_cap: Some(1),
        },
    )
    .await;
    force_running(&mut conn, holder, "holder").await;

    // 30 poisoned rows; the claimable row would only be reached in batch 4
    // at batch_size=10, but max_batches caps the search at 2.
    let mut poisoned = Vec::new();
    for _ in 0..30 {
        poisoned.push(
            enqueue_row(
                &mut conn,
                &queue,
                &RowSpec {
                    priority: 10,
                    concurrency_key: Some(saturated_key.clone()),
                    concurrency_cap: Some(1),
                },
            )
            .await,
        );
    }
    let claimable = enqueue_row(
        &mut conn,
        &queue,
        &RowSpec {
            priority: 1,
            concurrency_key: None,
            concurrency_cap: None,
        },
    )
    .await;

    let config = BatchedClaimConfig {
        batch_size: 10,
        max_batches: 2,
    };
    let claimed = batched_claim_one(&mut conn, &queue, "w4", config).await;
    assert_eq!(
        claimed, None,
        "search must stop at max_batches rather than continuing indefinitely"
    );
    assert_eq!(task_state(&mut conn, claimable).await, "PENDING");
    for id in poisoned {
        assert_eq!(task_state(&mut conn, id).await, "PENDING");
    }
}

// ── Real concurrent claimers -- the gap ledger #5's apparatus could not close ──

/// `N` concurrent claimers racing a backlog of rows sharing one
/// `concurrency_cap`-capped key must never push RUNNING rows for that key
/// past the cap. The count of successful claims must also exactly equal
/// the cap -- not more, and, since every row is otherwise identical,
/// not fewer either.
///
/// This is the exact property `pg_try_advisory_xact_lock` plus the fresh
/// per-candidate `COUNT` exists to guarantee, now checked under REAL
/// concurrency rather than one serial walk.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_batched_claimers_never_exceed_the_concurrency_cap() {
    let (url, mut conn, _container) = setup_db().await;
    let queue = unique_queue("batched-concurrent-cap");
    let key = format!("key-{}", Uuid::new_v4().simple());
    const CAP: u32 = 3;
    const CLAIMERS: usize = 10;

    for _ in 0..CLAIMERS {
        enqueue_row(
            &mut conn,
            &queue,
            &RowSpec {
                priority: 1,
                concurrency_key: Some(key.clone()),
                concurrency_cap: Some(CAP),
            },
        )
        .await;
    }

    let mut handles = Vec::new();
    for i in 0..CLAIMERS {
        let url = url.clone();
        let queue = queue.clone();
        handles.push(tokio::spawn(async move {
            let mut conn = connect(&url).await;
            batched_claim_one(
                &mut conn,
                &queue,
                &format!("concurrent-worker-{i}"),
                BatchedClaimConfig::default(),
            )
            .await
        }));
    }

    let mut claimed = 0usize;
    for handle in handles {
        if handle.await.expect("join").is_some() {
            claimed += 1;
        }
    }

    // `pg_try_advisory_xact_lock` does not block, as in `claim_task_on_shard`.
    // A claimer that loses the lock race gets 0 rows and retries on the next
    // poll, even when the cap is not saturated. So a round can land BELOW the
    // cap, which is safe. It must never land ABOVE it. One winner is the
    // soundness floor (10 claimers race 10 identical rows).
    assert!(
        claimed >= 1 && claimed <= CAP as usize,
        "claimed count must be in (0, CAP] -- non-blocking lock contention \
         can under-claim in one racing round but must never over-claim; got \
         {claimed}"
    );

    use diesel_async::RunQueryDsl;
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let running = diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_task_queue \
         WHERE concurrency_key = $1 AND state = 'RUNNING' AND worker_id IS NOT NULL",
    )
    .bind::<diesel::sql_types::Text, _>(&key)
    .get_result::<Count>(&mut conn)
    .await
    .expect("count running");
    assert!(
        running.n <= i64::from(CAP),
        "the cap must never be exceeded under real concurrent claimers; got {}",
        running.n
    );
    assert_eq!(
        running.n,
        i64::try_from(claimed).expect("claimed fits i64"),
        "every successful claim must correspond to exactly one RUNNING row \
         under this key, and vice versa"
    );
}

// ── Rate limiting (issues #332 / #699) ──────────────────────────────────────

/// A rate-limited row with an empty bucket must not be claimed; topping the
/// bucket up must make it claimable, debiting exactly one token.
#[tokio::test(flavor = "multi_thread")]
async fn batched_claim_respects_the_rate_limit_bucket() {
    let (_url, mut conn, _container) = setup_db().await;
    let queue = unique_queue("batched-rate-limit");
    let bucket_key = format!("bucket-{}", Uuid::new_v4().simple());

    // burst=1, refill_rate=0: room for one token, and no refill.
    // `ensure_rate_limit_bucket` starts `tokens` at `burst`, so drain it for
    // the empty start. A burst of 0 would cap the LEAST(burst, ...) of
    // `effective_available_tokens_expr` at 0, and no top-up could clear it.
    queue::ensure_rate_limit_bucket(&mut conn, &bucket_key, 0.0, 1.0)
        .await
        .expect("ensure bucket");
    use diesel_async::RunQueryDsl;
    diesel::sql_query("UPDATE harvest_rate_limit_buckets SET tokens = 0.0 WHERE key = $1")
        .bind::<diesel::sql_types::Text, _>(&bucket_key)
        .execute(&mut conn)
        .await
        .expect("drain bucket");

    let exec_id = insert_execution(&mut conn).await;
    let mut params = EnqueueParams::new(&queue, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = Some("noop".to_string());
    params.activity_id = Some(Uuid::new_v4());
    params.rate_limit_key = Some(bucket_key.clone());
    let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");

    let claimed = batched_claim_one(&mut conn, &queue, "w5", BatchedClaimConfig::default()).await;
    assert_eq!(
        claimed, None,
        "an empty, non-refilling bucket must block the claim"
    );
    assert_eq!(task_state(&mut conn, task_id).await, "PENDING");

    // Top the bucket up to exactly one token (its full burst capacity).
    diesel::sql_query("UPDATE harvest_rate_limit_buckets SET tokens = 1.0 WHERE key = $1")
        .bind::<diesel::sql_types::Text, _>(&bucket_key)
        .execute(&mut conn)
        .await
        .expect("top up bucket");

    let claimed = batched_claim_one(&mut conn, &queue, "w5", BatchedClaimConfig::default())
        .await
        .expect("a funded bucket must allow the claim");
    assert_eq!(claimed, task_id);

    #[derive(diesel::QueryableByName)]
    struct Tokens {
        #[diesel(sql_type = diesel::sql_types::Double)]
        tokens: f64,
    }
    let remaining =
        diesel::sql_query("SELECT tokens FROM harvest_rate_limit_buckets WHERE key = $1")
            .bind::<diesel::sql_types::Text, _>(&bucket_key)
            .get_result::<Tokens>(&mut conn)
            .await
            .expect("tokens")
            .tokens;
    assert!(
        remaining.abs() < 1e-9,
        "exactly one token must be debited; got {remaining}"
    );
}

/// A deadline that passes mid-transaction blocks the claim.
///
/// A candidate whose `schedule_to_close_at` passes in REAL wall-clock time,
/// while a batch search still walks earlier candidates, must not be claimed.
/// The frozen transaction `NOW()` of the batch scan would have let it through.
///
/// The test drives `claim_batched_candidate_attempt_query()` in a
/// hand-managed transaction. A `pg_sleep` stands in for "a long batch
/// search", so the test is deterministic. It walks no poisoned candidates.
#[tokio::test(flavor = "multi_thread")]
async fn batched_claim_attempt_rejects_a_deadline_that_passed_mid_transaction() {
    use diesel_async::RunQueryDsl;

    let (_url, mut conn, _container) = setup_db().await;
    let queue = unique_queue("batched-deadline");
    let exec_id = insert_execution(&mut conn).await;
    let mut params = EnqueueParams::new(&queue, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = Some("noop".to_string());
    params.activity_id = Some(Uuid::new_v4());
    // Due already, with a deadline 300ms out -- comfortably true at
    // enqueue time, and at the transaction's own BEGIN moments later.
    let deadline = chrono::Utc::now() + chrono::Duration::milliseconds(300);
    params.schedule_to_close_at = Some(deadline);
    let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");

    #[derive(diesel::QueryableByName)]
    struct ClaimedId {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
    }

    let mut tx = conn.build_transaction().read_committed();
    let claimed: Option<Uuid> = tx
        .run(
            async |conn: &mut AsyncPgConnection| -> Result<Option<Uuid>, diesel::result::Error> {
                // Real wall-clock time advances 600ms here. The frozen NOW()
                // of this transaction does not, so `deadline` stays ahead of
                // NOW() for the rest of it.
                diesel::sql_query("SELECT pg_sleep(0.6)")
                    .execute(conn)
                    .await?;

                let rows: Vec<ClaimedId> =
                    diesel::sql_query(queue::claim_batched_candidate_attempt_query())
                        .bind::<diesel::sql_types::Text, _>("deadline-tester")
                        .bind::<diesel::sql_types::Uuid, _>(task_id)
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                            None::<String>,
                        )
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(
                            None::<i32>,
                        )
                        .bind::<diesel::sql_types::Text, _>("activity")
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                            None::<String>,
                        )
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                            None::<String>,
                        )
                        .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(
                            &Vec::<String>::new(),
                        )
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>, _>(
                            Some(deadline),
                        )
                        .bind::<diesel::sql_types::Text, _>("")
                        .load(conn)
                        .await?;
                Ok(rows.into_iter().next().map(|r| r.id))
            },
        )
        .await
        .expect("transaction");

    assert_eq!(
        claimed, None,
        "a deadline that passed in real time mid-transaction must reject \
         the claim, even though this transaction's own frozen NOW() never \
         moved past it"
    );
    assert_eq!(task_state(&mut conn, task_id).await, "PENDING");
}

/// A candidate whose deadline passed mid-transaction debits no rate-limit
/// token.
///
/// A data-modifying CTE runs whether or not a later CTE reads its result.
/// `claimed` rejects the candidate anyway. The concurrency-rejected leak
/// test below has the same shape, on the concurrency recheck.
#[tokio::test(flavor = "multi_thread")]
async fn batched_claim_attempt_never_debits_rate_limit_for_a_candidate_past_its_deadline() {
    use diesel_async::RunQueryDsl;

    let (_url, mut conn, _container) = setup_db().await;
    let bucket_key = format!("bucket-{}", Uuid::new_v4().simple());
    queue::ensure_rate_limit_bucket(&mut conn, &bucket_key, 0.0, 100.0)
        .await
        .expect("ensure bucket");

    let queue = unique_queue("batched-deadline-no-leak");
    let exec_id = insert_execution(&mut conn).await;
    let mut params = EnqueueParams::new(&queue, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = Some("noop".to_string());
    params.activity_id = Some(Uuid::new_v4());
    params.rate_limit_key = Some(bucket_key.clone());
    let deadline = chrono::Utc::now() + chrono::Duration::milliseconds(300);
    params.schedule_to_close_at = Some(deadline);
    let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");

    #[derive(diesel::QueryableByName)]
    struct ClaimedId {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
    }

    let mut tx = conn.build_transaction().read_committed();
    let claimed: Option<Uuid> = tx
        .run(
            async |conn: &mut AsyncPgConnection| -> Result<Option<Uuid>, diesel::result::Error> {
                diesel::sql_query("SELECT pg_sleep(0.6)")
                    .execute(conn)
                    .await?;

                let rows: Vec<ClaimedId> =
                    diesel::sql_query(queue::claim_batched_candidate_attempt_query())
                        .bind::<diesel::sql_types::Text, _>("deadline-no-leak-tester")
                        .bind::<diesel::sql_types::Uuid, _>(task_id)
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                            None::<String>,
                        )
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(
                            None::<i32>,
                        )
                        .bind::<diesel::sql_types::Text, _>("activity")
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(Some(
                            bucket_key.clone(),
                        ))
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                            None::<String>,
                        )
                        .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(
                            &Vec::<String>::new(),
                        )
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>, _>(
                            Some(deadline),
                        )
                        .bind::<diesel::sql_types::Text, _>("")
                        .load(conn)
                        .await?;
                Ok(rows.into_iter().next().map(|r| r.id))
            },
        )
        .await
        .expect("transaction");

    assert_eq!(
        claimed, None,
        "a deadline that passed in real time mid-transaction must reject \
         the claim"
    );
    assert_eq!(task_state(&mut conn, task_id).await, "PENDING");

    #[derive(diesel::QueryableByName)]
    struct Tokens {
        #[diesel(sql_type = diesel::sql_types::Double)]
        tokens: f64,
    }
    let remaining =
        diesel::sql_query("SELECT tokens FROM harvest_rate_limit_buckets WHERE key = $1")
            .bind::<diesel::sql_types::Text, _>(&bucket_key)
            .get_result::<Tokens>(&mut conn)
            .await
            .expect("tokens")
            .tokens;
    assert!(
        (remaining - 100.0).abs() < 1e-9,
        "the debit CTE must not spend a token for a candidate whose \
         deadline already passed -- claimed was always going to reject \
         it, so the debit must too; got {remaining}"
    );
}

/// A candidate with no declared build compatibility debits no rate-limit
/// token.
///
/// As in the deadline-leak test above, a data-modifying CTE runs whether or
/// not `claimed` uses its result. `claimed` rejects the candidate on the
/// build-routing gate.
#[tokio::test(flavor = "multi_thread")]
async fn batched_claim_attempt_never_debits_rate_limit_for_a_build_incompatible_candidate() {
    use diesel_async::RunQueryDsl;

    let (_url, mut conn, _container) = setup_db().await;
    let bucket_key = format!("bucket-{}", Uuid::new_v4().simple());
    queue::ensure_rate_limit_bucket(&mut conn, &bucket_key, 0.0, 100.0)
        .await
        .expect("ensure bucket");

    let queue = unique_queue("batched-build-routing-no-leak");
    let unique = Uuid::new_v4().simple().to_string();
    let required_build = format!("build-required-{unique}");
    let worker_build = format!("build-worker-{unique}");
    let exec_id = insert_execution(&mut conn).await;
    let mut params = EnqueueParams::new(&queue, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = Some("noop".to_string());
    params.activity_id = Some(Uuid::new_v4());
    params.rate_limit_key = Some(bucket_key.clone());
    params.required_build_id = Some(required_build.clone());
    let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");

    #[derive(diesel::QueryableByName)]
    struct ClaimedId {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
    }

    let mut tx = conn.build_transaction().read_committed();
    let claimed: Option<Uuid> = tx
        .run(
            async |conn: &mut AsyncPgConnection| -> Result<Option<Uuid>, diesel::result::Error> {
                let rows: Vec<ClaimedId> =
                    diesel::sql_query(queue::claim_batched_candidate_attempt_query())
                        .bind::<diesel::sql_types::Text, _>("build-no-leak-tester")
                        .bind::<diesel::sql_types::Uuid, _>(task_id)
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                            None::<String>,
                        )
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(
                            None::<i32>,
                        )
                        .bind::<diesel::sql_types::Text, _>("activity")
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(Some(
                            bucket_key.clone(),
                        ))
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                            None::<String>,
                        )
                        .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(
                            &Vec::<String>::new(),
                        )
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>, _>(
                            None::<chrono::DateTime<chrono::Utc>>,
                        )
                        .bind::<diesel::sql_types::Text, _>(&worker_build)
                        .load(conn)
                        .await?;
                Ok(rows.into_iter().next().map(|r| r.id))
            },
        )
        .await
        .expect("transaction");

    assert_eq!(
        claimed, None,
        "an undeclared worker/required-build pair must reject the claim"
    );
    assert_eq!(task_state(&mut conn, task_id).await, "PENDING");

    #[derive(diesel::QueryableByName)]
    struct Tokens {
        #[diesel(sql_type = diesel::sql_types::Double)]
        tokens: f64,
    }
    let remaining =
        diesel::sql_query("SELECT tokens FROM harvest_rate_limit_buckets WHERE key = $1")
            .bind::<diesel::sql_types::Text, _>(&bucket_key)
            .get_result::<Tokens>(&mut conn)
            .await
            .expect("tokens")
            .tokens;
    assert!(
        (remaining - 100.0).abs() < 1e-9,
        "the debit CTE must not spend a token for a candidate whose \
         build is not compatible -- claimed was always going to reject \
         it, so the debit must too; got {remaining}"
    );
}

/// A candidate whose worker does not satisfy `required_capabilities` debits
/// no rate-limit token.
///
/// The build-routing and deadline leak tests above have the same shape.
#[tokio::test(flavor = "multi_thread")]
async fn batched_claim_attempt_never_debits_rate_limit_for_a_capability_mismatched_candidate() {
    use diesel_async::RunQueryDsl;

    let (_url, mut conn, _container) = setup_db().await;
    let bucket_key = format!("bucket-{}", Uuid::new_v4().simple());
    queue::ensure_rate_limit_bucket(&mut conn, &bucket_key, 0.0, 100.0)
        .await
        .expect("ensure bucket");

    let worker_id = format!("capability-no-leak-tester-{}", Uuid::new_v4().simple());
    diesel::sql_query(
        "INSERT INTO harvest_workers \
         (worker_id, queues, shard_assignments, max_concurrency, host, build_id, labels) \
         VALUES ($1, '[]'::jsonb, '[]'::jsonb, 1, 'test-host', '', '{}'::jsonb)",
    )
    .bind::<diesel::sql_types::Text, _>(&worker_id)
    .execute(&mut conn)
    .await
    .expect("insert worker");

    let queue = unique_queue("batched-capability-no-leak");
    let exec_id = insert_execution(&mut conn).await;
    let mut params = EnqueueParams::new(&queue, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = Some("noop".to_string());
    params.activity_id = Some(Uuid::new_v4());
    params.rate_limit_key = Some(bucket_key.clone());
    params.required_capabilities = Some(serde_json::json!([
        {"Exact": {"key": "region", "value": "us-east"}}
    ]));
    let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");

    #[derive(diesel::QueryableByName)]
    struct ClaimedId {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
    }

    let mut tx = conn.build_transaction().read_committed();
    let claimed: Option<Uuid> = tx
        .run(
            async |conn: &mut AsyncPgConnection| -> Result<Option<Uuid>, diesel::result::Error> {
                let rows: Vec<ClaimedId> =
                    diesel::sql_query(queue::claim_batched_candidate_attempt_query())
                        .bind::<diesel::sql_types::Text, _>(&worker_id)
                        .bind::<diesel::sql_types::Uuid, _>(task_id)
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                            None::<String>,
                        )
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(
                            None::<i32>,
                        )
                        .bind::<diesel::sql_types::Text, _>("activity")
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(Some(
                            bucket_key.clone(),
                        ))
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                            None::<String>,
                        )
                        .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(
                            &Vec::<String>::new(),
                        )
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>, _>(
                            None::<chrono::DateTime<chrono::Utc>>,
                        )
                        .bind::<diesel::sql_types::Text, _>("")
                        .load(conn)
                        .await?;
                Ok(rows.into_iter().next().map(|r| r.id))
            },
        )
        .await
        .expect("transaction");

    assert_eq!(
        claimed, None,
        "a worker whose labels do not satisfy required_capabilities must \
         reject the claim"
    );
    assert_eq!(task_state(&mut conn, task_id).await, "PENDING");

    #[derive(diesel::QueryableByName)]
    struct Tokens {
        #[diesel(sql_type = diesel::sql_types::Double)]
        tokens: f64,
    }
    let remaining =
        diesel::sql_query("SELECT tokens FROM harvest_rate_limit_buckets WHERE key = $1")
            .bind::<diesel::sql_types::Text, _>(&bucket_key)
            .get_result::<Tokens>(&mut conn)
            .await
            .expect("tokens")
            .tokens;
    assert!(
        (remaining - 100.0).abs() < 1e-9,
        "the debit CTE must not spend a token for a candidate whose \
         worker does not satisfy required_capabilities -- claimed was \
         always going to reject it, so the debit must too; \
         got {remaining}"
    );
}

/// `claimed` re-checks capability labels at the attempt.
///
/// The capability-label gate of the batch scan filters only at scan time,
/// the class of gap that the build-routing fix closed. Labels can change
/// before the attempt, a later statement. So the `WHERE` of `claimed` reads
/// `harvest_workers` again and does not trust the stale scan.
///
/// The test drives `claim_batched_candidate_attempt_query()` directly, as
/// the build-routing test does. A worker without the labels is rejected.
/// Labels that satisfy the requirement then make the same re-check pass.
#[tokio::test(flavor = "multi_thread")]
async fn batched_claim_attempt_rechecks_capability_labels_not_just_the_batch_scan() {
    use diesel_async::RunQueryDsl;

    let (_url, mut conn, _container) = setup_db().await;
    let worker_id = format!("capability-tester-{}", Uuid::new_v4().simple());
    diesel::sql_query(
        "INSERT INTO harvest_workers \
         (worker_id, queues, shard_assignments, max_concurrency, host, build_id, labels) \
         VALUES ($1, '[]'::jsonb, '[]'::jsonb, 1, 'test-host', '', '{}'::jsonb)",
    )
    .bind::<diesel::sql_types::Text, _>(&worker_id)
    .execute(&mut conn)
    .await
    .expect("insert worker");

    let queue = unique_queue("batched-capability-labels");
    let exec_id = insert_execution(&mut conn).await;
    let mut params = EnqueueParams::new(&queue, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = Some("noop".to_string());
    params.activity_id = Some(Uuid::new_v4());
    params.required_capabilities = Some(serde_json::json!([
        {"Exact": {"key": "region", "value": "us-east"}}
    ]));
    let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");

    #[derive(diesel::QueryableByName)]
    struct ClaimedId {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
    }
    async fn try_attempt(
        conn: &mut AsyncPgConnection,
        worker_id: &str,
        task_id: Uuid,
    ) -> Option<Uuid> {
        let mut tx = conn.build_transaction().read_committed();
        tx.run(
            async |conn: &mut AsyncPgConnection| -> Result<Option<Uuid>, diesel::result::Error> {
                let rows: Vec<ClaimedId> =
                    diesel::sql_query(queue::claim_batched_candidate_attempt_query())
                        .bind::<diesel::sql_types::Text, _>(worker_id)
                        .bind::<diesel::sql_types::Uuid, _>(task_id)
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                            None::<String>,
                        )
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(
                            None::<i32>,
                        )
                        .bind::<diesel::sql_types::Text, _>("activity")
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                            None::<String>,
                        )
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                            None::<String>,
                        )
                        .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(
                            &Vec::<String>::new(),
                        )
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>, _>(
                            None::<chrono::DateTime<chrono::Utc>>,
                        )
                        .bind::<diesel::sql_types::Text, _>("")
                        .load(conn)
                        .await?;
                Ok(rows.into_iter().next().map(|r| r.id))
            },
        )
        .await
        .expect("transaction")
    }

    // The worker's labels are empty: does not satisfy "region=us-east".
    let claimed = try_attempt(&mut conn, &worker_id, task_id).await;
    assert_eq!(claimed, None);
    assert_eq!(task_state(&mut conn, task_id).await, "PENDING");

    // New labels make the same re-check pass, so it reads current state.
    diesel::sql_query("UPDATE harvest_workers SET labels = $2 WHERE worker_id = $1")
        .bind::<diesel::sql_types::Text, _>(&worker_id)
        .bind::<diesel::sql_types::Jsonb, _>(serde_json::json!({"region": "us-east"}))
        .execute(&mut conn)
        .await
        .expect("update worker labels");
    let claimed = try_attempt(&mut conn, &worker_id, task_id).await;
    assert_eq!(claimed, Some(task_id));
    assert_eq!(task_state(&mut conn, task_id).await, "RUNNING");
}

/// A batch walk does NOT debit a rate-limit token for a candidate that the
/// concurrency gate always rejects.
///
/// Each poisoned row shares BOTH a saturated `concurrency_key` AND a
/// `rate_limit_key` with many tokens.
/// `queue::claim_batched_candidate_concurrency_probe_query` closes that
/// combination. Before that fix, the walk debited one token per row tried.
#[tokio::test(flavor = "multi_thread")]
async fn batched_claim_never_debits_rate_limit_for_a_concurrency_rejected_candidate() {
    let (_url, mut conn, _container) = setup_db().await;
    let queue = unique_queue("batched-no-leak");
    let saturated_key = format!("key-{}", Uuid::new_v4().simple());
    let bucket_key = format!("bucket-{}", Uuid::new_v4().simple());

    queue::ensure_rate_limit_bucket(&mut conn, &bucket_key, 0.0, 100.0)
        .await
        .expect("ensure bucket");

    let holder = enqueue_row(
        &mut conn,
        &queue,
        &RowSpec {
            priority: 0,
            concurrency_key: Some(saturated_key.clone()),
            concurrency_cap: Some(1),
        },
    )
    .await;
    force_running(&mut conn, holder, "holder").await;

    // 10 poisoned rows: saturated concurrency key AND a funded rate-limit
    // bucket. Each one that reaches the debit step would consume a token.
    for _ in 0..10 {
        let exec_id = insert_execution(&mut conn).await;
        let mut params = EnqueueParams::new(&queue, TaskType::Activity, serde_json::json!({}));
        params.workflow_exec_id = Some(exec_id);
        params.activity_name = Some("noop".to_string());
        params.activity_id = Some(Uuid::new_v4());
        params.priority = 10;
        params.concurrency_key = Some(saturated_key.clone());
        params.max_concurrent = Some(1);
        params.rate_limit_key = Some(bucket_key.clone());
        queue::enqueue(&mut conn, &params).await.expect("enqueue");
    }

    let claimable = enqueue_row(
        &mut conn,
        &queue,
        &RowSpec {
            priority: 1,
            concurrency_key: None,
            concurrency_cap: None,
        },
    )
    .await;

    let claimed = batched_claim_one(&mut conn, &queue, "w6", BatchedClaimConfig::default())
        .await
        .expect("the claimable row must be found despite the poisoned rows ahead of it");
    assert_eq!(claimed, claimable);

    use diesel_async::RunQueryDsl;
    #[derive(diesel::QueryableByName)]
    struct Tokens {
        #[diesel(sql_type = diesel::sql_types::Double)]
        tokens: f64,
    }
    let remaining =
        diesel::sql_query("SELECT tokens FROM harvest_rate_limit_buckets WHERE key = $1")
            .bind::<diesel::sql_types::Text, _>(&bucket_key)
            .get_result::<Tokens>(&mut conn)
            .await
            .expect("tokens")
            .tokens;
    assert!(
        (remaining - 100.0).abs() < 1e-9,
        "none of the 10 poisoned rows may debit a token -- the winning row \
         has no rate_limit_key, so the bucket must stay at its starting \
         100.0; got {remaining}"
    );
}

/// `started_at` reads the materialized `clock_timestamp()` of the claim.
///
/// A batch walk can spend real time inside one transaction, and `NOW()`
/// stays frozen at its start. `started_at = NOW()` would backdate the claim
/// by the walk. That shortens the `start_to_close` and `heartbeat_timeout`
/// budgets, which both run from `started_at`.
#[tokio::test(flavor = "multi_thread")]
async fn batched_claim_attempt_stamps_started_at_with_real_time_not_frozen_now() {
    use diesel_async::RunQueryDsl;

    let (_url, mut conn, _container) = setup_db().await;
    let queue = unique_queue("batched-started-at");
    let exec_id = insert_execution(&mut conn).await;
    let mut params = EnqueueParams::new(&queue, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = Some("noop".to_string());
    params.activity_id = Some(Uuid::new_v4());
    let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");

    let before_sleep = chrono::Utc::now();

    #[derive(diesel::QueryableByName)]
    struct ClaimedRow {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
        started_at: Option<chrono::DateTime<chrono::Utc>>,
    }

    let mut tx = conn.build_transaction().read_committed();
    let claimed: ClaimedRow = tx
        .run(
            async |conn: &mut AsyncPgConnection| -> Result<ClaimedRow, diesel::result::Error> {
                // Real wall-clock time advances 600ms here. This
                // transaction's own frozen NOW() does not.
                diesel::sql_query("SELECT pg_sleep(0.6)")
                    .execute(conn)
                    .await?;

                let rows: Vec<ClaimedRow> =
                    diesel::sql_query(queue::claim_batched_candidate_attempt_query())
                        .bind::<diesel::sql_types::Text, _>("started-at-tester")
                        .bind::<diesel::sql_types::Uuid, _>(task_id)
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                            None::<String>,
                        )
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(
                            None::<i32>,
                        )
                        .bind::<diesel::sql_types::Text, _>("activity")
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                            None::<String>,
                        )
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                            None::<String>,
                        )
                        .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(
                            &Vec::<String>::new(),
                        )
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>, _>(
                            None::<chrono::DateTime<chrono::Utc>>,
                        )
                        .bind::<diesel::sql_types::Text, _>("")
                        .load(conn)
                        .await?;
                Ok(rows.into_iter().next().expect("the row must be claimed"))
            },
        )
        .await
        .expect("transaction");

    assert_eq!(claimed.id, task_id);
    let started_at = claimed.started_at.expect("started_at must be set on claim");
    assert!(
        started_at > before_sleep + chrono::Duration::milliseconds(400),
        "started_at must reflect the real time of the claim, after the \
         600ms in-transaction sleep -- a frozen NOW() would stamp it near \
         before_sleep instead; before_sleep={before_sleep}, \
         started_at={started_at}"
    );
    assert_eq!(task_state(&mut conn, task_id).await, "RUNNING");
}

/// `now_ts` reads the clock after the wait for the bucket row lock.
///
/// `now_ts` has no `FROM` clause, so Postgres could resolve it before
/// `rate_limit_debit` takes its row lock. With a concurrent lock holder,
/// `now_ts` would capture a stale, pre-wait value. `rate_limit_debit`,
/// `claimed` and `started_at` all reuse it. A deadline that expires DURING
/// the wait would then still read as live.
///
/// Two real connections reproduce the race. One holds the bucket row lock
/// past the deadline. The other attempts the claim and blocks on that lock.
/// A `pg_sleep` cannot stand in: the bug is WHEN the wait resolves against
/// the clock read.
#[tokio::test(flavor = "multi_thread")]
async fn batched_claim_attempt_rejects_a_deadline_that_passes_while_waiting_on_the_bucket_lock() {
    use diesel_async::RunQueryDsl;

    let (url, mut conn, _container) = setup_db().await;
    let bucket_key = format!("bucket-{}", Uuid::new_v4().simple());
    queue::ensure_rate_limit_bucket(&mut conn, &bucket_key, 0.0, 100.0)
        .await
        .expect("ensure bucket");

    let queue = unique_queue("batched-lock-wait-deadline");
    let exec_id = insert_execution(&mut conn).await;
    let mut params = EnqueueParams::new(&queue, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = Some("noop".to_string());
    params.activity_id = Some(Uuid::new_v4());
    params.rate_limit_key = Some(bucket_key.clone());
    // Expires well before the locker below releases the bucket row.
    let deadline = chrono::Utc::now() + chrono::Duration::milliseconds(500);
    params.schedule_to_close_at = Some(deadline);
    let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");

    let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
    let locker_bucket_key = bucket_key.clone();
    let locker_url = url.clone();
    let locker = tokio::spawn(async move {
        let mut locker_conn = connect(&locker_url).await;
        let mut tx = locker_conn.build_transaction().read_committed();
        tx.run(
            async |conn: &mut AsyncPgConnection| -> Result<(), diesel::result::Error> {
                diesel::sql_query(
                    "SELECT tokens FROM harvest_rate_limit_buckets WHERE key = $1 FOR UPDATE",
                )
                .bind::<diesel::sql_types::Text, _>(&locker_bucket_key)
                .execute(conn)
                .await?;
                let _ = locked_tx.send(());
                // Holds the row lock well past the 500ms deadline above.
                diesel::sql_query("SELECT pg_sleep(1.2)")
                    .execute(conn)
                    .await?;
                Ok(())
            },
        )
        .await
        .expect("locker transaction");
    });

    // Wait until the locker holds the row lock. A fixed sleep cannot
    // guarantee that on a loaded CI host.
    locked_rx.await.expect("locker signaled");

    #[derive(diesel::QueryableByName)]
    struct ClaimedId {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
    }

    let mut tx = conn.build_transaction().read_committed();
    let claimed: Option<Uuid> = tx
        .run(
            async |conn: &mut AsyncPgConnection| -> Result<Option<Uuid>, diesel::result::Error> {
                let rows: Vec<ClaimedId> =
                    diesel::sql_query(queue::claim_batched_candidate_attempt_query())
                        .bind::<diesel::sql_types::Text, _>("lock-wait-tester")
                        .bind::<diesel::sql_types::Uuid, _>(task_id)
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                            None::<String>,
                        )
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(
                            None::<i32>,
                        )
                        .bind::<diesel::sql_types::Text, _>("activity")
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(Some(
                            bucket_key.clone(),
                        ))
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                            None::<String>,
                        )
                        .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(
                            &Vec::<String>::new(),
                        )
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>, _>(
                            Some(deadline),
                        )
                        .bind::<diesel::sql_types::Text, _>("")
                        .load(conn)
                        .await?;
                Ok(rows.into_iter().next().map(|r| r.id))
            },
        )
        .await
        .expect("transaction");

    locker.await.expect("locker joined");

    assert_eq!(
        claimed, None,
        "the deadline passed in real time while this attempt waited on \
         the bucket row lock -- now_ts must reflect that wait, not a \
         value captured before it started"
    );
    assert_eq!(task_state(&mut conn, task_id).await, "PENDING");

    #[derive(diesel::QueryableByName)]
    struct Tokens {
        #[diesel(sql_type = diesel::sql_types::Double)]
        tokens: f64,
    }
    let remaining =
        diesel::sql_query("SELECT tokens FROM harvest_rate_limit_buckets WHERE key = $1")
            .bind::<diesel::sql_types::Text, _>(&bucket_key)
            .get_result::<Tokens>(&mut conn)
            .await
            .expect("tokens")
            .tokens;
    assert!(
        (remaining - 100.0).abs() < 1e-9,
        "the debit must not spend a token for a candidate whose deadline \
         passed while waiting on the lock; got {remaining}"
    );
}

/// A circuit-breaker-bypassed claim does not wait on the bucket row lock.
///
/// This covers the `now_ts` fix above. `rate_limit_debit` never touches the
/// bucket row of a bypassed activity, so the forced lock of `now_ts` skips
/// it too. A bypassed claim runs at full speed, past rate limiting.
/// Otherwise it waits behind an unrelated transaction that holds the row.
///
/// A separate connection holds a real lock, not a `pg_sleep` stand-in. The
/// bypassed claim must still complete quickly.
#[tokio::test(flavor = "multi_thread")]
async fn batched_claim_attempt_skips_the_bucket_lock_for_a_circuit_breaker_activity() {
    use diesel_async::RunQueryDsl;

    let (url, mut conn, _container) = setup_db().await;
    let bucket_key = format!("bucket-{}", Uuid::new_v4().simple());
    queue::ensure_rate_limit_bucket(&mut conn, &bucket_key, 0.0, 100.0)
        .await
        .expect("ensure bucket");

    let queue = unique_queue("batched-breaker-no-block");
    let exec_id = insert_execution(&mut conn).await;
    let mut params = EnqueueParams::new(&queue, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = Some("breaker-bypassed".to_string());
    params.activity_id = Some(Uuid::new_v4());
    params.rate_limit_key = Some(bucket_key.clone());
    let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");

    let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
    let locker_bucket_key = bucket_key.clone();
    let locker_url = url.clone();
    let locker = tokio::spawn(async move {
        let mut locker_conn = connect(&locker_url).await;
        let mut tx = locker_conn.build_transaction().read_committed();
        tx.run(
            async |conn: &mut AsyncPgConnection| -> Result<(), diesel::result::Error> {
                diesel::sql_query(
                    "SELECT tokens FROM harvest_rate_limit_buckets WHERE key = $1 FOR UPDATE",
                )
                .bind::<diesel::sql_types::Text, _>(&locker_bucket_key)
                .execute(conn)
                .await?;
                let _ = locked_tx.send(());
                diesel::sql_query("SELECT pg_sleep(1.5)")
                    .execute(conn)
                    .await?;
                Ok(())
            },
        )
        .await
        .expect("locker transaction");
    });

    // Wait until the locker holds the row lock. A fixed sleep cannot
    // guarantee that on a loaded CI host.
    locked_rx.await.expect("locker signaled");

    #[derive(diesel::QueryableByName)]
    struct ClaimedId {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
    }

    let start = std::time::Instant::now();
    let mut tx = conn.build_transaction().read_committed();
    let claimed: Option<Uuid> = tx
        .run(
            async |conn: &mut AsyncPgConnection| -> Result<Option<Uuid>, diesel::result::Error> {
                let rows: Vec<ClaimedId> =
                    diesel::sql_query(queue::claim_batched_candidate_attempt_query())
                        .bind::<diesel::sql_types::Text, _>("breaker-no-block-tester")
                        .bind::<diesel::sql_types::Uuid, _>(task_id)
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                            None::<String>,
                        )
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(
                            None::<i32>,
                        )
                        .bind::<diesel::sql_types::Text, _>("activity")
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(Some(
                            bucket_key.clone(),
                        ))
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(Some(
                            "breaker-bypassed".to_string(),
                        ))
                        .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(&vec![
                            "breaker-bypassed".to_string(),
                        ])
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>, _>(
                            None::<chrono::DateTime<chrono::Utc>>,
                        )
                        .bind::<diesel::sql_types::Text, _>("")
                        .load(conn)
                        .await?;
                Ok(rows.into_iter().next().map(|r| r.id))
            },
        )
        .await
        .expect("transaction");
    let elapsed = start.elapsed();

    locker.await.expect("locker joined");

    assert_eq!(claimed, Some(task_id));
    assert!(
        elapsed < std::time::Duration::from_millis(500),
        "a circuit-breaker-bypassed claim must not wait on the bucket \
         lock at all -- the locker holds it for 1.5s, so any wait on it \
         would show up here; elapsed={elapsed:?}"
    );
    assert_eq!(task_state(&mut conn, task_id).await, "RUNNING");
}

/// An expired candidate never waits on its own bucket lock.
///
/// A deadline can pass DURING the batch walk, while the walk waits on the
/// bucket lock of an EARLIER candidate. The `now_ts` test above waits on the
/// lock of the candidate itself instead. The later candidate then can never
/// claim, so a second lock wait for it is wasted.
///
/// `try_claim_batched_candidate` must reject an expired candidate before it
/// sends the attempt query. Two real, separately held locks prove it, not a
/// `pg_sleep` stand-in. Candidate A absorbs a real wait. The deadline of
/// candidate B expires during it, and its locked bucket is never waited on.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::similar_names)]
async fn batched_claim_skips_the_bucket_lock_for_a_candidate_already_past_its_deadline() {
    use diesel_async::RunQueryDsl;

    let (url, mut conn, _container) = setup_db().await;
    let bucket_a = format!("bucket-a-{}", Uuid::new_v4().simple());
    let bucket_b = format!("bucket-b-{}", Uuid::new_v4().simple());
    // Exactly one token. The soft rate-limit pre-filter of the batch scan
    // (`{rate_limit_available} >= 1.0`) needs it. Otherwise the scan drops
    // candidate A, and the walk never touches the lock of bucket_a.
    queue::ensure_rate_limit_bucket(&mut conn, &bucket_a, 0.0, 1.0)
        .await
        .expect("ensure bucket a");
    queue::ensure_rate_limit_bucket(&mut conn, &bucket_b, 0.0, 100.0)
        .await
        .expect("ensure bucket b");

    let queue = unique_queue("batched-expired-mid-walk");

    // Candidate A: higher priority, walked first, exists only to make
    // this transaction spend real time waiting on bucket_a's lock.
    let exec_a = insert_execution(&mut conn).await;
    let mut params_a = EnqueueParams::new(&queue, TaskType::Activity, serde_json::json!({}));
    params_a.workflow_exec_id = Some(exec_a);
    params_a.activity_name = Some("noop".to_string());
    params_a.activity_id = Some(Uuid::new_v4());
    params_a.priority = 10;
    params_a.rate_limit_key = Some(bucket_a.clone());
    queue::enqueue(&mut conn, &params_a)
        .await
        .expect("enqueue a");

    // Candidate B: lower priority, walked second. Valid at scan time,
    // but expires well before the walk reaches it.
    let exec_b = insert_execution(&mut conn).await;
    let mut params_b = EnqueueParams::new(&queue, TaskType::Activity, serde_json::json!({}));
    params_b.workflow_exec_id = Some(exec_b);
    params_b.activity_name = Some("noop".to_string());
    params_b.activity_id = Some(Uuid::new_v4());
    params_b.priority = 1;
    params_b.rate_limit_key = Some(bucket_b.clone());
    params_b.schedule_to_close_at = Some(chrono::Utc::now() + chrono::Duration::milliseconds(300));
    let task_b = queue::enqueue(&mut conn, &params_b)
        .await
        .expect("enqueue b");

    let (locked_a_tx, locked_a_rx) = tokio::sync::oneshot::channel();
    let locker_a_bucket = bucket_a.clone();
    let locker_a_url = url.clone();
    let locker_a = tokio::spawn(async move {
        let mut locker_conn = connect(&locker_a_url).await;
        let mut tx = locker_conn.build_transaction().read_committed();
        tx.run(
            async |conn: &mut AsyncPgConnection| -> Result<(), diesel::result::Error> {
                // Lock the row and spend the one token that a concurrent
                // claimer would spend. After this lock, the attempt of A
                // finds no funds and moves on to B.
                diesel::sql_query(
                    "UPDATE harvest_rate_limit_buckets SET tokens = 0 WHERE key = $1",
                )
                .bind::<diesel::sql_types::Text, _>(&locker_a_bucket)
                .execute(conn)
                .await?;
                let _ = locked_a_tx.send(());
                diesel::sql_query("SELECT pg_sleep(1.0)")
                    .execute(conn)
                    .await?;
                Ok(())
            },
        )
        .await
        .expect("locker a transaction");
    });

    let (locked_b_tx, locked_b_rx) = tokio::sync::oneshot::channel();
    let locker_b_bucket = bucket_b.clone();
    let locker_b_url = url.clone();
    let locker_b = tokio::spawn(async move {
        let mut locker_conn = connect(&locker_b_url).await;
        let mut tx = locker_conn.build_transaction().read_committed();
        tx.run(
            async |conn: &mut AsyncPgConnection| -> Result<(), diesel::result::Error> {
                diesel::sql_query(
                    "SELECT tokens FROM harvest_rate_limit_buckets WHERE key = $1 FOR UPDATE",
                )
                .bind::<diesel::sql_types::Text, _>(&locker_b_bucket)
                .execute(conn)
                .await?;
                let _ = locked_b_tx.send(());
                diesel::sql_query("SELECT pg_sleep(3.0)")
                    .execute(conn)
                    .await?;
                Ok(())
            },
        )
        .await
        .expect("locker b transaction");
    });

    // Wait until both lockers hold their row locks. A fixed sleep cannot
    // guarantee that on a loaded CI host.
    locked_a_rx.await.expect("locker a signaled");
    locked_b_rx.await.expect("locker b signaled");

    let start = std::time::Instant::now();
    let claimed = batched_claim_one(
        &mut conn,
        &queue,
        "expired-mid-walk-tester",
        BatchedClaimConfig::default(),
    )
    .await;
    let elapsed = start.elapsed();

    locker_a.await.expect("locker a joined");
    locker_b.await.expect("locker b joined");

    assert_eq!(claimed, None);
    assert!(
        elapsed < std::time::Duration::from_secs(2),
        "candidate B's deadline expires during candidate A's ~1s bucket \
         wait, and bucket_b stays locked until ~3s -- B must be \
         rejected on its expired deadline alone, never waiting on its \
         own bucket; elapsed={elapsed:?}"
    );
    assert_eq!(task_state(&mut conn, task_b).await, "PENDING");
}

/// A build-incompatible claim does not wait on the bucket row lock.
///
/// This is the class of the circuit-breaker and deadline skips above.
/// `claimed` always rejects a build-incompatible candidate, by the
/// build-routing recheck. Without this gate, the forced lock still waits on
/// an unrelated bucket row first, and the whole wait is wasted.
///
/// A separate connection holds a real lock, not a `pg_sleep` stand-in, as
/// in the circuit-breaker skip test.
#[tokio::test(flavor = "multi_thread")]
async fn batched_claim_attempt_skips_the_bucket_lock_for_a_build_incompatible_candidate() {
    use diesel_async::RunQueryDsl;

    let (url, mut conn, _container) = setup_db().await;
    let bucket_key = format!("bucket-{}", Uuid::new_v4().simple());
    queue::ensure_rate_limit_bucket(&mut conn, &bucket_key, 0.0, 100.0)
        .await
        .expect("ensure bucket");

    let queue = unique_queue("batched-build-incompatible-no-block");
    // Unique per run: CI's DB-backed shards use a persistent Postgres,
    // not always a fresh testcontainer, and harvest_build_compat rows
    // outlive a single test process.
    let unique = Uuid::new_v4().simple().to_string();
    let required_build = format!("build-required-{unique}");
    let worker_build = format!("build-worker-{unique}");
    let exec_id = insert_execution(&mut conn).await;
    let mut params = EnqueueParams::new(&queue, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = Some("noop".to_string());
    params.activity_id = Some(Uuid::new_v4());
    params.rate_limit_key = Some(bucket_key.clone());
    params.required_build_id = Some(required_build);
    let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");

    let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
    let locker_bucket_key = bucket_key.clone();
    let locker_url = url.clone();
    let locker = tokio::spawn(async move {
        let mut locker_conn = connect(&locker_url).await;
        let mut tx = locker_conn.build_transaction().read_committed();
        tx.run(
            async |conn: &mut AsyncPgConnection| -> Result<(), diesel::result::Error> {
                diesel::sql_query(
                    "SELECT tokens FROM harvest_rate_limit_buckets WHERE key = $1 FOR UPDATE",
                )
                .bind::<diesel::sql_types::Text, _>(&locker_bucket_key)
                .execute(conn)
                .await?;
                let _ = locked_tx.send(());
                diesel::sql_query("SELECT pg_sleep(1.5)")
                    .execute(conn)
                    .await?;
                Ok(())
            },
        )
        .await
        .expect("locker transaction");
    });

    // Wait for the locker to actually hold the row lock -- a fixed sleep
    // cannot guarantee that on a loaded CI host.
    locked_rx.await.expect("locker signaled");

    #[derive(diesel::QueryableByName)]
    struct ClaimedId {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
    }

    let start = std::time::Instant::now();
    let mut tx = conn.build_transaction().read_committed();
    let claimed: Option<Uuid> = tx
        .run(
            async |conn: &mut AsyncPgConnection| -> Result<Option<Uuid>, diesel::result::Error> {
                let rows: Vec<ClaimedId> =
                    diesel::sql_query(queue::claim_batched_candidate_attempt_query())
                        .bind::<diesel::sql_types::Text, _>("build-incompatible-no-block-tester")
                        .bind::<diesel::sql_types::Uuid, _>(task_id)
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                            None::<String>,
                        )
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(
                            None::<i32>,
                        )
                        .bind::<diesel::sql_types::Text, _>("activity")
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(Some(
                            bucket_key.clone(),
                        ))
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                            None::<String>,
                        )
                        .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(
                            &Vec::<String>::new(),
                        )
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>, _>(
                            None::<chrono::DateTime<chrono::Utc>>,
                        )
                        .bind::<diesel::sql_types::Text, _>(&worker_build)
                        .load(conn)
                        .await?;
                Ok(rows.into_iter().next().map(|r| r.id))
            },
        )
        .await
        .expect("transaction");
    let elapsed = start.elapsed();

    locker.await.expect("locker joined");

    assert_eq!(claimed, None);
    assert!(
        elapsed < std::time::Duration::from_millis(500),
        "a build-incompatible claim must not wait on the bucket lock at \
         all -- the locker holds it for 1.5s, so any wait on it would \
         show up here; elapsed={elapsed:?}"
    );
    assert_eq!(task_state(&mut conn, task_id).await, "PENDING");
}

/// `rate_limit_debit` stamps `last_refilled_at` at the debit, not at `NOW()`.
///
/// After a long batch walk, the frozen `NOW()` of the transaction is well
/// behind real time. `last_refilled_at = NOW()` would store a past time. A
/// later claimant would re-accrue tokens for an interval already counted,
/// and exceed the rate limit.
///
/// The test uses the `pg_sleep` technique of the `started_at` test.
/// `last_refilled_at` must land after the sleep, not near the pre-sleep time.
#[tokio::test(flavor = "multi_thread")]
async fn batched_claim_attempt_stamps_last_refilled_at_with_real_time_not_frozen_now() {
    use diesel_async::RunQueryDsl;

    let (_url, mut conn, _container) = setup_db().await;
    let bucket_key = format!("bucket-{}", Uuid::new_v4().simple());
    queue::ensure_rate_limit_bucket(&mut conn, &bucket_key, 0.0, 100.0)
        .await
        .expect("ensure bucket");

    let queue = unique_queue("batched-last-refilled-at");
    let exec_id = insert_execution(&mut conn).await;
    let mut params = EnqueueParams::new(&queue, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = Some("noop".to_string());
    params.activity_id = Some(Uuid::new_v4());
    params.rate_limit_key = Some(bucket_key.clone());
    let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");

    let before_sleep = chrono::Utc::now();

    #[derive(diesel::QueryableByName)]
    struct ClaimedId {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
    }

    let mut tx = conn.build_transaction().read_committed();
    let claimed: Option<Uuid> = tx
        .run(
            async |conn: &mut AsyncPgConnection| -> Result<Option<Uuid>, diesel::result::Error> {
                // Real wall-clock time advances 600ms here. This
                // transaction's own frozen NOW() does not.
                diesel::sql_query("SELECT pg_sleep(0.6)")
                    .execute(conn)
                    .await?;

                let rows: Vec<ClaimedId> =
                    diesel::sql_query(queue::claim_batched_candidate_attempt_query())
                        .bind::<diesel::sql_types::Text, _>("last-refilled-at-tester")
                        .bind::<diesel::sql_types::Uuid, _>(task_id)
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                            None::<String>,
                        )
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(
                            None::<i32>,
                        )
                        .bind::<diesel::sql_types::Text, _>("activity")
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(Some(
                            bucket_key.clone(),
                        ))
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                            None::<String>,
                        )
                        .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(
                            &Vec::<String>::new(),
                        )
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>, _>(
                            None::<chrono::DateTime<chrono::Utc>>,
                        )
                        .bind::<diesel::sql_types::Text, _>("")
                        .load(conn)
                        .await?;
                Ok(rows.into_iter().next().map(|r| r.id))
            },
        )
        .await
        .expect("transaction");

    assert_eq!(claimed, Some(task_id));

    #[derive(diesel::QueryableByName)]
    struct RefilledAt {
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        last_refilled_at: chrono::DateTime<chrono::Utc>,
    }
    let final_refilled_at =
        diesel::sql_query("SELECT last_refilled_at FROM harvest_rate_limit_buckets WHERE key = $1")
            .bind::<diesel::sql_types::Text, _>(&bucket_key)
            .get_result::<RefilledAt>(&mut conn)
            .await
            .expect("final last_refilled_at")
            .last_refilled_at;

    assert!(
        final_refilled_at > before_sleep + chrono::Duration::milliseconds(400),
        "last_refilled_at must reflect the real time of the debit, \
         after the 600ms in-transaction sleep -- a frozen NOW() would \
         stamp it near before_sleep instead; before_sleep={before_sleep}, \
         final_refilled_at={final_refilled_at}"
    );
}

/// `claimed` re-checks build compatibility at the attempt.
///
/// The build-routing gate of the batch scan filters only at scan time. The
/// attempt is a separate, later statement, so it must catch a revoke in
/// between. Otherwise a worker could claim a task for a build that it may no
/// longer run. That breaks the replay determinism that build routing guards.
///
/// The test drives `claim_batched_candidate_attempt_query()` directly, as
/// the deadline and concurrency recheck tests do. So it proves the re-check
/// in the `WHERE` clause, not only the one in the batch scan.
#[tokio::test(flavor = "multi_thread")]
async fn batched_claim_attempt_rechecks_build_routing_not_just_the_batch_scan() {
    use diesel_async::RunQueryDsl;

    let (_url, mut conn, _container) = setup_db().await;
    let queue = unique_queue("batched-build-routing");
    // Unique per run: CI's DB-backed shards use a persistent Postgres,
    // not always a fresh testcontainer, and harvest_build_compat rows
    // outlive a single test process.
    let unique = Uuid::new_v4().simple().to_string();
    let required_build = format!("build-required-{unique}");
    let worker_build = format!("build-worker-{unique}");
    let exec_id = insert_execution(&mut conn).await;
    let mut params = EnqueueParams::new(&queue, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = Some("noop".to_string());
    params.activity_id = Some(Uuid::new_v4());
    params.required_build_id = Some(required_build.clone());
    let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");

    #[derive(diesel::QueryableByName)]
    struct ClaimedId {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
    }
    async fn try_attempt(
        conn: &mut AsyncPgConnection,
        task_id: Uuid,
        worker_build_id: &str,
    ) -> Option<Uuid> {
        let mut tx = conn.build_transaction().read_committed();
        tx.run(
            async |conn: &mut AsyncPgConnection| -> Result<Option<Uuid>, diesel::result::Error> {
                let rows: Vec<ClaimedId> =
                    diesel::sql_query(queue::claim_batched_candidate_attempt_query())
                        .bind::<diesel::sql_types::Text, _>("build-routing-tester")
                        .bind::<diesel::sql_types::Uuid, _>(task_id)
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                            None::<String>,
                        )
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(
                            None::<i32>,
                        )
                        .bind::<diesel::sql_types::Text, _>("activity")
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                            None::<String>,
                        )
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                            None::<String>,
                        )
                        .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(
                            &Vec::<String>::new(),
                        )
                        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>, _>(
                            None::<chrono::DateTime<chrono::Utc>>,
                        )
                        .bind::<diesel::sql_types::Text, _>(worker_build_id)
                        .load(conn)
                        .await?;
                Ok(rows.into_iter().next().map(|r| r.id))
            },
        )
        .await
        .expect("transaction")
    }

    // No declared compatibility between the worker's build and the
    // task's required build. The attempt must reject it, exactly as if
    // compatibility had just been revoked before this attempt ran.
    let claimed = try_attempt(&mut conn, task_id, &worker_build).await;
    assert_eq!(claimed, None);
    assert_eq!(task_state(&mut conn, task_id).await, "PENDING");

    // Declared compatibility makes the same re-check pass, so the gate
    // is live.
    build_routing::declare_compat(&mut conn, &worker_build, &required_build)
        .await
        .expect("declare compat");
    let claimed = try_attempt(&mut conn, task_id, &worker_build).await;
    assert_eq!(claimed, Some(task_id));
    assert_eq!(task_state(&mut conn, task_id).await, "RUNNING");
}

// ── Other preserved gates, exercised end-to-end (not just SQL-shape) ───────

/// Sticky routing, exercised through a real `claim_task_batched` call, not
/// just a SQL-text assertion.
///
/// A row pinned to a worker with a live sticky lease must be claimed by
/// that worker, and skipped when a different worker polls first.
#[tokio::test(flavor = "multi_thread")]
async fn batched_claim_honors_sticky_routing() {
    let (_url, mut conn, _container) = setup_db().await;
    let queue = unique_queue("batched-sticky");

    let exec_id = insert_execution(&mut conn).await;
    let mut params = EnqueueParams::new(&queue, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = Some("noop".to_string());
    params.activity_id = Some(Uuid::new_v4());
    params.sticky_worker_id = Some("sticky-owner".to_string());
    params.sticky_timeout = Some(std::time::Duration::from_secs(300));
    let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");

    let claimed_by_other = batched_claim_one(
        &mut conn,
        &queue,
        "not-the-owner",
        BatchedClaimConfig::default(),
    )
    .await;
    assert_eq!(
        claimed_by_other, None,
        "a live sticky pin must block every other worker"
    );

    let claimed_by_owner = batched_claim_one(
        &mut conn,
        &queue,
        "sticky-owner",
        BatchedClaimConfig::default(),
    )
    .await
    .expect("the pinned worker must be able to claim it");
    assert_eq!(claimed_by_owner, task_id);
}

/// The circuit-breaker/ineligible-activities gate exempts a
/// capability-routed activity (`required_capabilities IS NOT NULL`),
/// exercised end-to-end.
///
/// Both rows share `activity_name`, and that name is passed as
/// `ineligible_activities`. Only the row WITHOUT `required_capabilities`
/// must be excluded by it.
#[tokio::test(flavor = "multi_thread")]
async fn batched_claim_capability_routed_activity_bypasses_the_ineligible_gate() {
    let (_url, mut conn, _container) = setup_db().await;
    let queue = unique_queue("batched-capability");
    let poisoned_activity = "poison-activity";

    let exec_id = insert_execution(&mut conn).await;
    let mut plain = EnqueueParams::new(&queue, TaskType::Activity, serde_json::json!({}));
    plain.workflow_exec_id = Some(exec_id);
    plain.activity_name = Some(poisoned_activity.to_string());
    plain.activity_id = Some(Uuid::new_v4());
    queue::enqueue(&mut conn, &plain)
        .await
        .expect("enqueue plain");

    let exec_id2 = insert_execution(&mut conn).await;
    let mut capability_routed =
        EnqueueParams::new(&queue, TaskType::Activity, serde_json::json!({}));
    capability_routed.workflow_exec_id = Some(exec_id2);
    capability_routed.activity_name = Some(poisoned_activity.to_string());
    capability_routed.activity_id = Some(Uuid::new_v4());
    // An empty requirement array is present, not NULL. So it exempts this
    // row from the ineligible-activities gate. The separate
    // capability-match EXISTS check below it is vacuously satisfied too --
    // there are no elements to fail against.
    capability_routed.required_capabilities = Some(serde_json::json!([]));
    let capability_task_id = queue::enqueue(&mut conn, &capability_routed)
        .await
        .expect("enqueue capability-routed");

    let ineligible = vec![poisoned_activity.to_string()];
    let claimed = queue::claim_task_batched(
        &mut conn,
        std::slice::from_ref(&queue),
        "w7",
        "",
        None,
        &[],
        &ineligible,
        BatchedClaimConfig::default(),
    )
    .await
    .expect("claim")
    .expect("the capability-routed row must still be claimable");
    assert_eq!(
        claimed.id, capability_task_id,
        "the plain row sharing the same activity_name must stay excluded, \
         and the capability-routed row must be the one found"
    );
}

// ── End-to-end latency capture (evidence for docs/performance-claim-batched-seek-and-refine.md) ──

/// (Re)seed the 10,000-row/4-queue/256-key backlog plus 2,000 `RUNNING`
/// rows on the same keys -- the exact hot-contention fixture
/// `docs/performance-claim-batched-seek-and-refine.md` cites.
///
/// Called once per measured loop in
/// [`zz_capture_claim_batched_end_to_end_latency`], not once per capture.
/// So each path claims against an identical, freshly seeded backlog,
/// rather than whatever the other path's claims left behind.
async fn reseed_end_to_end_fixture(url: &str) {
    let mut seed_conn = connect(url).await;
    seed_conn
        .batch_execute("TRUNCATE harvest_task_queue")
        .await
        .expect("truncate");
    seed_conn
        .batch_execute(
            "INSERT INTO harvest_task_queue \
               (id, queue_name, task_type, activity_name, activity_id, input, state, \
                priority, attempt, max_attempts, scheduled_at, concurrency_key, \
                concurrency_cap, crash_strikes, wake_requested) \
             SELECT gen_random_uuid(), 'bench-q-' || (i % 4), 'activity', 'noop', \
                    gen_random_uuid(), '{}'::jsonb, 'PENDING', (i % 100), 0, 3, \
                    NOW() - INTERVAL '1 second', 'ckey-' || (i % 256), 1000000, 0, FALSE \
             FROM generate_series(1, 10000) AS i",
        )
        .await
        .expect("seed backlog");
    seed_conn
        .batch_execute(
            "INSERT INTO harvest_task_queue \
               (id, queue_name, task_type, activity_name, activity_id, input, state, \
                priority, worker_id, attempt, max_attempts, scheduled_at, started_at, \
                concurrency_key, concurrency_cap, crash_strikes, wake_requested) \
             SELECT gen_random_uuid(), 'bench-q-' || (i % 4), 'activity', 'noop', \
                    gen_random_uuid(), '{}'::jsonb, 'RUNNING', 0, 'holder-' || i, 1, 3, \
                    NOW() - INTERVAL '10 second', NOW() - INTERVAL '5 second', \
                    'ckey-' || (i % 256), 1000000, 0, FALSE \
             FROM generate_series(1, 2000) AS i",
        )
        .await
        .expect("seed hot-contention rows");
    seed_conn
        .batch_execute("ANALYZE harvest_task_queue")
        .await
        .expect("analyze");
}

/// Real end-to-end latency, `claim_task` against `claim_task_batched`, at
/// the hot-contention fixture `docs/performance-claim-batched-seek-and-refine.md`
/// cites.
///
/// `cargo xtask perf claim-batched-seek-and-refine` takes SQL-only
/// `EXPLAIN` captures. This test drives the REAL compiled functions over
/// real connections instead. So it is the only source of the headline
/// milliseconds per claim on that page. Each path claims against its own
/// reseeded copy of the fixture ([`reseed_end_to_end_fixture`] says why).
/// Single-row runs first, so the batched path never gains a warmer cache.
///
/// `CLAIM_BATCHED_CAPTURE_N` overrides the claim count per path (default
/// 400).
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[ignore = "evidence generator, not a CI assertion -- run via \
            cargo xtask perf claim-batched-seek-and-refine"]
async fn zz_capture_claim_batched_end_to_end_latency() {
    let bench = match super::claim_bench_support::db::setup_bench_db().await {
        Ok(bench) => bench,
        Err(reason) => {
            eprintln!("no database reachable ({}); nothing captured", reason.0);
            return;
        }
    };

    let n: usize = std::env::var("CLAIM_BATCHED_CAPTURE_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(400);

    let queues = vec![
        "bench-q-0".to_string(),
        "bench-q-1".to_string(),
        "bench-q-2".to_string(),
        "bench-q-3".to_string(),
    ];
    let mut conn = connect(&bench.url).await;

    // Each path gets its OWN fresh 10,000-row/2,000-`RUNNING` fixture. With
    // a shared fixture, the 400 claims of the single-row loop (PENDING ->
    // RUNNING) shrink and reshape the backlog of the batched loop. The two
    // loops then start from different conditions, and the ratio is invalid.
    reseed_end_to_end_fixture(&bench.url).await;
    let mut single_ms = Vec::with_capacity(n);
    for _ in 0..n {
        let start = std::time::Instant::now();
        let claimed = claim_task(&mut conn, &queues, "capture-single", "", None, &[], &[])
            .await
            .expect("claim");
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        if claimed.is_none() {
            break;
        }
        single_ms.push(elapsed_ms);
    }

    reseed_end_to_end_fixture(&bench.url).await;
    let mut batch_ms = Vec::with_capacity(n);
    for _ in 0..n {
        let start = std::time::Instant::now();
        let claimed = claim_task_batched(
            &mut conn,
            &queues,
            "capture-batched",
            "",
            None,
            &[],
            &[],
            BatchedClaimConfig::default(),
        )
        .await
        .expect("claim");
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        if claimed.is_none() {
            break;
        }
        batch_ms.push(elapsed_ms);
    }

    let single_stats = super::claim_bench_support::LatencyStats::from_samples(&single_ms);
    let batch_stats = super::claim_bench_support::LatencyStats::from_samples(&batch_ms);

    // The true minimum. The samples are not sorted, so `.first()` is not
    // the minimum.
    let single_min = single_ms.iter().copied().fold(f64::INFINITY, f64::min);
    let batch_min = batch_ms.iter().copied().fold(f64::INFINITY, f64::min);

    let summary = format!(
        "single-row claim_task: n={} mean={:.3}ms p50={:.3}ms p99={:.3}ms min={:.3}ms max={:.3}ms\n\
         batched  claim_task_batched: n={} mean={:.3}ms p50={:.3}ms p99={:.3}ms min={:.3}ms max={:.3}ms\n",
        single_stats.count,
        single_stats.mean_ms,
        single_stats.p50_ms,
        single_stats.p99_ms,
        single_min,
        single_stats.max_ms,
        batch_stats.count,
        batch_stats.mean_ms,
        batch_stats.p50_ms,
        batch_stats.p99_ms,
        batch_min,
        batch_stats.max_ms,
    );

    let out_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("autumn-harvest/ has a workspace-root parent")
        .join("docs")
        .join("perf-artifacts")
        .join("claim-batched-seek-and-refine");
    std::fs::create_dir_all(&out_dir).expect("create artifact output directory");
    std::fs::write(out_dir.join("end_to_end_latency.txt"), &summary).expect("write artifact");

    eprintln!("== capture complete: label=end_to_end_latency ==\n{summary}");
}
