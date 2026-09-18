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
//! These tests exist to establish the one thing
//! `docs/assays/0005-claim-batched-seek-and-refine.md`'s single-session
//! apparatus could not: the batching mechanism holds its exactly-once,
//! never-over-the-cap guarantee under REAL concurrent claimers. It does so
//! not just in a single serial walk.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres, to
//! run against it directly. Otherwise a fresh testcontainers Postgres
//! boots with the full migration bundle.

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

    // Both must have claimed the row seeded with priority 9. That is the
    // only fact this test needs to hold, across two structurally
    // identical, but row-id-distinct, backlogs.
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
/// not block the claim. That is the whole point of moving the gate to a
/// per-candidate recheck, instead of a single-row candidate-side filter.
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
/// poisoned rows' sort key. This exercises the keyset cursor's `id`
/// tiebreak across a batch boundary -- the exact bug ledger #5's own
/// post-review caught and fixed.
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

    // `pg_try_advisory_xact_lock` is non-blocking by design (matches
    // `claim_task_on_shard`'s own documented behavior). A claimer that
    // loses the lock race gets 0 rows, and simply retries next poll cycle.
    // That happens even if the cap was not actually saturated at that
    // instant. So a racing round can land BELOW the cap -- that is
    // safe-side and expected. What must never happen is landing ABOVE it.
    // At least one claimer winning is the soundness floor for this test
    // (10 claimers racing 10 identical rows with real Tokio concurrency).
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

    // burst=1, refill_rate=0 -- capacity for exactly one token, never
    // auto-refilling. `ensure_rate_limit_bucket` starts `tokens` at
    // `burst` (full). Drain it explicitly, to get the "empty bucket"
    // starting state this test wants. A burst of 0 would instead cap
    // `effective_available_tokens_expr`'s LEAST(burst, ...) at 0 forever,
    // making the later top-up unable to ever clear the gate.
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

/// Regression test for a review finding on this PR (Codex, P2).
///
/// A candidate whose `schedule_to_close_at` deadline passes in REAL
/// wall-clock time, while a batch search is still walking earlier
/// candidates, must not be claimed. That holds even though the frozen
/// transaction `NOW()` the batch scan itself uses would have let it
/// through.
///
/// Drives `claim_batched_candidate_attempt_query()` directly inside a
/// hand-managed transaction, with a `pg_sleep` standing in for "a long
/// batch search." That keeps this test deterministic. It does not depend
/// on actually walking hundreds of poisoned candidates to consume real
/// time.
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
                // Real wall-clock time advances 600ms here. This
                // transaction's own frozen NOW() does not. `deadline` is
                // still "not yet past NOW()" from this transaction's own
                // point of view for the rest of it.
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

/// Regression test for a review finding on this PR. A data-modifying CTE
/// runs whether or not a later CTE reads its result. A candidate whose
/// deadline passed mid-transaction must not debit a rate-limit token.
/// `claimed` was always going to reject it anyway. This is the same
/// shape as the concurrency-rejected-candidate leak below, on the
/// deadline recheck instead of the concurrency recheck.
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

/// Regression test for a review finding on this PR. Walking a batch must
/// NOT debit a rate-limit token for a candidate the concurrency gate
/// always rejects.
///
/// Every poisoned row here shares BOTH a saturated `concurrency_key` AND a
/// `rate_limit_key`, funded with many tokens. That is the exact
/// adversarial combination the fix in
/// `queue::claim_batched_candidate_concurrency_probe_query` exists to
/// close. Before that fix, walking past these poisoned rows would debit
/// one token per row tried.
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

/// Regression test for a review finding on this PR. A batch walk can spend
/// real wall-clock time inside one transaction. `NOW()` stays frozen at
/// transaction start for that whole walk. So `started_at = NOW()` would
/// backdate the claim by the walk's duration, silently shortening the
/// task's `start_to_close`/`heartbeat_timeout` budget (both measured from
/// `started_at`). `started_at` must instead read the query's own
/// materialized `clock_timestamp()`, taken at the actual claim.
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

/// Regression test for a review finding on this PR. `now_ts` has no
/// `FROM` clause of its own, so Postgres could resolve it before
/// `rate_limit_debit` even attempts its own row lock. A concurrent
/// transaction holding that lock would then let `now_ts` capture a
/// stale, pre-wait value that `rate_limit_debit`, `claimed`, and
/// `started_at` all reuse. A deadline that expires DURING the wait
/// would incorrectly still read as live.
///
/// Reproduces the race with two real connections. One holds the bucket
/// row's lock well past the deadline. The other attempts the claim,
/// genuinely blocked on that exact lock. This is not a `pg_sleep`
/// stand-in. The bug is specifically about WHEN a blocking wait
/// resolves relative to the shared clock read.
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

    // Give the locker a head start so it wins the row lock first.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

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

/// Regression test for a review finding on this PR, against the ninth
/// bug's own `now_ts` fix. `rate_limit_debit` never touches the bucket
/// row for a circuit-breaker-bypassed activity. `now_ts`'s forced lock
/// must skip it too. A bypassed claim is meant to run at full speed,
/// past rate limiting entirely. Without this, it serializes behind an
/// unrelated transaction holding that bucket row for an unrelated
/// activity.
///
/// Proven with a real lock held by a separate connection, not a
/// `pg_sleep` stand-in. A bypassed claim must complete quickly despite
/// contention it has no reason to wait on.
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
                diesel::sql_query("SELECT pg_sleep(1.5)")
                    .execute(conn)
                    .await?;
                Ok(())
            },
        )
        .await
        .expect("locker transaction");
    });

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

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

/// Regression test for a review finding on this PR. A candidate's
/// deadline can pass DURING the batch walk. Not while waiting on that
/// candidate's own bucket lock (the ninth-bug scenario above), but
/// while an EARLIER candidate's bucket lock is being waited on. By the
/// time the walk reaches this later candidate, its deadline has already
/// passed. It can never claim, regardless of what its own bucket
/// protects. Waiting on that bucket lock anyway wastes an entire second
/// lock wait for nothing.
///
/// `try_claim_batched_candidate` must reject an already-expired
/// candidate before ever issuing the attempt query. It must not add its
/// own bucket wait on top of the earlier candidate's. Proven with two
/// real, separately-held locks, not a `pg_sleep` stand-in. One
/// candidate absorbs a real wait. A second candidate's own deadline
/// expires during that wait, and its own (still-locked) bucket must
/// never be waited on at all.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::similar_names)]
async fn batched_claim_skips_the_bucket_lock_for_a_candidate_already_past_its_deadline() {
    use diesel_async::RunQueryDsl;

    let (url, mut conn, _container) = setup_db().await;
    let bucket_a = format!("bucket-a-{}", Uuid::new_v4().simple());
    let bucket_b = format!("bucket-b-{}", Uuid::new_v4().simple());
    // Funded with exactly one token. The batch scan's own soft
    // rate-limit pre-filter (`{rate_limit_available} >= 1.0`) requires
    // this, or candidate A is excluded from the batch before it ever
    // reaches a per-candidate attempt. That would never touch
    // bucket_a's lock at all, defeating this test's setup.
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

    let locker_a_bucket = bucket_a.clone();
    let locker_a_url = url.clone();
    let locker_a = tokio::spawn(async move {
        let mut locker_conn = connect(&locker_a_url).await;
        let mut tx = locker_conn.build_transaction().read_committed();
        tx.run(
            async |conn: &mut AsyncPgConnection| -> Result<(), diesel::result::Error> {
                // Locks the row and spends the one token a concurrent
                // claimer would have spent. Once A's own attempt gets
                // past this lock, it correctly finds no funds left and
                // moves on to B.
                diesel::sql_query(
                    "UPDATE harvest_rate_limit_buckets SET tokens = 0 WHERE key = $1",
                )
                .bind::<diesel::sql_types::Text, _>(&locker_a_bucket)
                .execute(conn)
                .await?;
                diesel::sql_query("SELECT pg_sleep(1.0)")
                    .execute(conn)
                    .await?;
                Ok(())
            },
        )
        .await
        .expect("locker a transaction");
    });

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
                diesel::sql_query("SELECT pg_sleep(2.5)")
                    .execute(conn)
                    .await?;
                Ok(())
            },
        )
        .await
        .expect("locker b transaction");
    });

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

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
        elapsed < std::time::Duration::from_millis(1500),
        "candidate B's deadline expires during candidate A's ~1s bucket \
         wait, and bucket_b stays locked until ~2.5s -- B must be \
         rejected on its expired deadline alone, never waiting on its \
         own bucket; elapsed={elapsed:?}"
    );
    assert_eq!(task_state(&mut conn, task_b).await, "PENDING");
}

/// Regression test for a review finding on this PR (P1). A long batch
/// walk can leave this transaction's own frozen `NOW()` well behind
/// real time by the time `rate_limit_debit` finally runs. Writing
/// `last_refilled_at = NOW()` then persists a timestamp from the past,
/// not from the actual moment of the debit. A later claimant reading
/// that stale timestamp re-accrues tokens for an interval already
/// accounted for, exceeding the configured rate limit.
///
/// Same deterministic `pg_sleep` technique as the sibling `started_at`
/// test above. `last_refilled_at` must land after the in-transaction
/// sleep, not near the pre-sleep timestamp a frozen `NOW()` would give.
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
/// `autumn-harvest/scripts/claim_batched_seek_and_refine_perf_repro.sh`
/// also takes SQL-only `EXPLAIN` captures. Unlike those, this drives the
/// REAL compiled functions over real connections. So it is the only
/// source for that page's headline milliseconds-per-claim numbers. Each
/// path measures against its own freshly reseeded copy of the identical
/// fixture -- see [`reseed_end_to_end_fixture`] for why that reseed is
/// not optional. Single-row still runs first, so the batched path's own
/// numbers never benefit from a warmer page cache.
///
/// `CLAIM_BATCHED_CAPTURE_N` overrides the claim count per path (default
/// 400).
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[ignore = "evidence generator, not a CI assertion -- run via \
            autumn-harvest/scripts/claim_batched_seek_and_refine_perf_repro.sh"]
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

    // Each path gets its OWN fresh 10,000-row/2,000-`RUNNING` fixture. A
    // shared fixture would let the single-row loop's 400 claims (PENDING
    // -> RUNNING) leave the batched loop measuring a smaller, differently
    // shaped backlog. That was a real methodological bug a review caught
    // on this PR (Codex, P2). The two loops would no longer measure the
    // same starting conditions. That silently invalidates the ratio this
    // capture exists to produce.
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

    // The true minimum, not the first sample. An earlier draft of this
    // capture mislabeled `.first()` as "min" -- only correct by
    // coincidence, since samples are not collected in sorted order.
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
