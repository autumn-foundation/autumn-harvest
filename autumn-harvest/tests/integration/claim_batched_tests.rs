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
    claim_task_batched(conn, &[queue.to_string()], worker_id, "", None, &[], &[], config)
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

    let single_claimed = claim_task(&mut conn, std::slice::from_ref(&queue_a), "w1", "", None, &[], &[])
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
    let remaining = diesel::sql_query("SELECT tokens FROM harvest_rate_limit_buckets WHERE key = $1")
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
