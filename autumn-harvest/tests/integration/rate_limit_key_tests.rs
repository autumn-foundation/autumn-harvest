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
    clippy::cast_precision_loss
)]
//! Per-key activity rate-limit integration tests — issue #699.
//!
//! Exercises the dynamic per-key rate-limit bucket path against a real Postgres:
//! - **AC2 + success metric** — distinct resolved tenant keys throttle
//!   independently; draining one tenant's bucket never blocks another's.
//! - **AC2 shared bucket** — two executions resolving to the same key share one
//!   composite bucket row.
//! - **AC6 defer-not-fail** — an exhausted bucket leaves the task PENDING (never
//!   FAILED); a refill makes it claimable.
//! - **Lazy registration** — the composite `dyn-rate:...` bucket row is created.
//! - **AC3 static unchanged** — a static `rate_limit_key` activity still buckets
//!   under its static key.
//! - **AC8 compose with #247 concurrency** — a task carrying BOTH a per-key
//!   concurrency cap and a per-key rate limit is gated by both.

use autumn_harvest::queue::{
    self, EnqueueParams, TaskType, claim_task, dynamic_rate_bucket_key, ensure_rate_limit_bucket,
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

/// Bring up a fresh Postgres 16 with the full schema in a testcontainer.
async fn setup_db() -> (AsyncPgConnection, ContainerAsync<Postgres>) {
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
    (conn, container)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async fn set_bucket_tokens(conn: &mut AsyncPgConnection, key: &str, tokens: f64) {
    use diesel_async::RunQueryDsl;
    diesel::sql_query(
        "UPDATE harvest_rate_limit_buckets SET tokens=$2, last_refilled_at=NOW() WHERE key=$1",
    )
    .bind::<diesel::sql_types::Text, _>(key)
    .bind::<diesel::sql_types::Double, _>(tokens)
    .execute(conn)
    .await
    .expect("set tokens");
}

async fn scalar_i64(conn: &mut AsyncPgConnection, sql: &str, bind: &str) -> i64 {
    use diesel_async::RunQueryDsl;
    #[derive(diesel::QueryableByName)]
    struct N {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    diesel::sql_query(sql)
        .bind::<diesel::sql_types::Text, _>(bind)
        .get_result::<N>(conn)
        .await
        .expect("scalar")
        .n
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

/// Insert a minimal RUNNING execution row so an activity task can satisfy the
/// `harvest_task_queue.workflow_exec_id` foreign key. Returns the execution id.
async fn insert_execution(conn: &mut AsyncPgConnection) -> Uuid {
    use diesel_async::RunQueryDsl;
    let id = Uuid::new_v4();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions (id, workflow_name, workflow_id, shard_id, input) \
         VALUES ($1, 'rlk', $2, 0, '{}'::jsonb)",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .bind::<diesel::sql_types::Text, _>(id.to_string())
    .execute(conn)
    .await
    .expect("insert execution");
    id
}

/// Enqueue an activity task, optionally with a rate-limit key and per-key
/// concurrency cap. Returns the task id.
async fn enqueue_activity(
    conn: &mut AsyncPgConnection,
    queue: &str,
    activity: &str,
    rate_limit_key: Option<String>,
    concurrency: Option<(String, u32)>,
) -> Uuid {
    let exec_id = insert_execution(conn).await;
    let mut params = EnqueueParams::new(queue, TaskType::Activity, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id);
    params.activity_name = Some(activity.to_string());
    params.activity_id = Some(Uuid::new_v4());
    params.rate_limit_key = rate_limit_key;
    if let Some((key, cap)) = concurrency {
        params.concurrency_key = Some(key);
        params.max_concurrent = Some(cap);
    }
    queue::enqueue(conn, &params).await.expect("enqueue")
}

const WORKER: &str = "worker-a";
const BUILD: &str = "";

async fn claim(conn: &mut AsyncPgConnection, queue: &str) -> Option<Uuid> {
    claim_task(conn, &[queue.to_string()], WORKER, BUILD, None, &[], &[])
        .await
        .expect("claim")
        .map(|t| t.id)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn ac2_dynamic_per_key_cross_tenant_isolation() {
    // Success metric: draining tenant A's bucket blocks A's task while B's task
    // stays claimable — zero cross-key bleed.
    let (mut conn, _g) = setup_db().await;
    let q = "iso-queue";
    let key_a = dynamic_rate_bucket_key("input.tenant_id", "acme");
    let key_b = dynamic_rate_bucket_key("input.tenant_id", "globex");
    ensure_rate_limit_bucket(&mut conn, &key_a, 5.0, 3.0)
        .await
        .unwrap();
    ensure_rate_limit_bucket(&mut conn, &key_b, 5.0, 3.0)
        .await
        .unwrap();

    let task_a = enqueue_activity(&mut conn, q, "charge", Some(key_a.clone()), None).await;
    let task_b = enqueue_activity(&mut conn, q, "charge", Some(key_b.clone()), None).await;

    // Drain ONLY tenant A.
    set_bucket_tokens(&mut conn, &key_a, 0.0).await;

    // The claim gate must skip A and hand back B; a second claim finds nothing
    // (A still throttled).
    let first = claim(&mut conn, q).await;
    assert_eq!(first, Some(task_b), "tenant B must be claimable");
    let second = claim(&mut conn, q).await;
    assert_eq!(second, None, "tenant A must stay throttled, not bleed over");
    assert_eq!(
        task_state(&mut conn, task_a).await,
        "PENDING",
        "throttled tenant A stays PENDING, never claimed"
    );

    // Refill A → now claimable.
    set_bucket_tokens(&mut conn, &key_a, 3.0).await;
    assert_eq!(claim(&mut conn, q).await, Some(task_a));
}

#[tokio::test]
async fn ac2_dynamic_shared_bucket_across_executions() {
    // Two executions resolving to the SAME key share one composite bucket row;
    // draining it blocks both.
    let (mut conn, _g) = setup_db().await;
    let q = "shared-queue";
    let key = dynamic_rate_bucket_key("input.tenant_id", "acme");
    // Ensuring twice must not create a second row (idempotent).
    ensure_rate_limit_bucket(&mut conn, &key, 5.0, 5.0)
        .await
        .unwrap();
    ensure_rate_limit_bucket(&mut conn, &key, 5.0, 5.0)
        .await
        .unwrap();
    let rows = scalar_i64(
        &mut conn,
        "SELECT COUNT(*)::bigint AS n FROM harvest_rate_limit_buckets WHERE key=$1",
        &key,
    )
    .await;
    assert_eq!(rows, 1, "same resolved key → exactly one composite bucket");

    let t1 = enqueue_activity(&mut conn, q, "charge", Some(key.clone()), None).await;
    let t2 = enqueue_activity(&mut conn, q, "charge", Some(key.clone()), None).await;

    // Exactly one token available: exactly one of the two can be claimed.
    set_bucket_tokens(&mut conn, &key, 1.0).await;
    let claimed = claim(&mut conn, q).await;
    assert!(claimed == Some(t1) || claimed == Some(t2));
    assert_eq!(
        claim(&mut conn, q).await,
        None,
        "shared bucket is now empty → the other task is throttled"
    );
}

#[tokio::test]
async fn ac6_dynamic_defer_not_fail() {
    // An exhausted bucket leaves the task PENDING (never FAILED); a refill makes
    // it claimable.
    let (mut conn, _g) = setup_db().await;
    let q = "defer-queue";
    let key = dynamic_rate_bucket_key("input.tenant_id", "acme");
    ensure_rate_limit_bucket(&mut conn, &key, 5.0, 3.0)
        .await
        .unwrap();
    let task = enqueue_activity(&mut conn, q, "charge", Some(key.clone()), None).await;

    set_bucket_tokens(&mut conn, &key, 0.0).await;
    assert_eq!(claim(&mut conn, q).await, None);
    assert_eq!(
        task_state(&mut conn, task).await,
        "PENDING",
        "throttled task must stay PENDING, never FAILED"
    );

    set_bucket_tokens(&mut conn, &key, 3.0).await;
    assert_eq!(claim(&mut conn, q).await, Some(task));
}

#[tokio::test]
async fn lazy_registration_creates_composite_bucket_row() {
    let (mut conn, _g) = setup_db().await;
    let key = dynamic_rate_bucket_key("input.tenant_id", "acme");
    // Bucket does not exist yet.
    assert_eq!(
        scalar_i64(
            &mut conn,
            "SELECT COUNT(*)::bigint AS n FROM harvest_rate_limit_buckets WHERE key=$1",
            &key,
        )
        .await,
        0
    );
    ensure_rate_limit_bucket(&mut conn, &key, 7.0, 4.0)
        .await
        .unwrap();
    // Row created with tokens seeded to burst.
    use diesel_async::RunQueryDsl;
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Double)]
        refill_rate: f64,
        #[diesel(sql_type = diesel::sql_types::Double)]
        burst: f64,
        #[diesel(sql_type = diesel::sql_types::Double)]
        tokens: f64,
    }
    let row: Row = diesel::sql_query(
        "SELECT refill_rate, burst, tokens FROM harvest_rate_limit_buckets WHERE key=$1",
    )
    .bind::<diesel::sql_types::Text, _>(&key)
    .get_result(&mut conn)
    .await
    .expect("bucket row");
    assert!((row.refill_rate - 7.0).abs() < 1e-9);
    assert!((row.burst - 4.0).abs() < 1e-9);
    assert!((row.tokens - 4.0).abs() < 1e-9);
}

#[tokio::test]
async fn ac3_static_rate_limit_still_buckets_under_static_key() {
    // A static (non-dynamic) rate-limit key still gates claims — unchanged.
    let (mut conn, _g) = setup_db().await;
    let q = "static-queue";
    let key = "send_email".to_string(); // static bucket key == activity name
    ensure_rate_limit_bucket(&mut conn, &key, 5.0, 3.0)
        .await
        .unwrap();
    assert!(!key.starts_with(queue::DYNAMIC_RATE_PREFIX));
    let task = enqueue_activity(&mut conn, q, "send_email", Some(key.clone()), None).await;
    set_bucket_tokens(&mut conn, &key, 0.0).await;
    assert_eq!(claim(&mut conn, q).await, None);
    set_bucket_tokens(&mut conn, &key, 3.0).await;
    assert_eq!(claim(&mut conn, q).await, Some(task));
}

#[tokio::test]
async fn ac8_dynamic_rate_composes_with_per_key_concurrency() {
    // A task carrying BOTH a per-key concurrency cap (#247) and a per-key rate
    // limit (#699) is gated by both, independently.
    let (mut conn, _g) = setup_db().await;
    let q = "compose-queue";
    let rate_key = dynamic_rate_bucket_key("input.tenant_id", "acme");
    ensure_rate_limit_bucket(&mut conn, &rate_key, 100.0, 100.0)
        .await
        .unwrap();
    let concurrency = ("tenant:acme".to_string(), 1u32);

    // Two tasks, same concurrency key (cap 1), same rate key (plenty of tokens).
    let t1 = enqueue_activity(
        &mut conn,
        q,
        "charge",
        Some(rate_key.clone()),
        Some(concurrency.clone()),
    )
    .await;
    let _t2 = enqueue_activity(
        &mut conn,
        q,
        "charge",
        Some(rate_key.clone()),
        Some(concurrency.clone()),
    )
    .await;

    // Rate tokens are available, so the concurrency cap governs: exactly one runs.
    let first = claim(&mut conn, q).await;
    assert_eq!(first, Some(t1));
    assert_eq!(
        claim(&mut conn, q).await,
        None,
        "concurrency cap 1 saturated → second task deferred despite ample rate tokens"
    );

    // Now exhaust the rate bucket: even after the concurrency slot would free up,
    // the rate limit blocks the second task.
    set_bucket_tokens(&mut conn, &rate_key, 0.0).await;
    // Mark t1 COMPLETED to free the concurrency slot.
    use diesel_async::RunQueryDsl;
    diesel::sql_query("UPDATE harvest_task_queue SET state='COMPLETED' WHERE id=$1")
        .bind::<diesel::sql_types::Uuid, _>(t1)
        .execute(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        claim(&mut conn, q).await,
        None,
        "rate bucket exhausted → second task deferred despite free concurrency slot"
    );
    // Refill rate → now the concurrency-free, rate-available task runs.
    set_bucket_tokens(&mut conn, &rate_key, 100.0).await;
    assert!(claim(&mut conn, q).await.is_some());
}
