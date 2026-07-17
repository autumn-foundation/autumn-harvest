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

use autumn_harvest::prelude::*;
use autumn_harvest::queue::{
    self, EnqueueParams, TaskType, claim_task, dynamic_rate_bucket_key, ensure_rate_limit_bucket,
};
use autumn_harvest::types::{ExecutionId, Priority};
use autumn_harvest::worker::HandlerRegistry;
use autumn_harvest::{
    StartWorkflowParams, WorkflowIdReusePolicy, start_or_load_workflow_execution,
};
use diesel_async::AsyncPgConnection;
use diesel_async::SimpleAsyncConnection;
use std::sync::Arc;
use std::time::Duration;
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

/// Bring up a fresh Postgres 16 with the full schema in a testcontainer and
/// return the connection URL (so a caller can open extra connections).
async fn setup_db_url() -> (String, ContainerAsync<Postgres>) {
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
    (url, container)
}

/// Bring up a fresh Postgres 16 with the full schema in a testcontainer.
async fn setup_db() -> (AsyncPgConnection, ContainerAsync<Postgres>) {
    let (url, container) = setup_db_url().await;
    (connect(&url).await, container)
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

async fn bucket_tokens(conn: &mut AsyncPgConnection, key: &str) -> f64 {
    use diesel_async::RunQueryDsl;
    #[derive(diesel::QueryableByName)]
    struct T {
        #[diesel(sql_type = diesel::sql_types::Double)]
        tokens: f64,
    }
    diesel::sql_query("SELECT tokens FROM harvest_rate_limit_buckets WHERE key=$1")
        .bind::<diesel::sql_types::Text, _>(key)
        .get_result::<T>(conn)
        .await
        .expect("tokens")
        .tokens
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

// ── Worker-level resolution: the WORKFLOW input is the source (issue #699, #3) ──

// An activity whose OWN input differs from the workflow input. If the enqueue
// path ever resolved the dynamic key from `scheduled.input` (the activity input)
// instead of the workflow input, this test would fail (both executions would
// resolve to the same fallback bucket).
#[activity(rate_limit(key = "input.tenant_id", rps = 50))]
async fn charge(_ctx: &ActivityContext, item: serde_json::Value) -> Result<(), String> {
    let _ = item;
    Ok(())
}

#[workflow]
async fn tenant_wf(ctx: &WorkflowContext, input: serde_json::Value) -> Result<(), String> {
    let _ = input; // the engine resolves the rate-limit key from the execution row's input
    // The activity input is deliberately DIFFERENT from the workflow input and
    // carries no `tenant_id`, so a `workflow_input` -> `scheduled.input` swap
    // would collapse the key to the fallback bucket.
    let _: () = ctx
        .execute_activity(&charge_info(), serde_json::json!({ "item": "x" }))
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn resolution_start_params(
    workflow_id: &str,
    exec_id: ExecutionId,
    input: serde_json::Value,
) -> StartWorkflowParams<'_> {
    StartWorkflowParams {
        workflow_name: "tenant_wf",
        workflow_id,
        exec_id,
        input,
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: WorkflowIdReusePolicy::default(),
        trace_context: None,
        max_execution_timeout_ceiling: None,
        concurrency_key: None,
        concurrency_limit: None,
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
        start_source: autumn_harvest::StartSource::Api,
        start_source_ref: None,
        started_by: None,
    }
}

async fn rate_limit_key_for_exec(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> String {
    use diesel_async::RunQueryDsl;
    #[derive(diesel::QueryableByName)]
    struct K {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        rate_limit_key: Option<String>,
    }
    diesel::sql_query(
        "SELECT rate_limit_key FROM harvest_task_queue \
         WHERE workflow_exec_id=$1 AND activity_name='charge'",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result::<K>(conn)
    .await
    .expect("activity task row")
    .rate_limit_key
    .expect("dynamic rate-limit key must be stamped on the activity task")
}

#[tokio::test]
async fn worker_resolves_dynamic_key_from_workflow_input_not_activity_input() {
    // Spec-critical: the composite bucket key is resolved from the WORKFLOW
    // input at enqueue time, never the activity's own input.
    let (url, _guard) = crate::integration_e2e::setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    // Two executions of the same workflow/activity, differing ONLY in workflow
    // input. The activity input is identical (`{"item":"x"}`) for both — so if
    // the activity input were the source, both would resolve to the same
    // fallback key. They don't: acme -> ...:acme, {} -> ...: (empty fallback).
    let exec_acme = ExecutionId::new();
    let exec_empty = ExecutionId::new();
    start_or_load_workflow_execution(
        &mut conn,
        resolution_start_params(
            "wf-acme",
            exec_acme,
            serde_json::json!({ "tenant_id": "acme" }),
        ),
        None,
    )
    .await
    .expect("start acme");
    start_or_load_workflow_execution(
        &mut conn,
        resolution_start_params("wf-empty", exec_empty, serde_json::json!({})),
        None,
    )
    .await
    .expect("start empty");

    let registry = Arc::new(HandlerRegistry::new(
        vec![tenant_wf_info()],
        vec![charge_info()],
    ));
    let worker = crate::integration_e2e::build_runtime_worker("rlk-resolve-worker", 2, 2, registry);
    let pool = crate::integration_e2e::build_test_pool(&url);
    let handle = crate::integration_e2e::spawn_test_worker(Arc::clone(&worker), pool);

    // Drive both executions to completion.
    let completed = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let a = task_state_workflow(&mut conn, exec_acme).await;
            let b = task_state_workflow(&mut conn, exec_empty).await;
            if a == "COMPLETED" && b == "COMPLETED" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    worker.shutdown();
    let _ = handle.await;
    completed.expect("both workflows should complete within timeout");

    // The key MUST be derived from the workflow input, not the activity input.
    assert_eq!(
        rate_limit_key_for_exec(&mut conn, exec_acme).await,
        "dyn-rate:9:tenant_id:acme",
        "acme's key must resolve from the WORKFLOW input"
    );
    // Absent-field fallback: workflow input `{}` -> the shared fallback bucket.
    assert_eq!(
        rate_limit_key_for_exec(&mut conn, exec_empty).await,
        "dyn-rate:9:tenant_id:",
        "empty workflow input must fall back to the shared, still-bounded bucket"
    );
}

async fn task_state_workflow(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> String {
    use diesel_async::RunQueryDsl;
    #[derive(diesel::QueryableByName)]
    struct S {
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
    }
    diesel::sql_query("SELECT state FROM harvest_workflow_executions WHERE id=$1")
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .get_result::<S>(conn)
        .await
        .expect("execution state")
        .state
}

// ── AC8: concurrency + rate-limit contention safe-side invariant (issue #699, #9)

#[tokio::test]
async fn concurrency_and_rate_limit_contention_never_over_dispatches() {
    // A task under BOTH a per-key concurrency cap (#247) and a per-key rate limit
    // (#699). We drive TRUE advisory-lock contention on the concurrency_key by
    // holding the exact `pg_advisory_xact_lock(hashtext(key)::bigint)` the CTE
    // acquires, on a second connection, while a claim runs. The claim's
    // `rate_limit_debit` CTE debits a token but the `claimed` UPDATE matches 0
    // rows (lock held), so the token leaks WITHOUT the task being claimed.
    //
    // The invariant asserted here is SAFE-SIDE: the number of tasks transitioned
    // to RUNNING never EXCEEDS the concurrency cap NOR the rate budget. It may
    // over-throttle (the leaked token) but it NEVER over-dispatches.
    let (url, _g) = setup_db_url().await;
    let mut conn = connect(&url).await;
    let mut lock_conn = connect(&url).await;

    let q = "contend-queue";
    let ckey = "tenant:acme";
    let rkey = dynamic_rate_bucket_key("input.tenant_id", "acme");
    ensure_rate_limit_bucket(&mut conn, &rkey, 10.0, 10.0)
        .await
        .unwrap();

    let task = enqueue_activity(
        &mut conn,
        q,
        "charge",
        Some(rkey.clone()),
        Some((ckey.to_string(), 1)),
    )
    .await;

    let tokens_before = bucket_tokens(&mut conn, &rkey).await;

    // Hold the concurrency advisory lock on a separate connection so the claim's
    // `pg_try_advisory_xact_lock` fails for this candidate.
    lock_conn
        .batch_execute(&format!(
            "BEGIN; SELECT pg_advisory_xact_lock(hashtext('{ckey}')::bigint);"
        ))
        .await
        .expect("hold advisory lock");

    // Claim under contention: nothing is claimed (SAFE — never over-dispatch),
    // even though a token is debited.
    assert_eq!(
        claim(&mut conn, q).await,
        None,
        "advisory-lock contention must block the claim (never over-dispatch)"
    );
    assert_eq!(
        task_state(&mut conn, task).await,
        "PENDING",
        "the contended task stays PENDING — RUNNING count (0) never exceeds the cap (1)"
    );
    // Documented safe-side leak: the token was spent but no task ran.
    let tokens_after_contention = bucket_tokens(&mut conn, &rkey).await;
    assert!(
        tokens_after_contention < tokens_before,
        "a token is debited even on a lost claim (documented safe-side leak): {tokens_after_contention} < {tokens_before}"
    );

    // Release the lock; the task is now claimable and runs exactly once.
    lock_conn
        .batch_execute("ROLLBACK;")
        .await
        .expect("release lock");
    assert_eq!(
        claim(&mut conn, q).await,
        Some(task),
        "once the lock frees, the single task runs (RUNNING count 1 == cap 1)"
    );
    // Invariant across the whole test: exactly one task ever went RUNNING.
    let running: i64 = scalar_i64(
        &mut conn,
        "SELECT COUNT(*)::bigint AS n FROM harvest_task_queue \
         WHERE concurrency_key=$1 AND state='RUNNING'",
        ckey,
    )
    .await;
    assert_eq!(
        running, 1,
        "RUNNING count must never exceed the concurrency cap (1) nor the rate budget"
    );
}

// ── Success metric: cross-key burst fairness (issue #699, #10) ─────────────────

#[tokio::test]
async fn success_metric_cross_key_burst_fairness() {
    // The 10:1 burst scenario: tenant A's bucket is drained to 0 while tenant B's
    // is full. B's tasks are claimed at ~its configured rate; A's are NOT claimed
    // at all (zero cross-key bleed) — one noisy tenant cannot starve another.
    let (mut conn, _g) = setup_db().await;
    let q = "fairness-queue";
    let key_a = dynamic_rate_bucket_key("input.tenant_id", "acme");
    let key_b = dynamic_rate_bucket_key("input.tenant_id", "globex");
    // Both tenants: burst 5. A tiny refill rate keeps the test deterministic —
    // neither bucket refills meaningfully during the tight claim loop, so a
    // drained A stays drained and a full B yields exactly its burst.
    ensure_rate_limit_bucket(&mut conn, &key_a, 0.001, 5.0)
        .await
        .unwrap();
    ensure_rate_limit_bucket(&mut conn, &key_b, 0.001, 5.0)
        .await
        .unwrap();

    // Many tasks for each tenant.
    let mut a_tasks = std::collections::HashSet::new();
    let mut b_tasks = std::collections::HashSet::new();
    for _ in 0..8 {
        a_tasks.insert(enqueue_activity(&mut conn, q, "charge", Some(key_a.clone()), None).await);
        b_tasks.insert(enqueue_activity(&mut conn, q, "charge", Some(key_b.clone()), None).await);
    }

    // Drain A entirely; leave B at full burst.
    set_bucket_tokens(&mut conn, &key_a, 0.0).await;
    set_bucket_tokens(&mut conn, &key_b, 5.0).await;

    // Claim in a loop: every claim must be a B task, up to B's burst (5), then
    // the queue is throttled (A never bleeds through).
    let mut claimed_b = 0;
    for _ in 0..20 {
        match claim(&mut conn, q).await {
            Some(id) => {
                assert!(
                    b_tasks.contains(&id),
                    "a drained tenant (A) must NEVER be claimed — zero cross-key bleed"
                );
                assert!(!a_tasks.contains(&id));
                claimed_b += 1;
            }
            None => break,
        }
    }

    // B claimed at its configured burst; A untouched.
    assert_eq!(
        claimed_b, 5,
        "tenant B claimed exactly its burst (5) at full rate while A was saturated"
    );
    for a in &a_tasks {
        assert_eq!(
            task_state(&mut conn, *a).await,
            "PENDING",
            "every tenant-A task stays PENDING — no bleed from A's saturation into B"
        );
    }
}
