#![cfg(feature = "db")]
//! `quota_reconcile` candidate scan: work per tick stays bounded (issue #1631).
//!
//! The scan must read about `batch_size` rows per registered workflow
//! type. The count of unrelated `quota_key IS NULL` rows in other types
//! must not change it. Once a registered type has a large resolved
//! history, the old query scanned every unrelated candidate row instead.
//!
//! Two tests:
//!
//! - The buffer bound runs the production SQL under `EXPLAIN (ANALYZE,
//!   BUFFERS)` at two noise sizes and caps the pages touched.
//! - The cursor test registers two types with interleaved ids. It proves
//!   the merged batches keep strict global `id` order and the cursor
//!   moves past every row.

use std::collections::HashMap;
use std::sync::LazyLock;

use autumn_harvest::completion_trigger::{GLOBAL_WORKFLOW_METADATA, WorkflowMetadata};
use autumn_harvest::quota::QuotaPolicy;
use autumn_harvest::quota_reconcile::{quota_reconcile_candidate_query, reconcile_quota_keys_from};
use autumn_harvest::types::ShardId;
use diesel::QueryableByName;
use diesel::sql_types::{Array, BigInt, Nullable, Text};
use diesel_async::{AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use uuid::Uuid;

use super::claim_bench_support::db;

const BATCH: i64 = 200;
const TARGET: &str = "bound_target";
const TARGET_RESOLVED: i64 = 50_000;
const NOISE_SWEEP: [i64; 2] = [10_000, 40_000];
const NOISE_TYPES: i64 = 10;
/// One name, one batch, plus a margin. The old query read one page per
/// noise row, so it exceeded this at both sizes.
const BUFFER_CEILING: i64 = 1_500;

static TEST_SERIAL: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

/// Installs a quota policy for each name. Restores the old registry on drop.
struct MetadataGuard {
    previous: Option<HashMap<String, WorkflowMetadata>>,
    _permit: tokio::sync::MutexGuard<'static, ()>,
}

impl MetadataGuard {
    async fn install(names: &[&str]) -> Self {
        let permit = TEST_SERIAL.lock().await;
        let mut lock = GLOBAL_WORKFLOW_METADATA.write().expect("metadata lock");
        let previous = lock.take();
        let map = names
            .iter()
            .map(|name| {
                let meta = WorkflowMetadata {
                    concurrency: None,
                    max_input_bytes: None,
                    owner: None,
                    runbook_url: None,
                    severity: None,
                    input_schema: None,
                    sla: None,
                    retry_policy: None,
                    quota: Some(QuotaPolicy::new("tenant_id").with_max_active_executions(100_000)),
                };
                ((*name).to_string(), meta)
            })
            .collect();
        *lock = Some(map);
        Self {
            previous,
            _permit: permit,
        }
    }
}

impl Drop for MetadataGuard {
    fn drop(&mut self) {
        if let Ok(mut lock) = GLOBAL_WORKFLOW_METADATA.write() {
            *lock = self.previous.take();
        }
    }
}

async fn bench_db_or_skip() -> Option<db::BenchDb> {
    match db::setup_bench_db().await {
        Ok(bench) => Some(bench),
        Err(reason) => {
            assert!(
                std::env::var("CI").is_err(),
                "quota_reconcile candidate-bound test could not reach a database under CI: {}",
                reason.0,
            );
            eprintln!(
                "SKIP: quota_reconcile candidate-bound test needs Postgres ({})",
                reason.0,
            );
            None
        }
    }
}

/// Seed the issue #1631 shape: a matured registered type plus noise.
///
/// The target has `BATCH` unresolved rows and a large resolved history.
/// Noise rows belong to unregistered types and all have `quota_key IS NULL`.
async fn seed(conn: &mut AsyncPgConnection, noise: i64) {
    let sql = format!(
        "TRUNCATE harvest_workflow_executions CASCADE; \
         INSERT INTO harvest_workflow_executions \
           (id, workflow_name, workflow_id, shard_id, state, input, quota_key) \
         SELECT md5('b-open-' || i)::uuid, '{TARGET}', 'open-' || i, 0, \
                CASE WHEN i % 11 = 0 THEN 'PAUSED' ELSE 'RUNNING' END, \
                jsonb_build_object('tenant_id', 't' || (i % 25)), NULL \
         FROM generate_series(1, {BATCH}) AS i; \
         INSERT INTO harvest_workflow_executions \
           (id, workflow_name, workflow_id, shard_id, state, input, quota_key) \
         SELECT md5('b-done-' || i)::uuid, '{TARGET}', 'done-' || i, 0, \
                CASE WHEN i % 5 = 0 THEN 'COMPLETED' ELSE 'RUNNING' END, \
                jsonb_build_object('tenant_id', 't' || (i % 25)), 't' || (i % 25) \
         FROM generate_series(1, {TARGET_RESOLVED}) AS i; \
         INSERT INTO harvest_workflow_executions \
           (id, workflow_name, workflow_id, shard_id, state, input, quota_key) \
         SELECT md5('b-noise-' || i)::uuid, 'bound_noise_' || (i % {NOISE_TYPES}), \
                'noise-' || i, 0, \
                CASE WHEN i % 11 = 0 THEN 'PAUSED' ELSE 'RUNNING' END, \
                jsonb_build_object('tenant_id', 'o' || (i % 25)), NULL \
         FROM generate_series(1, {noise}) AS i; \
         ANALYZE harvest_workflow_executions;"
    );
    conn.batch_execute(&sql)
        .await
        .expect("seed candidate-bound fixture");
}

#[derive(QueryableByName)]
struct PlanLine {
    #[diesel(sql_type = Text, column_name = "QUERY PLAN")]
    line: String,
}

/// Pages the production candidate SQL touches, hit plus read.
async fn candidate_scan_buffers(conn: &mut AsyncPgConnection) -> i64 {
    let rows: Vec<PlanLine> = diesel::sql_query(format!(
        "EXPLAIN (ANALYZE, BUFFERS) {}",
        quota_reconcile_candidate_query()
    ))
    .bind::<Array<Text>, _>(vec![TARGET.to_string()])
    .bind::<Nullable<diesel::sql_types::Uuid>, _>(None::<Uuid>)
    .bind::<BigInt, _>(BATCH)
    .load(conn)
    .await
    .expect("EXPLAIN candidate scan");
    // The first `Buffers:` line belongs to the root node and covers the plan.
    let line = rows
        .iter()
        .map(|r| r.line.as_str())
        .find(|l| l.trim_start().starts_with("Buffers: shared"))
        .expect("plan has a root Buffers line");
    ["hit=", "read="]
        .iter()
        .filter_map(|key| line.split(key).nth(1))
        .filter_map(|rest| rest.split(|c: char| !c.is_ascii_digit()).next())
        .filter_map(|n| n.parse::<i64>().ok())
        .sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn candidate_scan_buffers_do_not_grow_with_other_type_backlog() {
    let Some(bench) = bench_db_or_skip().await else {
        return;
    };
    let mut conn = db::connect(&bench.url).await;
    for noise in NOISE_SWEEP {
        seed(&mut conn, noise).await;
        let buffers = candidate_scan_buffers(&mut conn).await;
        assert!(
            buffers <= BUFFER_CEILING,
            "candidate scan touched {buffers} pages with {noise} unrelated NULL rows; \
             the ceiling is {BUFFER_CEILING}",
        );
    }
}

fn ordered_id(n: u32) -> Uuid {
    Uuid::parse_str(&format!("00000000-0000-0000-0000-{n:012}")).expect("valid uuid")
}

async fn insert_row(
    conn: &mut AsyncPgConnection,
    n: u32,
    name: &str,
    state: &str,
    input: &serde_json::Value,
) {
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
           (id, workflow_name, workflow_id, shard_id, state, input, quota_key) \
         VALUES ($1, $2, $3, 0, $4, $5, NULL)",
    )
    .bind::<diesel::sql_types::Uuid, _>(ordered_id(n))
    .bind::<Text, _>(name)
    .bind::<Text, _>(format!("wf-{n}"))
    .bind::<Text, _>(state)
    .bind::<diesel::sql_types::Jsonb, _>(input.clone())
    .execute(conn)
    .await
    .expect("insert row");
}

async fn quota_key_of(conn: &mut AsyncPgConnection, n: u32) -> Option<String> {
    #[derive(QueryableByName)]
    struct Row {
        #[diesel(sql_type = Nullable<Text>)]
        quota_key: Option<String>,
    }
    let row: Row =
        diesel::sql_query("SELECT quota_key FROM harvest_workflow_executions WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(ordered_id(n))
            .get_result(conn)
            .await
            .expect("read quota_key");
    row.quota_key
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn merged_batches_keep_global_id_order_across_registered_types() {
    let Some(bench) = bench_db_or_skip().await else {
        return;
    };
    let _guard = MetadataGuard::install(&["bound_even", "bound_odd"]).await;
    let mut conn = db::connect(&bench.url).await;
    diesel::sql_query("TRUNCATE harvest_workflow_executions CASCADE")
        .execute(&mut conn)
        .await
        .expect("truncate");
    let resolvable = serde_json::json!({"tenant_id": "t"});
    // Twelve registered rows at ids 10, 20, ... 120. Odd positions belong to
    // one type, even positions to the other. Row 3 is PAUSED. Row 6 has no
    // tenant, so it never resolves.
    for n in 1..=12_u32 {
        let name = if n % 2 == 0 {
            "bound_even"
        } else {
            "bound_odd"
        };
        let state = if n == 3 { "PAUSED" } else { "RUNNING" };
        let input = if n == 6 {
            serde_json::json!({})
        } else {
            resolvable.clone()
        };
        insert_row(&mut conn, n * 10, name, state, &input).await;
    }
    // Unregistered rows sit between registered ids and must stay untouched.
    for n in [25_u32, 85] {
        insert_row(&mut conn, n, "bound_unregistered", "RUNNING", &resolvable).await;
    }

    let shard = Some(ShardId::new(0));
    let (first, cursor) = reconcile_quota_keys_from(&mut conn, 5, None, shard)
        .await
        .expect("first tick");
    assert_eq!(first.backfilled, 5);
    assert_eq!(
        cursor,
        Some(ordered_id(50)),
        "cursor is the 5th registered id"
    );

    let (second, cursor) = reconcile_quota_keys_from(&mut conn, 5, cursor, shard)
        .await
        .expect("second tick");
    assert_eq!(second.backfilled, 4);
    assert_eq!(second.unresolvable, 1, "the cursor still moves past row 6");
    assert_eq!(cursor, Some(ordered_id(100)));

    let (third, cursor) = reconcile_quota_keys_from(&mut conn, 5, cursor, shard)
        .await
        .expect("third tick");
    assert_eq!(third.backfilled, 2);
    assert_eq!(cursor, None, "a short batch wraps the cursor");

    assert_eq!(
        quota_key_of(&mut conn, 30).await.as_deref(),
        Some("t"),
        "PAUSED row"
    );
    assert_eq!(quota_key_of(&mut conn, 60).await, None, "unresolvable row");
    assert_eq!(quota_key_of(&mut conn, 25).await, None, "unregistered row");
    assert_eq!(quota_key_of(&mut conn, 85).await, None, "unregistered row");
}
