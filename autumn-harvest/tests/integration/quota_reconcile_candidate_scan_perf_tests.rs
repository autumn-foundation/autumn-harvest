#![cfg(feature = "db")]
//! Ledger perf pass: `quota_reconcile::CANDIDATE_SQL`'s residual
//! `workflow_name = ANY($1)` filter (issue #1226).
//!
//! # Workload
//!
//! [`reconcile_quota_keys_from`] runs on `worker_heartbeat_interval`
//! cadence, once per assigned shard, on every worker process. Its candidate
//! scan is backed by `idx_harvest_we_quota_reconcile_candidates` (migration
//! `20260910192721_harvest_quota_reconcile_candidate_index`), a partial
//! index on `(id) WHERE quota_key IS NULL AND state IN ('RUNNING',
//! 'PAUSED')`. That index has no `workflow_name` column, so
//! `workflow_name = ANY($1)` is a residual filter evaluated row by row
//! against each id-ordered candidate, not an index seek.
//!
//! The migration's own comment names this a "KNOWN SCALING TRADE-OFF".
//! Picture a mixed deployment with a large non-quota'd population and few
//! matching rows. One tick can then touch many discarded rows before
//! filling `batch_size`. The comment proposes and rejects a
//! `(workflow_name, id)` index as an alternative. It states the trade-off
//! "needs `EXPLAIN ANALYZE` against production-scale data" and is
//! "tracked as a follow-up, not silently accepted." This file is that
//! follow-up.
//!
//! # Fixture
//!
//! Seeded, deterministic (no `random()`), production-shaped. One quota'd
//! workflow type (`billing_saga`, policy key `tenant_id`) has a small,
//! fixed pre-upgrade backfill population. That population is 1,000
//! `RUNNING`/`PAUSED` rows with `quota_key IS NULL`, matching the issue
//! #1226 rollout-window scenario. [`TARGET_TERMINAL`] adds 50,000 of its
//! own terminal rows too — a realistic accumulated history for a
//! workflow type important enough to have a declared `QuotaPolicy`.
//! `idx_harvest_wfx_workflow_identity` is not partial by state, so this
//! history matters directly to this file's measurement; see
//! [`TARGET_TERMINAL`]'s doc comment.
//!
//! Alongside it sits a large population of 50 distinct NON-quota'd
//! workflow types with no declared `QuotaPolicy`. Those rows are also
//! non-terminal and also `quota_key IS NULL`. This is the realistic case
//! a busy shard produces. Most workflow types never adopt quotas, but
//! their non-terminal rows still sit under the same partial index
//! forever, since nothing ever sets their `quota_key`. [`NOISE_SWEEP`]
//! scales that population across three sizes: 20,000 / 100,000 /
//! 500,000. The target population and `batch_size`
//! (`QUOTA_RECONCILE_DEFAULT_BATCH` = 200) stay fixed throughout, so any
//! change in rows touched per batch isolates the noise population's
//! effect. A 10% terminal (`COMPLETED`) population is also seeded per
//! noise size, outside the index's `state IN (...)` predicate, for a
//! realistic terminal/non-terminal mix.
//!
//! Row ids are server-generated UUIDs. Target and noise rows are
//! therefore already interleaved uniformly in `id` order, with no extra
//! scattering step needed — exactly the ordering `ORDER BY id` scans.
//!
//! Evidence-capture test below is ignored by default. Run via
//! `autumn-harvest/scripts/quota_reconcile_candidate_scan_perf_repro.sh`.
//! Full write-up: `docs/performance-quota-reconcile-candidate-scan.md`.

use std::collections::HashMap;
use std::sync::LazyLock;

use autumn_harvest::completion_trigger::{GLOBAL_WORKFLOW_METADATA, WorkflowMetadata};
use autumn_harvest::quota::QuotaPolicy;
use autumn_harvest::quota_reconcile::{
    QUOTA_RECONCILE_DEFAULT_BATCH, quota_reconcile_candidate_query, reconcile_quota_keys_from,
};
use autumn_harvest::types::ShardId;
use diesel::QueryableByName;
use diesel::sql_types::{Array, BigInt, Nullable, Text};
use diesel_async::RunQueryDsl;

use super::claim_bench_support::db;

const NOISE_SWEEP: [i64; 3] = [20_000, 100_000, 500_000];
const TARGET_ACTIVE: i64 = 1_000;
/// `idx_harvest_wfx_workflow_identity` is NOT partial by state. A busy
/// quota'd workflow type accumulates terminal history of its own, and any
/// plan that seeks through that index scans this history too. Fixed
/// (independent of [`NOISE_SWEEP`]) so its effect isolates from the noise
/// population's effect (issue #1226 follow-up review, PR #1691).
const TARGET_TERMINAL: i64 = 50_000;
const TARGET_WORKFLOW: &str = "billing_saga";
const NOISE_WORKFLOW_TYPES: i64 = 50;
const SHARD: Option<ShardId> = Some(ShardId::new(0));

/// Serializes access to the process-global `GLOBAL_WORKFLOW_METADATA`
/// mirror across this file's tests, mirroring `quota_reconcile_tests.rs`'s
/// identical `TEST_SERIAL` convention.
static TEST_SERIAL: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

/// RAII installer for `GLOBAL_WORKFLOW_METADATA`. Installs one policy for
/// [`TARGET_WORKFLOW`] and restores whatever was there before on drop.
struct MetadataGuard {
    previous: Option<HashMap<String, WorkflowMetadata>>,
    _permit: tokio::sync::MutexGuard<'static, ()>,
}

impl MetadataGuard {
    async fn install_target_policy() -> Self {
        let permit = TEST_SERIAL.lock().await;
        let previous = {
            let mut lock = GLOBAL_WORKFLOW_METADATA.write().expect("metadata lock");
            lock.take()
        };
        let mut map = HashMap::new();
        map.insert(
            TARGET_WORKFLOW.to_string(),
            WorkflowMetadata {
                concurrency: None,
                max_input_bytes: None,
                owner: None,
                runbook_url: None,
                severity: None,
                input_schema: None,
                sla: None,
                retry_policy: None,
                quota: Some(QuotaPolicy::new("tenant_id").with_max_active_executions(100_000)),
            },
        );
        {
            let mut lock = GLOBAL_WORKFLOW_METADATA.write().expect("metadata lock");
            *lock = Some(map);
        }
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
                "quota_reconcile candidate-scan evidence capture could not reach a \
                 database under CI: {}. Set HARVEST_TEST_DATABASE_URL or start Docker.",
                reason.0,
            );
            eprintln!(
                "SKIP: quota_reconcile candidate-scan evidence capture needs Postgres \
                 ({}). Set HARVEST_TEST_DATABASE_URL or start Docker.",
                reason.0,
            );
            None
        }
    }
}

/// Deterministically (re)seed the target population plus `noise_count`
/// non-quota'd rows spread across [`NOISE_WORKFLOW_TYPES`] distinct
/// workflow types, then `ANALYZE`.
///
/// Idempotent: `TRUNCATE`s `harvest_workflow_executions` first.
async fn seed_fixture(conn: &mut diesel_async::AsyncPgConnection, noise_count: i64) {
    diesel::sql_query("TRUNCATE harvest_workflow_executions CASCADE")
        .execute(conn)
        .await
        .expect("truncate quota_reconcile fixture table");

    diesel::sql_query(format!(
        "INSERT INTO harvest_workflow_executions \
           (id, workflow_name, workflow_id, shard_id, state, input, quota_key) \
         SELECT gen_random_uuid(), '{TARGET_WORKFLOW}', 'target-' || i, 0, \
                CASE WHEN i % 5 = 0 THEN 'PAUSED' ELSE 'RUNNING' END, \
                jsonb_build_object('tenant_id', 'tenant_' || (i % 500)), \
                NULL \
         FROM generate_series(1, {TARGET_ACTIVE}) AS i"
    ))
    .execute(conn)
    .await
    .expect("seed target workflow rows");

    diesel::sql_query(format!(
        "INSERT INTO harvest_workflow_executions \
           (id, workflow_name, workflow_id, shard_id, state, input, quota_key) \
         SELECT gen_random_uuid(), '{TARGET_WORKFLOW}', 'target-terminal-' || i, 0, \
                'COMPLETED', jsonb_build_object('tenant_id', 'tenant_' || (i % 500)), \
                'tenant_' || (i % 500) \
         FROM generate_series(1, {TARGET_TERMINAL}) AS i"
    ))
    .execute(conn)
    .await
    .expect("seed target workflow terminal history");

    diesel::sql_query(format!(
        "INSERT INTO harvest_workflow_executions \
           (id, workflow_name, workflow_id, shard_id, state, input, quota_key) \
         SELECT gen_random_uuid(), 'noise_wf_' || (i % {NOISE_WORKFLOW_TYPES}), \
                'noise-' || i, 0, \
                CASE WHEN i % 5 = 0 THEN 'PAUSED' ELSE 'RUNNING' END, \
                jsonb_build_object('tenant_id', 'tenant_' || (i % 500)), \
                NULL \
         FROM generate_series(1, {noise_count}) AS i"
    ))
    .execute(conn)
    .await
    .expect("seed noise workflow rows");

    let terminal_count = noise_count / 10;
    diesel::sql_query(format!(
        "INSERT INTO harvest_workflow_executions \
           (id, workflow_name, workflow_id, shard_id, state, input, quota_key) \
         SELECT gen_random_uuid(), 'noise_wf_' || (i % {NOISE_WORKFLOW_TYPES}), \
                'terminal-' || i, 0, 'COMPLETED', \
                jsonb_build_object('tenant_id', 'tenant_' || (i % 500)), \
                NULL \
         FROM generate_series(1, {terminal_count}) AS i"
    ))
    .execute(conn)
    .await
    .expect("seed terminal workflow rows");

    diesel::sql_query("ANALYZE harvest_workflow_executions")
        .execute(conn)
        .await
        .expect("analyze harvest_workflow_executions");
}

#[derive(QueryableByName)]
struct ExplainRow {
    #[diesel(sql_type = Text, column_name = "QUERY PLAN")]
    query_plan: String,
}

async fn explain_candidate_scan(conn: &mut diesel_async::AsyncPgConnection) -> String {
    let sql = format!(
        "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS) {}",
        quota_reconcile_candidate_query()
    );
    let rows: Vec<ExplainRow> = diesel::sql_query(sql)
        .bind::<Array<Text>, _>(vec![TARGET_WORKFLOW.to_string()])
        .bind::<Nullable<diesel::sql_types::Uuid>, _>(None::<uuid::Uuid>)
        .bind::<BigInt, _>(QUOTA_RECONCILE_DEFAULT_BATCH)
        .load(conn)
        .await
        .expect("EXPLAIN quota_reconcile_candidate_query()");
    rows.into_iter()
        .map(|r| r.query_plan)
        .collect::<Vec<_>>()
        .join("\n")
}

/// The migration comment proposes and rejects a `(workflow_name, id)`
/// partial index. Tested here against the SAME unmodified `CANDIDATE_SQL`
/// text, to see whether the planner would even choose it. Dropped again
/// immediately after the capture -- never left in the schema.
async fn explain_with_alternative_index(conn: &mut diesel_async::AsyncPgConnection) -> String {
    diesel::sql_query(
        "CREATE INDEX idx_ledger_experiment_we_quota_reconcile_by_name_id \
         ON harvest_workflow_executions (workflow_name, id) \
         WHERE quota_key IS NULL AND state IN ('RUNNING', 'PAUSED')",
    )
    .execute(conn)
    .await
    .expect("create alternative experiment index");
    diesel::sql_query("ANALYZE harvest_workflow_executions")
        .execute(conn)
        .await
        .expect("analyze after alternative index");

    let plan_text = explain_candidate_scan(conn).await;

    diesel::sql_query("DROP INDEX idx_ledger_experiment_we_quota_reconcile_by_name_id")
        .execute(conn)
        .await
        .expect("drop alternative experiment index");

    plan_text
}

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    count: i64,
}

#[derive(QueryableByName, Debug)]
struct StatRow {
    #[diesel(sql_type = BigInt)]
    calls: i64,
    #[diesel(sql_type = BigInt)]
    shared_blks_hit: i64,
    #[diesel(sql_type = BigInt)]
    shared_blks_read: i64,
    #[diesel(sql_type = BigInt)]
    total_buffers: i64,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[ignore = "evidence generator, not a CI assertion -- run via \
            autumn-harvest/scripts/quota_reconcile_candidate_scan_perf_repro.sh"]
#[allow(clippy::too_many_lines)] // one-shot evidence capture, not control flow
async fn zz_capture_quota_reconcile_candidate_scan_evidence() {
    let Some(bench) = bench_db_or_skip().await else {
        eprintln!("no database reachable; nothing captured");
        return;
    };
    let _guard = MetadataGuard::install_target_policy().await;

    let out_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("autumn-harvest/ has a workspace-root parent")
        .join("docs")
        .join("perf-artifacts")
        .join("quota-reconcile-candidate-scan");
    std::fs::create_dir_all(&out_dir).expect("create artifact output directory");

    let mut conn = db::connect(&bench.url).await;
    let mut summary_lines: Vec<String> = Vec::new();

    for noise_count in NOISE_SWEEP {
        seed_fixture(&mut conn, noise_count).await;

        let plan_text = explain_candidate_scan(&mut conn).await;
        let file_name = format!("noise-{noise_count}.explain.txt");
        std::fs::write(
            out_dir.join(&file_name),
            format!(
                "-- CANDIDATE_SQL @ target={TARGET_ACTIVE} rows / noise={noise_count} rows \
                 across {NOISE_WORKFLOW_TYPES} non-quota'd workflow types, first tick \
                 (after_id=NULL, batch_size={QUOTA_RECONCILE_DEFAULT_BATCH}) --\n{plan_text}\n"
            ),
        )
        .expect("write explain artifact");
        eprintln!("wrote {file_name}");

        if noise_count == NOISE_SWEEP[NOISE_SWEEP.len() - 1] {
            let alt_plan_text = explain_with_alternative_index(&mut conn).await;
            std::fs::write(
                out_dir.join("alternative-index-explain.txt"),
                format!(
                    "-- SAME unmodified CANDIDATE_SQL, with the migration comment's \
                     rejected (workflow_name, id) partial index also present \
                     @ noise={noise_count} -- shows whether the planner would even \
                     choose it --\n{alt_plan_text}\n"
                ),
            )
            .expect("write alternative-index artifact");
        }

        summary_lines.push(format!(
            "noise={noise_count} target_active={TARGET_ACTIVE} terminal={}",
            noise_count / 10
        ));
    }

    // Real reconcile_quota_keys_from() calls at the largest fixture, driven
    // to completion (a full pass), for the pg_stat_statements snapshot.
    seed_fixture(&mut conn, NOISE_SWEEP[NOISE_SWEEP.len() - 1]).await;

    let _ = diesel::sql_query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .execute(&mut conn)
        .await;
    let available: Result<i64, _> =
        diesel::sql_query("SELECT count(*)::BIGINT AS count FROM pg_stat_statements WHERE FALSE")
            .get_result::<CountRow>(&mut conn)
            .await
            .map(|r| r.count);
    let reset_result = if available.is_ok() {
        Some(
            diesel::sql_query(
                "SELECT pg_stat_statements_reset(0, \
                        (SELECT oid FROM pg_database WHERE datname = current_database()), 0)",
            )
            .execute(&mut conn)
            .await,
        )
    } else {
        None
    };
    let skip_reason = match (&available, &reset_result) {
        (Err(_), _) => Some(
            "pg_stat_statements is not usable on this server (needs \
             shared_preload_libraries = 'pg_stat_statements')",
        ),
        (Ok(_), Some(Err(_))) => Some(
            "pg_stat_statements_reset() failed -- the connected role likely lacks reset permission",
        ),
        _ => None,
    };

    let mut total_calls = 0i64;
    let mut total_backfilled = 0usize;
    if skip_reason.is_none() {
        let mut cursor = None;
        loop {
            let (summary, next_cursor) =
                reconcile_quota_keys_from(&mut conn, QUOTA_RECONCILE_DEFAULT_BATCH, cursor, SHARD)
                    .await
                    .expect("reconcile_quota_keys_from");
            total_calls += 1;
            total_backfilled += summary.backfilled;
            if next_cursor.is_none() {
                break;
            }
            cursor = next_cursor;
        }
        assert_eq!(
            total_backfilled,
            usize::try_from(TARGET_ACTIVE).expect("TARGET_ACTIVE fits in usize"),
            "every target row must be backfilled by the end of one full pass"
        );
    }

    if let Some(reason) = skip_reason {
        eprintln!("SKIP: {reason} -- skipping the pg_stat_statements snapshot.");
        std::fs::write(
            out_dir.join("pg_stat_statements.txt"),
            format!(
                "-- SKIPPED: {reason}. See the EXPLAIN artifacts for the primary evidence. --\n"
            ),
        )
        .expect("write pg_stat_statements skip notice");
    } else {
        let stats_result: Result<Vec<StatRow>, _> = diesel::sql_query(
            "SELECT calls, shared_blks_hit, shared_blks_read, \
                    (shared_blks_hit + shared_blks_read) AS total_buffers \
             FROM pg_stat_statements \
             WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) \
               AND query LIKE '%quota_key IS NULL AND state IN%' \
             ORDER BY total_buffers DESC",
        )
        .load(&mut conn)
        .await;

        match stats_result {
            Ok(stats) if !stats.is_empty() => {
                let stats_text = stats
                    .iter()
                    .map(|r| {
                        format!(
                            "calls={} shared_blks_hit={} shared_blks_read={} total_buffers={}",
                            r.calls, r.shared_blks_hit, r.shared_blks_read, r.total_buffers
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                std::fs::write(
                    out_dir.join("pg_stat_statements.txt"),
                    format!(
                        "-- pg_stat_statements after one full reconcile pass \
                         ({total_calls} ticks, {total_backfilled} rows backfilled) \
                         @ noise={} (largest fixture), scoped to this database's dbid --\n{stats_text}\n",
                        NOISE_SWEEP[NOISE_SWEEP.len() - 1]
                    ),
                )
                .expect("write pg_stat_statements artifact");
            }
            _ => {
                let reason = "pg_stat_statements recorded no matching row for the driven calls";
                eprintln!("SKIP: {reason}.");
                std::fs::write(
                    out_dir.join("pg_stat_statements.txt"),
                    format!("-- SKIPPED: {reason}. See the EXPLAIN artifacts for the primary evidence. --\n"),
                )
                .expect("write pg_stat_statements skip notice");
            }
        }
    }

    summary_lines.push(format!(
        "full pass @ largest fixture: ticks={total_calls} backfilled={total_backfilled}"
    ));
    std::fs::write(
        out_dir.join("fixture-summary.txt"),
        summary_lines.join("\n") + "\n",
    )
    .expect("write fixture summary");

    eprintln!("== capture complete: artifacts in {} ==", out_dir.display());
}
