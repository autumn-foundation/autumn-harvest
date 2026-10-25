#![cfg(feature = "db")]
//! Ledger performance investigation: the queue age gauge (issue #2339).
//!
//! `queue::oldest_pending_ages` feeds the `harvest.queue.oldest_pending_age`
//! gauge and the load shedder. The queue depth sampler runs it once per worker
//! poll interval, right beside `queue::queue_depths`. The query reads every
//! eligible pending row of the queue, fetches each heap tuple, and tests the
//! claim-gate filters on it. So its cost grows with the backlog, not with the
//! answer, which is one timestamp per queue.
//!
//! The harness replays worker poll ticks against a seeded backlog. One tick
//! runs, for each of four workers: `queue_depths`, `oldest_pending_ages` and one
//! `claim_task` with its `complete_claimed_task`. That is the shape of the
//! worker loop with a metrics recorder configured.
//!
//! The fixture is skewed. It has 20 queues with a cubic skew and 40% workflow
//! tasks. It has 1% paused executions and 5% future-scheduled rows. It has 10%
//! rows with a `schedule_to_close_at`, 5% concurrency-capped rows and 3%
//! rate-limited rows. A terminal row per pending row stays in the table, as an
//! unswept queue holds them. Everything derives from a seeded `random()`, so one
//! size gives one table. A row is created at its due time. One row in ten
//! waited out a backoff, so it was created up to 30 minutes before.
//!
//! The harness reads `pg_stat_statements`. Evidence is call counts and buffers.
//! Wall-clock is not evidence. Output goes to
//! `docs/perf-artifacts/queue-age-gauge/` under the label in `PERF_LABEL`.
//!
//! Two tests:
//! - [`zz_capture_queue_age_gauge_evidence`] is the `#[ignore]`d evidence
//!   generator. It needs `pg_stat_statements` and a superuser.
//! - [`age_gauge_matches_the_reference_query`] is an always-run equivalence
//!   check. It compares the public function with the reference query on edge
//!   cases, inside one transaction so that `NOW()` is the same for both.

use std::fmt::Write as _;

use autumn_harvest::queue::{self, TaskClaim};
use diesel::sql_types::{BigInt, Double, Text};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::json;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use crate::throwaway_db::ThrowawayDb;

/// Queues the workers poll.
const QUEUES: i64 = 20;

/// Workers that sample on each tick.
const WORKERS: usize = 4;

/// Poll ticks per measurement.
const TICKS: usize = 10;

/// The query as it stood before issue #2339. It is the equivalence oracle.
const REFERENCE_SQL: &str = "SELECT queue_name, GREATEST( EXTRACT(EPOCH FROM (NOW() - MIN(GREATEST(scheduled_at, COALESCE(created_at, scheduled_at))))), 0 )::DOUBLE PRECISION AS age_secs FROM harvest_task_queue WHERE queue_name = ANY($1) AND state = 'PENDING' AND scheduled_at <= NOW() AND NOT EXISTS (SELECT 1 FROM harvest_queue_pauses qp WHERE qp.queue_name = harvest_task_queue.queue_name) AND NOT EXISTS (SELECT 1 FROM harvest_activity_pauses ap WHERE ap.activity_name = harvest_task_queue.activity_name AND harvest_task_queue.task_type = 'activity') AND ( schedule_to_close_at IS NULL OR schedule_to_close_at > NOW() ) AND ( task_type <> 'workflow' OR workflow_exec_id IS NULL OR NOT EXISTS ( SELECT 1 FROM harvest_workflow_executions e WHERE e.id = harvest_task_queue.workflow_exec_id AND e.state = 'PAUSED' ) ) AND ( concurrency_key IS NULL OR concurrency_cap IS NULL OR ( SELECT COUNT(*) FROM harvest_task_queue inner_q WHERE inner_q.concurrency_key = harvest_task_queue.concurrency_key AND inner_q.task_type = harvest_task_queue.task_type AND inner_q.state = 'RUNNING' AND inner_q.worker_id IS NOT NULL ) < harvest_task_queue.concurrency_cap ) AND ( rate_limit_key IS NULL OR activity_name = ANY($2) OR EXISTS ( SELECT 1 FROM harvest_rate_limit_buckets b WHERE b.key = harvest_task_queue.rate_limit_key AND LEAST(COALESCE(CASE WHEN b.override_expires_at > NOW() THEN b.override_burst ELSE NULL END, b.burst), b.tokens + GREATEST(0, EXTRACT(EPOCH FROM (LEAST(COALESCE(b.override_expires_at, b.last_refilled_at), NOW()) - b.last_refilled_at))) * COALESCE(b.override_refill_rate, b.refill_rate) + GREATEST(0, EXTRACT(EPOCH FROM (NOW() - GREATEST(COALESCE(b.override_expires_at, b.last_refilled_at), b.last_refilled_at)))) * b.refill_rate) >= 1.0 ) ) GROUP BY queue_name";

fn queue_names() -> Vec<String> {
    (0..QUEUES).map(|i| format!("q{i}")).collect()
}

/// Prefer an operator-supplied server and fall back to testcontainers.
async fn setup_server() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_tag("16")
        .with_cmd(["-c", "shared_preload_libraries=pg_stat_statements"])
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

/// Seeds `pending` PENDING rows, a tenth as many RUNNING rows, and as many
/// terminal rows as pending ones.
async fn seed(conn: &mut AsyncPgConnection, pending: i64) {
    let sql = format!(
        "SELECT setseed(0.42); \
         INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, shard_id, state, input, queue_name) \
         SELECT gen_random_uuid(), 'wf' || (g % 30), 'wid-' || g, 0, \
                CASE WHEN g % 100 = 0 THEN 'PAUSED' WHEN g % 7 = 0 THEN 'COMPLETED' \
                     ELSE 'RUNNING' END, \
                '{{}}'::jsonb, 'q' || floor({QUEUES} * power(random(), 3))::int \
         FROM generate_series(1, ({pending} * 0.5)::int) g; \
         CREATE TEMP TABLE ex AS \
             SELECT id, row_number() OVER (ORDER BY workflow_id) AS rn \
             FROM harvest_workflow_executions; \
         INSERT INTO harvest_task_queue \
             (queue_name, task_type, input, state, priority, scheduled_at, workflow_exec_id, \
              activity_name, schedule_to_close_at, concurrency_key, concurrency_cap, \
              rate_limit_key, created_at) \
         SELECT 'q' || floor({QUEUES} * power(random(), 3))::int, \
                CASE WHEN g % 5 < 2 THEN 'workflow' ELSE 'activity' END, '{{}}'::jsonb, \
                'PENDING', (random() * 3)::int, \
                CASE WHEN random() < 0.05 THEN now() + interval '1 hour' \
                     ELSE now() - (random() * interval '6 hours') END, \
                CASE WHEN g % 5 < 2 \
                     THEN (SELECT id FROM ex WHERE rn = 1 + (g % (SELECT count(*) FROM ex))) END, \
                CASE WHEN g % 5 >= 2 THEN 'act' || (g % 60) END, \
                CASE WHEN random() < 0.10 THEN now() + interval '2 hours' END, \
                CASE WHEN random() < 0.05 THEN 'ck' || (g % 40) END, \
                CASE WHEN random() < 0.05 THEN 10 END, \
                CASE WHEN random() < 0.03 THEN 'rl' || (g % 5) END, \
                NULL \
         FROM generate_series(1, {pending}) g; \
         UPDATE harvest_task_queue \
            SET created_at = scheduled_at \
                - CASE WHEN random() < 0.1 THEN random() * interval '30 minutes' \
                       ELSE interval '0' END \
          WHERE state = 'PENDING'; \
         INSERT INTO harvest_task_queue \
             (queue_name, task_type, input, state, worker_id, activity_name, \
              concurrency_key, concurrency_cap, started_at) \
         SELECT 'q' || floor({QUEUES} * power(random(), 3))::int, 'activity', '{{}}'::jsonb, \
                'RUNNING', 'w' || (g % 50), 'act' || (g % 60), \
                CASE WHEN random() < 0.3 THEN 'ck' || (g % 40) END, \
                CASE WHEN random() < 0.3 THEN 10 END, now() \
         FROM generate_series(1, ({pending} * 0.1)::int) g; \
         INSERT INTO harvest_task_queue \
             (queue_name, task_type, input, state, activity_name, completed_at) \
         SELECT 'q' || floor({QUEUES} * power(random(), 3))::int, 'activity', '{{}}'::jsonb, \
                'COMPLETED', 'act' || (g % 60), now() - (random() * interval '1 hour') \
         FROM generate_series(1, {pending}) g; \
         DROP TABLE ex;"
    );
    conn.batch_execute(&sql).await.expect("seed the backlog");
    // A steady-state table has a current visibility map and fresh statistics.
    // VACUUM cannot share a transaction block with the seed.
    for table in ["harvest_task_queue", "harvest_workflow_executions"] {
        conn.batch_execute(&format!("VACUUM ANALYZE {table}"))
            .await
            .expect("vacuum");
    }
}

#[derive(diesel::QueryableByName)]
struct StatRow {
    #[diesel(sql_type = Text)]
    query: String,
    #[diesel(sql_type = BigInt)]
    calls: i64,
    #[diesel(sql_type = BigInt)]
    buffers: i64,
    #[diesel(sql_type = BigInt)]
    wal_bytes: i64,
}

async fn snapshot(conn: &mut AsyncPgConnection, db_name: &str) -> Vec<StatRow> {
    diesel::sql_query(format!(
        "SELECT query, calls, (shared_blks_hit + shared_blks_read) AS buffers, \
                wal_bytes::bigint AS wal_bytes \
         FROM pg_stat_statements \
         WHERE dbid = (SELECT oid FROM pg_database WHERE datname = '{db_name}') \
           AND query NOT ILIKE '%pg_stat_statements%' \
         ORDER BY buffers DESC, query"
    ))
    .load(conn)
    .await
    .expect("pg_stat_statements must be preloaded")
}

/// One summed row of the measurement.
struct Point {
    pending: i64,
    statements: i64,
    buffers: i64,
    age_calls: i64,
    age_buffers: i64,
    depth_buffers: i64,
    claim_buffers: i64,
    wal_bytes: i64,
    ages: Vec<(String, i64)>,
}

async fn measure(admin: &str, label: &str, pending: i64) -> (Point, String) {
    let db = ThrowawayDb::create_on(admin, "queue_age_gauge_perf").await;
    let db_name = db.name().to_owned();
    let mut conn = AsyncPgConnection::establish(&db.url())
        .await
        .expect("connection");
    conn.batch_execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .await
        .expect("pg_stat_statements extension");
    seed(&mut conn, pending).await;
    diesel::sql_query(format!(
        "SELECT pg_stat_statements_reset(0, \
                (SELECT oid FROM pg_database WHERE datname = '{db_name}'), 0)"
    ))
    .execute(&mut conn)
    .await
    .expect("pg_stat_statements_reset needs a superuser");

    let queues = queue_names();
    let mut last_ages = Vec::new();
    for tick in 0..TICKS {
        for worker in 0..WORKERS {
            queue::queue_depths(&mut conn, &queues)
                .await
                .expect("depths");
            last_ages = queue::oldest_pending_ages(&mut conn, &queues, &[])
                .await
                .expect("ages")
                .into_iter()
                // Whole seconds: the age moves with the clock between runs.
                .map(|(q, secs)| (q, secs.floor() as i64))
                .collect();
            let worker_id = format!("perf-w{worker}-t{tick}");
            if let Some(task) =
                queue::claim_task(&mut conn, &queues, &worker_id, "", None, &[], &[])
                    .await
                    .expect("claim")
                && let Some(claim) = TaskClaim::of(&task)
            {
                let _ = queue::complete_claimed_task(&mut conn, &claim, json!({}))
                    .await
                    .expect("complete");
            }
        }
    }
    last_ages.sort();
    let rows = snapshot(&mut conn, &db_name).await;
    let sum = |keep: &dyn Fn(&str) -> bool, f: fn(&StatRow) -> i64| -> i64 {
        rows.iter().filter(|r| keep(&r.query)).map(f).sum()
    };
    let is_age = |q: &str| q.contains("age_secs");
    let is_depth = |q: &str| q.contains("AS depth");
    let is_claim = |q: &str| q.contains("UPDATE harvest_task_queue") && q.contains("claimed");
    let point = Point {
        pending,
        statements: sum(&|_| true, |r| r.calls),
        buffers: sum(&|_| true, |r| r.buffers),
        age_calls: sum(&is_age, |r| r.calls),
        age_buffers: sum(&is_age, |r| r.buffers),
        depth_buffers: sum(&is_depth, |r| r.buffers),
        claim_buffers: sum(&is_claim, |r| r.buffers),
        wal_bytes: sum(&|_| true, |r| r.wal_bytes),
        ages: last_ages,
    };
    let mut raw = format!(
        "-- {label}: pending={pending}, {TICKS} ticks x {WORKERS} workers, \
         depth + age + claim + complete per worker tick\n"
    );
    for row in &rows {
        let query: String = row.query.chars().take(140).collect();
        let _ = writeln!(
            raw,
            "calls={} buffers={} wal_bytes={} | {query}",
            row.calls, row.buffers, row.wal_bytes
        );
    }
    drop(conn);
    drop(db);
    (point, raw)
}

/// Writes the evidence for one label (`before` or `after`).
#[tokio::test]
#[ignore = "evidence generator, not a CI assertion -- see \
            docs/performance-queue-age-gauge.md"]
async fn zz_capture_queue_age_gauge_evidence() {
    let (admin, _guard) = setup_server().await;
    let label = std::env::var("PERF_LABEL").unwrap_or_else(|_| "after".to_owned());
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .join("docs/perf-artifacts/queue-age-gauge");
    std::fs::create_dir_all(&dir).expect("create artifact dir");
    let mut table = format!(
        "-- {label}: depth + age + claim per worker tick, {TICKS} ticks x {WORKERS} workers\n\
         pending | statements | all buffers | age calls | age buffers | age share | \
         depth buffers | claim buffers | WAL bytes | queues with an age\n"
    );
    let mut raw = String::new();
    for pending in [20_000, 100_000, 300_000] {
        let (p, rows) = measure(&admin, &label, pending).await;
        #[allow(clippy::cast_precision_loss)]
        let share = 100.0 * p.age_buffers as f64 / p.buffers as f64;
        let _ = writeln!(
            table,
            "{} | {} | {} | {} | {} | {share:.1}% | {} | {} | {} | {}",
            p.pending,
            p.statements,
            p.buffers,
            p.age_calls,
            p.age_buffers,
            p.depth_buffers,
            p.claim_buffers,
            p.wal_bytes,
            p.ages.len()
        );
        raw.push_str(&rows);
    }
    println!("{table}");
    std::fs::write(dir.join(format!("{label}-summary.txt")), &table).expect("write summary");
    std::fs::write(dir.join(format!("{label}-pg_stat_statements.txt")), raw)
        .expect("write raw statements");
}

#[derive(diesel::QueryableByName)]
struct PlanLine {
    #[diesel(sql_type = Text, column_name = "QUERY PLAN")]
    line: String,
}

/// Writes `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)` of both gauge queries
/// at each backlog size, under the label in `PERF_LABEL`.
#[tokio::test]
#[ignore = "evidence generator, not a CI assertion -- see \
            docs/performance-queue-age-gauge.md"]
async fn zz_capture_queue_age_gauge_plans() {
    let (admin, _guard) = setup_server().await;
    let label = std::env::var("PERF_LABEL").unwrap_or_else(|_| "after".to_owned());
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .join("docs/perf-artifacts/queue-age-gauge");
    std::fs::create_dir_all(&dir).expect("create artifact dir");
    let queues = format!("{{{}}}", queue_names().join(","));
    let mut out = String::new();
    for pending in [20_000, 100_000, 300_000] {
        let db = ThrowawayDb::create_on(&admin, "queue_age_gauge_plan").await;
        let mut conn = AsyncPgConnection::establish(&db.url())
            .await
            .expect("connection");
        seed(&mut conn, pending).await;
        let prepare = format!(
            "PREPARE age_gauge(text[], text[]) AS {}",
            queue::oldest_pending_ages_query()
        );
        conn.batch_execute(&prepare).await.expect("prepare");
        let _ = writeln!(out, "=== {label}: oldest_pending_ages, pending={pending}");
        let plan: Vec<PlanLine> = diesel::sql_query(format!(
            "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS) \
             EXECUTE age_gauge('{queues}', '{{}}')"
        ))
        .load(&mut conn)
        .await
        .expect("explain");
        for line in plan {
            let _ = writeln!(out, "{}", line.line);
        }
        out.push('\n');
    }
    std::fs::write(dir.join(format!("{label}-explain.txt")), out).expect("write plans");
}

#[derive(diesel::QueryableByName)]
struct AgeRow {
    #[diesel(sql_type = Text)]
    queue_name: String,
    #[diesel(sql_type = Double)]
    age_secs: f64,
}

/// Runs the public function and the reference query in one transaction.
async fn both(conn: &mut AsyncPgConnection, queues: &[String], breakers: &[String]) {
    let queues = queues.to_vec();
    let breakers = breakers.to_vec();
    conn.transaction::<(), diesel::result::Error, _>(async |conn| {
        let mut got = queue::oldest_pending_ages(conn, &queues, &breakers)
            .await
            .expect("ages");
        let mut want: Vec<(String, f64)> = diesel::sql_query(REFERENCE_SQL)
            .bind::<diesel::sql_types::Array<Text>, _>(&queues)
            .bind::<diesel::sql_types::Array<Text>, _>(&breakers)
            .load::<AgeRow>(conn)
            .await?
            .into_iter()
            .map(|r| (r.queue_name, r.age_secs))
            .collect();
        got.sort_by(|a, b| a.0.cmp(&b.0));
        want.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            got, want,
            "public function differs from the reference query"
        );
        Ok(())
    })
    .await
    .expect("transaction");
}

/// The age gauge, edge by edge, against the reference query.
///
/// The small cases are an empty table and a future-scheduled oldest row. They
/// are `created_at` later than `scheduled_at`, a NULL `created_at`, and equal
/// timestamps in one queue. They are a paused queue, a paused activity and a
/// paused execution. They are an expired `schedule_to_close_at`, a full
/// concurrency key, and a rate-limited row with and without a bucket.
///
/// The large cases exercise the seek window of the age query. A queue holds
/// exactly the window, one row more, and many rows. In one queue the oldest rows
/// are all gated, so the answer sits outside the window. In another the oldest
/// row has a `created_at` far past the window. In a third every row is gated.
#[tokio::test]
async fn age_gauge_matches_the_reference_query() {
    let (admin, _guard) = setup_server().await;
    let db = ThrowawayDb::create_on(&admin, "queue_age_gauge_edge").await;
    let mut conn = AsyncPgConnection::establish(&db.url())
        .await
        .expect("connection");
    let queues: Vec<String> = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "empty"]
        .map(String::from)
        .to_vec();
    both(&mut conn, &queues, &[]).await;

    conn.batch_execute(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, shard_id, state, input, queue_name) \
         VALUES ('00000000-0000-0000-0000-0000000000a1', 'w', 'paused', 0, 'PAUSED', '{}', 'f'), \
                ('00000000-0000-0000-0000-0000000000a2', 'w', 'live', 0, 'RUNNING', '{}', 'f'); \
         INSERT INTO harvest_queue_pauses (queue_name, reason) VALUES ('c', 'edge'); \
         INSERT INTO harvest_activity_pauses (activity_name, reason) VALUES ('paused-act', 'edge'); \
         INSERT INTO harvest_task_queue \
             (queue_name, task_type, input, state, scheduled_at, created_at, activity_name, \
              workflow_exec_id, schedule_to_close_at, concurrency_key, concurrency_cap, \
              worker_id, rate_limit_key) \
         VALUES \
           ('a','activity','{}','PENDING', now()-interval '5 min', now()-interval '9 min', 'x', NULL, NULL, NULL, NULL, NULL, NULL), \
           ('a','activity','{}','PENDING', now()-interval '3 min', now()-interval '3 min', 'x', NULL, NULL, NULL, NULL, NULL, NULL), \
           ('b','activity','{}','PENDING', now()+interval '1 hour', now()-interval '1 hour', 'x', NULL, NULL, NULL, NULL, NULL, NULL), \
           ('b','activity','{}','PENDING', now()-interval '2 min', now()-interval '1 min', 'x', NULL, NULL, NULL, NULL, NULL, NULL), \
           ('b','activity','{}','PENDING', now()-interval '4 min', NULL, 'x', NULL, NULL, NULL, NULL, NULL, NULL), \
           ('c','activity','{}','PENDING', now()-interval '5 min', now()-interval '5 min', 'x', NULL, NULL, NULL, NULL, NULL, NULL), \
           ('d','activity','{}','PENDING', now()-interval '6 min', now()-interval '6 min', 'paused-act', NULL, NULL, NULL, NULL, NULL, NULL), \
           ('d','activity','{}','PENDING', now()-interval '2 min', now()-interval '2 min', 'x', NULL, NULL, NULL, NULL, NULL, NULL), \
           ('e','activity','{}','PENDING', now()-interval '7 min', now()-interval '7 min', 'x', NULL, now()-interval '1 min', NULL, NULL, NULL, NULL), \
           ('e','activity','{}','PENDING', now()-interval '1 min', now()-interval '1 min', 'x', NULL, now()+interval '1 min', NULL, NULL, NULL, NULL), \
           ('f','workflow','{}','PENDING', now()-interval '8 min', now()-interval '8 min', NULL, '00000000-0000-0000-0000-0000000000a1', NULL, NULL, NULL, NULL, NULL), \
           ('f','workflow','{}','PENDING', now()-interval '2 min', now()-interval '2 min', NULL, '00000000-0000-0000-0000-0000000000a2', NULL, NULL, NULL, NULL, NULL), \
           ('g','activity','{}','RUNNING', now(), now(), 'x', NULL, NULL, 'k', 1, 'w1', NULL), \
           ('g','activity','{}','PENDING', now()-interval '9 min', now()-interval '9 min', 'x', NULL, NULL, 'k', 1, NULL, NULL), \
           ('g','activity','{}','PENDING', now()-interval '1 min', now()-interval '1 min', 'x', NULL, NULL, NULL, NULL, NULL, NULL), \
           ('h','activity','{}','PENDING', now()-interval '9 min', now()-interval '9 min', 'x', NULL, NULL, NULL, NULL, NULL, 'nobucket'), \
           ('h','activity','{}','PENDING', now()-interval '1 min', now()-interval '1 min', 'x', NULL, NULL, NULL, NULL, NULL, NULL), \
           ('i','activity','{}','PENDING', now()-interval '4 min', now()-interval '4 min', 'x', NULL, NULL, NULL, NULL, NULL, NULL), \
           ('i','activity','{}','PENDING', now()-interval '4 min', now()-interval '4 min', 'x', NULL, NULL, NULL, NULL, NULL, NULL), \
           ('i','activity','{}','COMPLETED', now()-interval '30 min', now()-interval '30 min', 'x', NULL, NULL, NULL, NULL, NULL, NULL);",
    )
    .await
    .expect("edge fixture");
    both(&mut conn, &queues, &[]).await;
    both(&mut conn, &queues, &["x".to_owned()]).await;

    // Large queues. Row `n` of a queue is due `n` minutes ago, oldest first.
    let large: Vec<String> = ["j", "k", "l", "m", "n", "o"].map(String::from).to_vec();
    conn.batch_execute(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, shard_id, state, input, queue_name) \
         VALUES ('00000000-0000-0000-0000-0000000000b1', 'w', 'paused-2', 0, 'PAUSED', '{}', 'j'); \
         -- j: 100 rows, the oldest 40 belong to a paused execution.
         INSERT INTO harvest_task_queue \
             (queue_name, task_type, input, state, scheduled_at, created_at, workflow_exec_id) \
         SELECT 'j', 'workflow', '{}', 'PENDING', now() - (200 - g) * interval '1 minute', \
                now() - (200 - g) * interval '1 minute', \
                CASE WHEN g <= 40 THEN '00000000-0000-0000-0000-0000000000b1'::uuid END \
         FROM generate_series(1, 100) g; \
         -- k: 100 rows, the oldest row was created long after it was due.
         INSERT INTO harvest_task_queue (queue_name, task_type, input, state, scheduled_at, created_at) \
         SELECT 'k', 'activity', '{}', 'PENDING', now() - (200 - g) * interval '1 minute', \
                CASE WHEN g = 1 THEN now() - interval '1 minute' \
                     ELSE now() - (200 - g) * interval '1 minute' END \
         FROM generate_series(1, 100) g; \
         -- l: exactly the window, then one row more in m.
         INSERT INTO harvest_task_queue (queue_name, task_type, input, state, scheduled_at, created_at) \
         SELECT 'l', 'activity', '{}', 'PENDING', now() - (100 - g) * interval '1 minute', \
                now() - (100 - g) * interval '1 minute' \
         FROM generate_series(1, 32) g; \
         INSERT INTO harvest_task_queue (queue_name, task_type, input, state, scheduled_at, created_at) \
         SELECT 'm', 'activity', '{}', 'PENDING', now() - (100 - g) * interval '1 minute', \
                now() - (100 - g) * interval '1 minute' \
         FROM generate_series(1, 33) g; \
         -- n: 100 rows of one paused activity, so nothing is claimable.
         INSERT INTO harvest_task_queue \
             (queue_name, task_type, input, state, scheduled_at, created_at, activity_name) \
         SELECT 'n', 'activity', '{}', 'PENDING', now() - (100 - g) * interval '1 minute', \
                now() - (100 - g) * interval '1 minute', 'paused-act' \
         FROM generate_series(1, 100) g; \
         -- o: 100 rows with one timestamp, so the window cuts through a tie.
         INSERT INTO harvest_task_queue (queue_name, task_type, input, state, scheduled_at, created_at) \
         SELECT 'o', 'activity', '{}', 'PENDING', now() - interval '10 minutes', \
                now() - interval '10 minutes' \
         FROM generate_series(1, 100) g;",
    )
    .await
    .expect("large fixture");
    both(&mut conn, &large, &[]).await;
    let all: Vec<String> = queues.iter().chain(&large).cloned().collect();
    both(&mut conn, &all, &["x".to_owned()]).await;
}
