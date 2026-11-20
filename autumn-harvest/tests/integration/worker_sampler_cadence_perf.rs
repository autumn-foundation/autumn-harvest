#![cfg(feature = "db")]
//! Ledger investigation: the poll-interval gauge samplers (follow-up to #2668).
//!
//! Issue #2668 moved the two population samplers to their own interval.
//! Before this change, six more gauge samplers ran on `poll_interval`, on
//! every worker:
//!
//! | sampler | statement |
//! |:--|:--|
//! | `spawn_queue_depth_sampler` | `queue::bounded_queue_depths`, `queue::oldest_pending_ages` |
//! | `spawn_concurrency_sampler` | `queue::concurrency_key_stats` |
//! | `spawn_rate_limit_sampler` | `queue::sample_rate_limit_buckets` |
//! | `spawn_dlq_depth_sampler` | `dlq::dead_letter_count` |
//! | `spawn_queue_pause_sampler` | `queue_pause::paused_queue_names` |
//! | `spawn_stranded_work_sampler` | `queue::claimable_pending_demand_by_queue` |
//!
//! The stranded-work sampler runs only with a sharded pool.
//!
//! The harness seeds a fixture at three sizes. It then replays poll ticks of
//! two workers. One tick runs the seven sampler statements and one claim with
//! its completion, as a busy worker with a metrics recorder does. The harness
//! reads `pg_stat_statements`. Evidence is calls and buffers, not wall-clock.
//!
//! The fixture reuses the backlog shape of `queue_age_gauge_perf`. It adds
//! dead letters with production-sized payloads, and rate-limit buckets of
//! which most are per-tenant keys. The sampler filters those keys out, but the
//! scan still reads them.
//!
//! Before its fix, one stranded-work call took minutes at 100,000 pending rows.
//! Set `PERF_SKIP_STRANDED=1` to leave that statement out of a run.
//!
//! `zz_capture_dead_letter_write_costs` measures the other side of the DLQ
//! depth counter: the cost of each dead-letter insert and discard. It seeds
//! only the dead letters of each size.
//!
//! The capture is `#[ignore]`d. It needs a superuser and a preloaded
//! `pg_stat_statements`. `HARVEST_TEST_DATABASE_URL` names an admin URL.
//! Without it the harness starts a Postgres container. Output goes to
//! `docs/perf-artifacts/worker-sampler-cadence/`, under the label in
//! `PERF_LABEL`.

#![allow(clippy::too_many_lines, clippy::cast_precision_loss)]

use std::fmt::Write as _;

use autumn_harvest::queue::{self, TaskClaim};
use diesel::sql_types::{BigInt, Text};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::json;

use crate::queue_age_gauge_perf::setup_server;
use crate::throwaway_db::ThrowawayDb;

/// Queues that the workers poll.
const QUEUES: i64 = 20;

/// Workers that replay ticks.
const WORKERS: usize = 2;

/// Poll ticks per worker.
const TICKS: usize = 20;

/// One fixture size.
struct Size {
    name: &'static str,
    pending: i64,
    dead_letters: i64,
    buckets: i64,
}

/// The fixture sizes. Each size gets a fresh database.
const SIZES: [Size; 3] = [
    Size {
        name: "small",
        pending: 10_000,
        dead_letters: 10_000,
        buckets: 100,
    },
    Size {
        name: "medium",
        pending: 100_000,
        dead_letters: 100_000,
        buckets: 1_000,
    },
    Size {
        name: "large",
        pending: 300_000,
        dead_letters: 1_000_000,
        buckets: 10_000,
    },
];

/// One sampler statement, matched by a fragment of its text.
struct Kind {
    label: &'static str,
    fragment: &'static str,
}

/// The seven sampler statements. The claim and every other statement fall
/// into the workload.
const KINDS: [Kind; 7] = [
    Kind {
        label: "depth (bounded_queue_depths)",
        fragment: "AS depth",
    },
    Kind {
        label: "age (oldest_pending_ages)",
        fragment: "age_secs",
    },
    Kind {
        label: "concurrency (concurrency_key_stats)",
        fragment: "AS in_flight",
    },
    Kind {
        label: "rate limit (sample_rate_limit_buckets)",
        fragment: "AS estimated_tokens",
    },
    Kind {
        label: "dlq (dead_letter_count)",
        fragment: "harvest_dead_letter_depth",
    },
    Kind {
        label: "queue pause (paused_queue_names)",
        fragment: "FROM harvest_queue_pauses ORDER BY",
    },
    Kind {
        label: "stranded work (claimable_pending_demand_by_queue)",
        fragment: "AS cnt",
    },
];

fn artifact_dir() -> std::path::PathBuf {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .join("docs/perf-artifacts/worker-sampler-cadence");
    std::fs::create_dir_all(&dir).expect("create artifact dir");
    dir
}

fn queue_names() -> Vec<String> {
    (0..QUEUES).map(|i| format!("q{i}")).collect()
}

/// Inserts `dead_letters` rows with production-sized payloads.
fn dead_letter_seed_sql(dead_letters: i64) -> String {
    format!(
        "INSERT INTO harvest_dead_letters \
             (id, original_task_id, queue_name, task_type, activity_name, input, error, \
              attempts, failed_at, workflow_name) \
         SELECT gen_random_uuid(), gen_random_uuid(), \
                'q' || floor({QUEUES} * power(random(), 3))::int, 'activity', \
                'act' || (g % 60), \
                jsonb_build_object('order', g, 'pad', repeat('x', 100 + (random() * 300)::int)), \
                'activity failed: ' || repeat('e', 200 + (random() * 600)::int), \
                5, now() - (random() * interval '30 days'), 'wf' || (g % 30) \
         FROM generate_series(1, {dead_letters}) g;"
    )
}

/// Seeds the backlog of `queue_age_gauge_perf`, the dead letters and the
/// rate-limit buckets.
async fn seed(conn: &mut AsyncPgConnection, size: &Size) {
    let Size {
        pending,
        dead_letters,
        buckets,
        ..
    } = size;
    let dead_letter_sql = dead_letter_seed_sql(*dead_letters);
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
         CREATE UNIQUE INDEX ON ex (rn); \
         ANALYZE ex; \
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
         UPDATE harvest_task_queue SET created_at = scheduled_at WHERE state = 'PENDING'; \
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
         DROP TABLE ex; \
         {dead_letter_sql} \
         INSERT INTO harvest_rate_limit_buckets \
             (key, refill_rate, burst, tokens, last_refilled_at, created_at, updated_at) \
         SELECT CASE WHEN g % 10 = 0 THEN 'rl' || g ELSE 'dyn-rate:tenant-' || g END, \
                10, 20, random() * 20, now() - (random() * interval '10 seconds'), now(), now() \
         FROM generate_series(1, {buckets}) g;"
    );
    conn.batch_execute(&sql).await.expect("seed the fixture");
    // A steady-state table has a current visibility map and fresh statistics.
    for table in [
        "harvest_task_queue",
        "harvest_workflow_executions",
        "harvest_dead_letters",
        "harvest_rate_limit_buckets",
    ] {
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
}

async fn snapshot(conn: &mut AsyncPgConnection, db_name: &str) -> Vec<StatRow> {
    diesel::sql_query(format!(
        "SELECT query, calls, (shared_blks_hit + shared_blks_read) AS buffers \
         FROM pg_stat_statements \
         WHERE dbid = (SELECT oid FROM pg_database WHERE datname = '{db_name}') \
           AND query NOT ILIKE '%pg_stat_statements%' \
           AND query NOT ILIKE 'EXPLAIN%' \
         ORDER BY buffers DESC, query"
    ))
    .load(conn)
    .await
    .expect("pg_stat_statements must be preloaded")
}

/// Calls and buffers of one statement kind, or of the workload.
#[derive(Default, Clone, Copy)]
struct Cost {
    calls: i64,
    buffers: i64,
}

impl Cost {
    fn per_call(self) -> f64 {
        if self.calls == 0 {
            0.0
        } else {
            self.buffers as f64 / self.calls as f64
        }
    }
}

/// The result of one size.
struct Point {
    kinds: Vec<Cost>,
    workload: Cost,
    total_buffers: i64,
}

/// One worker tick: the seven sampler statements, then one claim and its
/// completion.
async fn tick(conn: &mut AsyncPgConnection, queues: &[String], worker_id: &str) {
    queue::bounded_queue_depths(conn, queues, autumn_harvest::telemetry::QUEUE_DEPTH_CAP)
        .await
        .expect("depths");
    queue::oldest_pending_ages(conn, queues, &[])
        .await
        .expect("ages");
    queue::concurrency_key_stats(conn, queues)
        .await
        .expect("concurrency stats");
    queue::sample_rate_limit_buckets(conn)
        .await
        .expect("rate limit buckets");
    autumn_harvest::dlq::dead_letter_count(conn)
        .await
        .expect("dead letter count");
    autumn_harvest::queue_pause::paused_queue_names(conn)
        .await
        .expect("paused queues");
    if std::env::var_os("PERF_SKIP_STRANDED").is_none() {
        queue::claimable_pending_demand_by_queue(conn, &[])
            .await
            .expect("stranded demand");
    }
    if let Some(task) = queue::claim_task(conn, queues, worker_id, "", None, &[], &[])
        .await
        .expect("claim")
        && let Some(claim) = TaskClaim::of(&task)
    {
        let _ = queue::complete_claimed_task(conn, &claim, json!({}))
            .await
            .expect("complete");
    }
}

/// `EXPLAIN (ANALYZE, BUFFERS)` of the three statements that the earlier
/// pages did not measure.
async fn explain(conn: &mut AsyncPgConnection, queues: &[String]) -> String {
    #[derive(diesel::QueryableByName)]
    struct Line {
        #[diesel(sql_type = Text, column_name = "QUERY PLAN")]
        line: String,
    }
    let list = queues
        .iter()
        .map(|q| format!("'{q}'"))
        .collect::<Vec<_>>()
        .join(",");
    let statements = [
        (
            "concurrency_key_stats",
            format!(
                "SELECT concurrency_key AS key, task_type, MAX(concurrency_cap)::INT4, \
                 COUNT(*) FILTER (WHERE state = 'RUNNING' AND worker_id IS NOT NULL), \
                 COUNT(*) FILTER (WHERE state = 'PENDING') FROM harvest_task_queue \
                 WHERE concurrency_key IS NOT NULL AND concurrency_cap IS NOT NULL \
                 AND queue_name = ANY(ARRAY[{list}]) AND (state = 'PENDING' OR \
                 (state = 'RUNNING' AND worker_id IS NOT NULL)) \
                 GROUP BY concurrency_key, task_type"
            ),
        ),
        (
            "sample_rate_limit_buckets",
            "SELECT key, refill_rate, tokens FROM harvest_rate_limit_buckets \
             WHERE key NOT LIKE 'dyn-rate:%' AND key NOT LIKE 'start-throttle:%'"
                .to_owned(),
        ),
        (
            "dead_letter_count",
            autumn_harvest::dlq::DLQ_DEPTH_SQL.to_owned(),
        ),
    ];
    let mut out = String::new();
    for (name, sql) in statements {
        let lines: Vec<Line> = diesel::sql_query(format!("EXPLAIN (ANALYZE, BUFFERS) {sql}"))
            .load(conn)
            .await
            .expect("explain");
        let _ = writeln!(out, "-- {name}");
        for line in lines {
            let _ = writeln!(out, "{}", line.line);
        }
        out.push('\n');
    }
    out
}

async fn measure(admin: &str, size: &Size) -> (Point, String, String) {
    let db = ThrowawayDb::create_on(admin, "worker_sampler_cadence").await;
    let db_name = db.name().to_owned();
    let mut conn = AsyncPgConnection::establish(&db.url())
        .await
        .expect("connection");
    conn.batch_execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .await
        .expect("pg_stat_statements extension");
    seed(&mut conn, size).await;
    let queues = queue_names();
    // One untimed tick warms the plan cache and the buffer cache.
    tick(&mut conn, &queues, "warm").await;
    diesel::sql_query(format!(
        "SELECT pg_stat_statements_reset(0, \
                (SELECT oid FROM pg_database WHERE datname = '{db_name}'), 0)"
    ))
    .execute(&mut conn)
    .await
    .expect("pg_stat_statements_reset needs a superuser");

    for t in 0..TICKS {
        for w in 0..WORKERS {
            tick(&mut conn, &queues, &format!("perf-w{w}-t{t}")).await;
        }
    }
    let rows = snapshot(&mut conn, &db_name).await;
    let plans = explain(&mut conn, &queues).await;

    let mut kinds = vec![Cost::default(); KINDS.len()];
    let mut workload = Cost::default();
    for row in &rows {
        let slot = KINDS
            .iter()
            .position(|k| row.query.contains(k.fragment))
            .map_or(&mut workload, |i| &mut kinds[i]);
        slot.calls += row.calls;
        slot.buffers += row.buffers;
    }
    let total_buffers = rows.iter().map(|r| r.buffers).sum();
    let mut raw = format!(
        "-- {}: pending={}, dead_letters={}, buckets={}, {TICKS} ticks x {WORKERS} workers\n",
        size.name, size.pending, size.dead_letters, size.buckets
    );
    for row in &rows {
        let query: String = row.query.chars().take(140).collect();
        let _ = writeln!(raw, "calls={} buffers={} | {query}", row.calls, row.buffers);
    }
    drop(conn);
    drop(db);
    (
        Point {
            kinds,
            workload,
            total_buffers,
        },
        raw,
        plans,
    )
}

/// Writes the per-call evidence for every size.
#[tokio::test]
#[ignore = "evidence generator, not a CI assertion -- see \
            docs/performance-gauge-sample-interval.md"]
async fn zz_capture_worker_sampler_statement_costs() {
    let (admin, _guard) = setup_server().await;
    let label = std::env::var("PERF_LABEL").unwrap_or_else(|_| "baseline".to_owned());
    let dir = artifact_dir();
    let mut table = format!(
        "-- {label}: buffers per call, {TICKS} ticks x {WORKERS} workers, one claim per tick\n\
         | size | statement | calls | buffers | buffers per call | share of buffers |\n\
         |:--|:--|--:|--:|--:|--:|\n"
    );
    for size in &SIZES {
        let (point, raw, plans) = measure(&admin, size).await;
        let share = |b: i64| 100.0 * b as f64 / point.total_buffers.max(1) as f64;
        for (kind, cost) in KINDS.iter().zip(&point.kinds) {
            let _ = writeln!(
                table,
                "| {} | {} | {} | {} | {:.0} | {:.1}% |",
                size.name,
                kind.label,
                cost.calls,
                cost.buffers,
                cost.per_call(),
                share(cost.buffers)
            );
        }
        let samplers: i64 = point.kinds.iter().map(|c| c.buffers).sum();
        let _ = writeln!(
            table,
            "| {} | workload (claim, complete, other) | {} | {} | {:.0} | {:.1}% |",
            size.name,
            point.workload.calls,
            point.workload.buffers,
            point.workload.per_call(),
            share(point.workload.buffers)
        );
        let _ = writeln!(
            table,
            "| {} | **all seven samplers** | | {samplers} | | **{:.1}%** |",
            size.name,
            share(samplers)
        );
        std::fs::write(
            dir.join(format!("{label}-{}-pg_stat_statements.txt", size.name)),
            raw,
        )
        .expect("write raw");
        std::fs::write(
            dir.join(format!("{label}-{}-explain.txt", size.name)),
            plans,
        )
        .expect("write plans");
        eprintln!("{table}");
    }
    std::fs::write(dir.join(format!("{label}-statement-costs.md")), &table).expect("write table");
    eprintln!("{table}");
}

/// Dead letters that one write phase inserts.
const WRITES: usize = 200;

/// Rows in one batch of the batch-discard phase.
const DISCARD_BATCH: usize = 100;

/// Connections of the concurrent-insert phase.
const WRITERS: usize = 8;

/// Inserts of each connection in the concurrent-insert phase.
const WRITES_PER_WRITER: usize = 50;

#[derive(diesel::QueryableByName)]
struct TimedStatRow {
    #[diesel(sql_type = Text)]
    query: String,
    #[diesel(sql_type = BigInt)]
    calls: i64,
    #[diesel(sql_type = BigInt)]
    buffers: i64,
    #[diesel(sql_type = diesel::sql_types::Double)]
    total_ms: f64,
}

/// Calls, buffers and time of the statements that contain `fragment`.
async fn timed_cost(
    conn: &mut AsyncPgConnection,
    db_name: &str,
    fragment: &str,
) -> (i64, i64, f64) {
    let rows: Vec<TimedStatRow> = diesel::sql_query(format!(
        "SELECT query, calls, (shared_blks_hit + shared_blks_read) AS buffers, \
                total_exec_time AS total_ms \
         FROM pg_stat_statements \
         WHERE dbid = (SELECT oid FROM pg_database WHERE datname = '{db_name}')"
    ))
    .load(conn)
    .await
    .expect("pg_stat_statements must be preloaded");
    rows.iter()
        .filter(|r| r.query.contains(fragment))
        .fold((0, 0, 0.0), |(c, b, t), r| {
            (c + r.calls, b + r.buffers, t + r.total_ms)
        })
}

async fn reset_stats(conn: &mut AsyncPgConnection, db_name: &str) {
    diesel::sql_query(format!(
        "SELECT pg_stat_statements_reset(0, \
                (SELECT oid FROM pg_database WHERE datname = '{db_name}'), 0)"
    ))
    .execute(conn)
    .await
    .expect("pg_stat_statements_reset needs a superuser");
}

fn write_entry(n: usize) -> autumn_harvest::dlq::NewDeadLetterEntry {
    autumn_harvest::dlq::NewDeadLetterEntry {
        original_task_id: uuid::Uuid::new_v4(),
        queue_name: "q0".to_owned(),
        task_type: "activity".to_owned(),
        workflow_exec_id: None,
        activity_name: Some("act0".to_owned()),
        input: json!({ "order": n, "pad": "x".repeat(300) }),
        error: format!("activity failed: {}", "e".repeat(400)),
        attempts: 5,
        owner: None,
        severity: None,
    }
}

/// One row of the write-cost table.
fn write_row(table: &mut String, size: &str, label: &str, (calls, buffers, ms): (i64, i64, f64)) {
    let per = |v: f64| if calls == 0 { 0.0 } else { v / calls as f64 };
    let _ = writeln!(
        table,
        "| {size} | {label} | {calls} | {buffers} | {:.1} | {:.3} |",
        per(buffers as f64),
        per(ms)
    );
}

/// Measures the dead-letter writes on one size: single inserts, single
/// discards, batch discards and concurrent inserts.
async fn measure_writes(admin: &str, size: &Size, table: &mut String) {
    let db = ThrowawayDb::create_on(admin, "dlq_write_cost").await;
    let db_name = db.name().to_owned();
    let mut conn = AsyncPgConnection::establish(&db.url())
        .await
        .expect("connection");
    if without_counter() {
        conn.batch_execute(&format!(
            "BEGIN;\n{}\n;\nCOMMIT;",
            include_str!("../../migrations/20261119233037_harvest_dead_letter_counts/down.sql")
        ))
        .await
        .expect("drop the counter");
    }
    conn.batch_execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .await
        .expect("pg_stat_statements extension");
    // Nested statements count too, so the counter update of a trigger shows
    // as its own row. A top-level row still includes its nested buffers.
    conn.batch_execute("SET pg_stat_statements.track = 'all'")
        .await
        .expect("track nested statements");
    conn.batch_execute(&format!(
        "SELECT setseed(0.42); {}",
        dead_letter_seed_sql(size.dead_letters)
    ))
    .await
    .expect("seed the dead letters");
    conn.batch_execute("VACUUM ANALYZE harvest_dead_letters")
        .await
        .expect("vacuum");
    // One untimed insert and discard warm the plan cache.
    let warm = autumn_harvest::dlq::dead_letter(&mut conn, &write_entry(0))
        .await
        .expect("warm insert");
    autumn_harvest::dlq::discard_dead_letters_batch(&mut conn, &[warm])
        .await
        .expect("warm discard");

    let insert = "INSERT INTO \"harvest_dead_letters\"";
    let delete = "DELETE FROM \"harvest_dead_letters\"";
    // The trigger body: its delta insert and, now and then, a fold.
    let counter = "harvest_dead_letter_counts";

    reset_stats(&mut conn, &db_name).await;
    let mut ids = Vec::with_capacity(WRITES);
    for n in 0..WRITES {
        ids.push(
            autumn_harvest::dlq::dead_letter(&mut conn, &write_entry(n))
                .await
                .expect("insert"),
        );
    }
    let cost = timed_cost(&mut conn, &db_name, insert).await;
    write_row(table, size.name, "insert, one row", cost);
    let cost = timed_cost(&mut conn, &db_name, counter).await;
    write_row(
        table,
        size.name,
        "insert, one row: nested counter work",
        cost,
    );

    reset_stats(&mut conn, &db_name).await;
    for id in &ids {
        autumn_harvest::dlq::discard_dead_letters_batch(&mut conn, &[*id])
            .await
            .expect("discard");
    }
    let cost = timed_cost(&mut conn, &db_name, delete).await;
    write_row(table, size.name, "discard, one row", cost);
    let cost = timed_cost(&mut conn, &db_name, counter).await;
    write_row(
        table,
        size.name,
        "discard, one row: nested counter work",
        cost,
    );

    let mut ids = Vec::with_capacity(WRITES * 2);
    for n in 0..WRITES * 2 {
        ids.push(
            autumn_harvest::dlq::dead_letter(&mut conn, &write_entry(n))
                .await
                .expect("insert"),
        );
    }
    reset_stats(&mut conn, &db_name).await;
    for batch in ids.chunks(DISCARD_BATCH) {
        autumn_harvest::dlq::discard_dead_letters_batch(&mut conn, batch)
            .await
            .expect("batch discard");
    }
    let cost = timed_cost(&mut conn, &db_name, delete).await;
    write_row(table, size.name, "discard, 100 rows", cost);
    let cost = timed_cost(&mut conn, &db_name, counter).await;
    write_row(
        table,
        size.name,
        "discard, 100 rows: nested counter work",
        cost,
    );

    measure_held_snapshot(&mut conn, &db, size, table).await;

    reset_stats(&mut conn, &db_name).await;
    let url = db.url();
    let writers: Vec<_> = (0..WRITERS)
        .map(|w| {
            let url = url.clone();
            tokio::spawn(async move {
                let mut conn = AsyncPgConnection::establish(&url)
                    .await
                    .expect("writer connection");
                conn.batch_execute("SET pg_stat_statements.track = 'all'")
                    .await
                    .expect("track nested statements");
                for n in 0..WRITES_PER_WRITER {
                    autumn_harvest::dlq::dead_letter(&mut conn, &write_entry(w * 1000 + n))
                        .await
                        .expect("concurrent insert");
                }
            })
        })
        .collect();
    for writer in writers {
        writer.await.expect("writer task");
    }
    let label = format!("insert, {WRITERS} connections at once");
    let cost = timed_cost(&mut conn, &db_name, insert).await;
    write_row(table, size.name, &label, cost);
    let cost = timed_cost(&mut conn, &db_name, counter).await;
    write_row(
        table,
        size.name,
        &format!("{label}: nested counter work"),
        cost,
    );
    if !without_counter() {
        let _ = writeln!(
            table,
            "| {} | counter rows after the concurrent inserts: {} | | | | |",
            size.name,
            counter_rows(&mut conn).await
        );
    }
    drop(conn);
    drop(db);
}

/// Dead letters that the held-snapshot phase inserts.
const HELD_WRITES: usize = 2_000;

/// `PERF_WITHOUT_COUNTER=1` drops the counter after the migration. A run with
/// it measures the writes as they were before the counter.
fn without_counter() -> bool {
    std::env::var_os("PERF_WITHOUT_COUNTER").is_some()
}

async fn counter_rows(conn: &mut AsyncPgConnection) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Rows {
        #[diesel(sql_type = BigInt)]
        n: i64,
    }
    diesel::sql_query("SELECT COUNT(*)::BIGINT AS n FROM harvest_dead_letter_counts")
        .get_result::<Rows>(conn)
        .await
        .expect("counter rows")
        .n
}

/// The writes and the gauge read while another session holds a snapshot open.
/// Vacuum then keeps each dead row version, as during a long `pg_dump`.
async fn measure_held_snapshot(
    conn: &mut AsyncPgConnection,
    db: &ThrowawayDb,
    size: &Size,
    table: &mut String,
) {
    let db_name = db.name();
    let mut holder = AsyncPgConnection::establish(&db.url())
        .await
        .expect("holder connection");
    holder
        .batch_execute(
            "BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT 1 FROM harvest_dead_letters LIMIT 1;",
        )
        .await
        .expect("hold a snapshot");
    reset_stats(conn, db_name).await;
    for n in 0..HELD_WRITES {
        autumn_harvest::dlq::dead_letter(conn, &write_entry(n))
            .await
            .expect("insert under a held snapshot");
    }
    let label = format!("insert, one row, snapshot held ({HELD_WRITES} writes)");
    let cost = timed_cost(conn, db_name, "INSERT INTO \"harvest_dead_letters\"").await;
    write_row(table, size.name, &label, cost);

    let gauge = if without_counter() {
        "SELECT COUNT(*)::BIGINT AS entries FROM harvest_dead_letters"
    } else {
        autumn_harvest::dlq::DLQ_DEPTH_SQL
    };
    reset_stats(conn, db_name).await;
    for _ in 0..5 {
        diesel::sql_query(gauge)
            .execute(conn)
            .await
            .expect("gauge read");
    }
    let fragment = if without_counter() {
        "FROM harvest_dead_letters"
    } else {
        "harvest_dead_letter_depth"
    };
    let cost = timed_cost(conn, db_name, fragment).await;
    write_row(
        table,
        size.name,
        &format!("gauge read after {HELD_WRITES} writes, snapshot held"),
        cost,
    );
    if !without_counter() {
        #[derive(diesel::QueryableByName)]
        struct Pages {
            #[diesel(sql_type = BigInt)]
            n: i64,
        }
        let pages: Pages = diesel::sql_query(
            "SELECT (pg_relation_size('harvest_dead_letter_counts') / 8192)::BIGINT AS n",
        )
        .get_result(conn)
        .await
        .expect("counter pages");
        let _ = writeln!(
            table,
            "| {} | counter after {HELD_WRITES} writes, snapshot held: {} rows, {} pages | | | | |",
            size.name,
            counter_rows(conn).await,
            pages.n
        );
    }
    holder
        .batch_execute("COMMIT")
        .await
        .expect("release the snapshot");
}

/// Writes the per-call cost of the dead-letter writes for every size.
#[tokio::test]
#[ignore = "evidence generator, not a CI assertion -- see \
            docs/performance-dlq-depth-counter.md"]
async fn zz_capture_dead_letter_write_costs() {
    let (admin, _guard) = setup_server().await;
    let label = std::env::var("PERF_LABEL").unwrap_or_else(|_| "write-baseline".to_owned());
    let mut table = format!(
        "-- {label}: dead-letter writes, {WRITES} single writes, \
         {DISCARD_BATCH}-row batches, {HELD_WRITES} writes under a held snapshot, \
         {WRITERS} x {WRITES_PER_WRITER} concurrent inserts\n\
         | size | statement | calls | buffers | buffers per call | ms per call |\n\
         |:--|:--|--:|--:|--:|--:|\n"
    );
    for size in &SIZES {
        measure_writes(&admin, size, &mut table).await;
        eprintln!("{table}");
    }
    std::fs::write(
        artifact_dir().join(format!("{label}-write-costs.md")),
        &table,
    )
    .expect("write table");
}

/// The stranded-work demand query as it stood before its `PAUSED` gate became a
/// `NOT EXISTS` probe. It is the equivalence oracle.
const REFERENCE_DEMAND_SQL: &str = "SELECT tq.queue_name, tq.required_capabilities::text AS caps, \
     tq.required_build_id AS build_id, \
     CASE WHEN tq.sticky_worker_id IS NOT NULL AND tq.sticky_until IS NOT NULL \
          AND tq.sticky_until > NOW() THEN tq.sticky_worker_id ELSE NULL END AS sticky_owner, \
     tq.activity_name, COUNT(*)::BIGINT AS cnt \
     FROM harvest_task_queue tq \
     LEFT JOIN harvest_workflow_executions e ON e.id = tq.workflow_exec_id \
     WHERE tq.state = 'PENDING' AND tq.scheduled_at <= NOW() \
       AND NOT EXISTS (SELECT 1 FROM harvest_queue_pauses qp WHERE qp.queue_name = tq.queue_name) \
       AND NOT EXISTS (SELECT 1 FROM harvest_activity_pauses ap \
                       WHERE ap.activity_name = tq.activity_name AND tq.task_type = 'activity') \
       AND (tq.schedule_to_close_at IS NULL OR tq.schedule_to_close_at > NOW()) \
       AND (tq.task_type <> 'workflow' OR tq.workflow_exec_id IS NULL OR e.id IS NULL \
            OR e.state <> 'PAUSED') \
       AND (tq.concurrency_key IS NULL OR tq.concurrency_cap IS NULL OR \
            (SELECT COUNT(*) FROM harvest_task_queue inner_q \
             WHERE inner_q.concurrency_key = tq.concurrency_key \
               AND inner_q.task_type = tq.task_type AND inner_q.state = 'RUNNING' \
               AND inner_q.worker_id IS NOT NULL) < tq.concurrency_cap) \
       AND (tq.rate_limit_key IS NULL OR tq.activity_name = ANY($1) OR EXISTS \
            (SELECT 1 FROM harvest_rate_limit_buckets b WHERE b.key = tq.rate_limit_key \
             AND b.tokens >= 1.0)) \
     GROUP BY tq.queue_name, tq.required_capabilities, tq.required_build_id, \
              CASE WHEN tq.sticky_worker_id IS NOT NULL AND tq.sticky_until IS NOT NULL \
                   AND tq.sticky_until > NOW() THEN tq.sticky_worker_id ELSE NULL END, \
              tq.activity_name";

#[derive(diesel::QueryableByName, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct DemandRow {
    #[diesel(sql_type = Text)]
    queue_name: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
    sticky_owner: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
    activity_name: Option<String>,
    #[diesel(sql_type = BigInt)]
    cnt: i64,
}

/// The new demand query returns the same groups as the reference on the edge
/// cases of its `PAUSED` gate. The buckets hold no override, so the plain
/// token test of the reference matches the effective one.
#[tokio::test]
async fn stranded_demand_matches_the_reference_query() {
    let (admin, _guard) = setup_server().await;
    let db = ThrowawayDb::create_on(&admin, "stranded_demand_edge").await;
    let mut conn = AsyncPgConnection::establish(&db.url())
        .await
        .expect("connection");
    conn.batch_execute(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, shard_id, state, input, queue_name) VALUES \
         ('00000000-0000-0000-0000-000000000001', 'wf', 'running', 0, 'RUNNING', '{}', 'q0'), \
         ('00000000-0000-0000-0000-000000000002', 'wf', 'paused', 0, 'PAUSED', '{}', 'q0'), \
         ('00000000-0000-0000-0000-000000000003', 'wf', 'done', 0, 'COMPLETED', '{}', 'q0'); \
         INSERT INTO harvest_rate_limit_buckets \
             (key, refill_rate, burst, tokens, last_refilled_at, created_at, updated_at) VALUES \
         ('empty', 0, 1, 0, now(), now(), now()), ('full', 0, 5, 5, now(), now(), now()); \
         INSERT INTO harvest_task_queue \
             (queue_name, task_type, input, state, scheduled_at, workflow_exec_id, activity_name, \
              concurrency_key, concurrency_cap, rate_limit_key, worker_id, sticky_worker_id, \
              sticky_until) VALUES \
         ('q0', 'workflow', '{}', 'PENDING', now() - interval '1 minute', \
              '00000000-0000-0000-0000-000000000001', NULL, NULL, NULL, NULL, NULL, NULL, NULL), \
         ('q0', 'workflow', '{}', 'PENDING', now() - interval '1 minute', \
              '00000000-0000-0000-0000-000000000002', NULL, NULL, NULL, NULL, NULL, NULL, NULL), \
         ('q0', 'workflow', '{}', 'PENDING', now() - interval '1 minute', \
              '00000000-0000-0000-0000-000000000003', NULL, NULL, NULL, NULL, NULL, NULL, NULL), \
         ('q0', 'workflow', '{}', 'PENDING', now() - interval '1 minute', \
              NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL), \
         ('q1', 'activity', '{}', 'PENDING', now() - interval '1 minute', \
              '00000000-0000-0000-0000-000000000002', 'act', NULL, NULL, NULL, NULL, NULL, NULL), \
         ('q1', 'activity', '{}', 'PENDING', now() - interval '1 minute', \
              NULL, 'capped', 'ck', 1, NULL, NULL, NULL, NULL), \
         ('q1', 'activity', '{}', 'RUNNING', now() - interval '1 minute', \
              NULL, 'capped', 'ck', 1, NULL, 'w1', NULL, NULL), \
         ('q1', 'activity', '{}', 'PENDING', now() - interval '1 minute', \
              NULL, 'limited', NULL, NULL, 'empty', NULL, NULL, NULL), \
         ('q1', 'activity', '{}', 'PENDING', now() - interval '1 minute', \
              NULL, 'limited', NULL, NULL, 'full', NULL, NULL, NULL), \
         ('q2', 'activity', '{}', 'PENDING', now() - interval '1 minute', \
              NULL, 'sticky', NULL, NULL, NULL, NULL, 'w2', now() + interval '1 hour'), \
         ('q2', 'activity', '{}', 'PENDING', now() + interval '1 hour', \
              NULL, 'future', NULL, NULL, NULL, NULL, NULL, NULL);",
    )
    .await
    .expect("seed the edge cases");

    let mut reference: Vec<DemandRow> = diesel::sql_query(REFERENCE_DEMAND_SQL)
        .bind::<diesel::sql_types::Array<Text>, _>(Vec::<String>::new())
        .load(&mut conn)
        .await
        .expect("reference query");
    let mut actual: Vec<DemandRow> = queue::claimable_pending_demand_by_queue(&mut conn, &[])
        .await
        .expect("demand query")
        .into_iter()
        .map(|d| DemandRow {
            queue_name: d.queue_name,
            sticky_owner: d.sticky_owner,
            activity_name: d.activity_name,
            cnt: d.count,
        })
        .collect();
    reference.sort();
    actual.sort();
    // Three q0 workflow rows count: RUNNING, COMPLETED and no execution. The
    // PAUSED one does not. A foreign key rules out a missing execution. The
    // q1 activity of the PAUSED run counts, as does the bucket with tokens.
    assert_eq!(actual, reference);
    let q0: i64 = actual
        .iter()
        .filter(|r| r.queue_name == "q0")
        .map(|r| r.cnt)
        .sum();
    assert_eq!(q0, 3, "only the PAUSED workflow row is held: {actual:?}");
    drop(conn);
    drop(db);
}
