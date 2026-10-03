//! Claim latency after 1M terminal task rows, before and after the
//! task-queue hygiene fix (issue #1811).
//!
//! The bench runs four arms on one fresh database. Each arm truncates the
//! queue, seeds the headline claim backlog, and measures claims with the
//! shared `claim_bench` harness.
//!
//! | arm | schema | terminal rows |
//! |---|---|---|
//! | `baseline` | after | none |
//! | `before-dead` | before | 1M, dead tuples not yet vacuumed |
//! | `before-vacuumed` | before | 1M, vacuumed |
//! | `after-swept` | after | 1M, deleted by the janitor, then vacuumed |
//!
//! "Before" applies the migration's `down.sql`: the old indexes and default
//! reloptions. "After" applies its `up.sql`. Each terminal row enters as
//! `PENDING` in a bench queue and then moves to `COMPLETED`, as a real row
//! does. That churn leaves dead index entries, which is the cost the issue
//! describes. Autovacuum is off on the table during the run, so each arm's
//! vacuum state is exactly what the arm says.
//!
//! # Running
//!
//! ```text
//! HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo bench -p autumn-harvest --features db --bench task_queue_hygiene_bench
//! ```
//!
//! The URL is an admin connection. The harness creates and migrates a fresh
//! database. Without the URL it starts a Docker Postgres. With neither, it
//! prints a skip notice and exits 0. `HARVEST_BENCH_TERMINAL_ROWS` sets the
//! terminal row count (default 1,000,000).
//!
//! # Scope
//!
//! Each arm is one sample on one host. The numbers show the shape of the cost,
//! not an SLO.

#[path = "../tests/integration/claim_bench_support.rs"]
mod support;

use std::time::Instant;

use diesel::QueryableByName;
use diesel_async::{AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};

use support::db::{self, ClaimReport, SeedOutcome};
use support::{LatencyStats, headline_scenario, scenario_time_budget};

/// Default terminal row count. The issue asks for 1M.
const DEFAULT_TERMINAL_ROWS: u64 = 1_000_000;

/// The janitor's batch size in the `after-swept` arm: the stock default.
const SWEEP_BATCH: usize = 1_000;

/// Rows per seeding statement, so one statement does not hold 1M row locks.
const SEED_CHUNK: u64 = 100_000;

const BEFORE_SQL: &str =
    include_str!("../migrations/20261003201739_harvest_task_queue_hygiene/down.sql");
const AFTER_SQL: &str =
    include_str!("../migrations/20261003201739_harvest_task_queue_hygiene/up.sql");

#[derive(Clone, Copy, PartialEq, Eq)]
enum Schema {
    Before,
    After,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Terminal {
    None,
    Dead,
    Vacuumed,
    Swept,
}

struct Arm {
    name: &'static str,
    schema: Schema,
    terminal: Terminal,
}

const ARMS: [Arm; 4] = [
    Arm {
        name: "baseline",
        schema: Schema::After,
        terminal: Terminal::None,
    },
    Arm {
        name: "before-dead",
        schema: Schema::Before,
        terminal: Terminal::Dead,
    },
    Arm {
        name: "before-vacuumed",
        schema: Schema::Before,
        terminal: Terminal::Vacuumed,
    },
    Arm {
        name: "after-swept",
        schema: Schema::After,
        terminal: Terminal::Swept,
    },
];

/// Table state read just before the claims run.
struct TableState {
    live: i64,
    dead: i64,
    heap_mb: f64,
    index_mb: f64,
}

/// What the janitor did in the `after-swept` arm.
struct SweepRun {
    deleted: u64,
    passes: usize,
    secs: f64,
}

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .expect("tokio runtime");
    rt.block_on(run());
}

fn terminal_rows() -> u64 {
    std::env::var("HARVEST_BENCH_TERMINAL_ROWS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_TERMINAL_ROWS)
}

async fn run() {
    let db = match db::setup_bench_db().await {
        Ok(db) => db,
        Err(reason) => {
            println!(
                "SKIP: task_queue_hygiene_bench needs Postgres: {}",
                reason.0
            );
            return;
        }
    };
    let rows = terminal_rows();
    let scenario = headline_scenario();
    let mut conn = db::connect(&db.url).await;

    println!("# Task-queue hygiene benchmark (issue #1811)");
    println!();
    println!("machine: {}", db::machine_fingerprint());
    println!("postgres: {}", db::server_version(&mut conn).await);
    println!(
        "claim scenario: {} pending / {} claimers / {} queues; terminal rows: {rows}",
        scenario.backlog, scenario.claimers, scenario.queues
    );
    println!();
    println!(
        "| arm | live rows | dead rows | heap MB | index MB | n | p50 ms | p99 ms | max ms | claims/s |"
    );
    println!("|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|");

    let mut sweep = None;
    for arm in &ARMS {
        let seeded = prepare(&mut conn, arm, rows, &mut sweep).await;
        let state = table_state(&mut conn).await;
        let report = db::measure_seeded_claims(&db, scenario, seeded, scenario_time_budget()).await;
        print_row(arm, &state, &report);
    }
    sweep_note(sweep.as_ref());

    exec(
        &mut conn,
        "ALTER TABLE harvest_task_queue RESET (autovacuum_enabled)",
    )
    .await;
}

/// Put the table into the arm's state and return the claim seed outcome.
async fn prepare(
    conn: &mut AsyncPgConnection,
    arm: &Arm,
    rows: u64,
    sweep: &mut Option<SweepRun>,
) -> SeedOutcome {
    let schema_sql = match arm.schema {
        Schema::Before => BEFORE_SQL,
        Schema::After => AFTER_SQL,
    };
    exec(conn, &format!("BEGIN; {schema_sql}; COMMIT;")).await;
    exec(
        conn,
        "ALTER TABLE harvest_task_queue SET (autovacuum_enabled = false)",
    )
    .await;

    // `seed` truncates first, so every arm starts from an empty table.
    let seeded = db::seed(conn, headline_scenario()).await;
    if arm.terminal != Terminal::None {
        seed_terminal_rows(conn, rows).await;
    }
    match arm.terminal {
        Terminal::None | Terminal::Vacuumed => {
            exec(conn, "VACUUM (ANALYZE) harvest_task_queue").await;
        }
        Terminal::Dead => exec(conn, "ANALYZE harvest_task_queue").await,
        Terminal::Swept => {
            *sweep = Some(run_janitor(conn).await);
            exec(conn, "VACUUM (ANALYZE) harvest_task_queue").await;
        }
    }
    seeded
}

/// Insert `rows` rows as `PENDING`, then finish them 30 days ago.
async fn seed_terminal_rows(conn: &mut AsyncPgConnection, rows: u64) {
    let queues = headline_scenario().queues.max(1);
    let mut start = 0;
    while start < rows {
        let end = (start + SEED_CHUNK).min(rows);
        exec(
            conn,
            &format!(
                "INSERT INTO harvest_task_queue \
                   (queue_name, task_type, activity_name, input, state, priority, \
                    max_attempts, scheduled_at) \
                 SELECT '{prefix}-q-' || (i % {queues}), 'activity', 'hygiene_done', \
                        '{{}}'::jsonb, 'PENDING', 0, 3, NOW() - INTERVAL '31 days' \
                 FROM generate_series({start}, {last}) AS s(i)",
                prefix = db::BENCH_PREFIX,
                last = end - 1,
            ),
        )
        .await;
        exec(
            conn,
            "UPDATE harvest_task_queue \
                SET state = 'COMPLETED', worker_id = NULL, \
                    started_at = NOW() - INTERVAL '30 days', \
                    completed_at = NOW() - INTERVAL '30 days' \
              WHERE activity_name = 'hygiene_done' AND state = 'PENDING'",
        )
        .await;
        start = end;
    }
}

/// Run the real janitor sweep until it finds nothing.
async fn run_janitor(conn: &mut AsyncPgConnection) -> SweepRun {
    let cutoff = chrono::Utc::now() - chrono::Duration::days(7);
    let started = Instant::now();
    let mut deleted = 0;
    let mut passes = 0;
    loop {
        let by_state =
            autumn_harvest::queue::sweep_terminal_tasks(conn, cutoff, SWEEP_BATCH, false)
                .await
                .expect("terminal-task sweep");
        passes += 1;
        let n: u64 = by_state.values().sum();
        deleted += n;
        if n == 0 {
            break;
        }
    }
    SweepRun {
        deleted,
        passes,
        secs: started.elapsed().as_secs_f64(),
    }
}

async fn table_state(conn: &mut AsyncPgConnection) -> TableState {
    #[derive(QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        live: i64,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        dead: i64,
        #[diesel(sql_type = diesel::sql_types::Double)]
        heap_mb: f64,
        #[diesel(sql_type = diesel::sql_types::Double)]
        index_mb: f64,
    }
    // Wait for this backend's table counters to reach the stats view.
    let _ = diesel::sql_query("SELECT pg_stat_force_next_flush()")
        .execute(conn)
        .await;
    let row: Row = diesel::sql_query(
        "SELECT \
             (SELECT COUNT(*) FROM harvest_task_queue) AS live, \
             COALESCE((SELECT n_dead_tup FROM pg_stat_user_tables \
                        WHERE relid = 'harvest_task_queue'::regclass), 0) AS dead, \
             (pg_relation_size('harvest_task_queue') / 1048576.0)::float8 AS heap_mb, \
             (pg_indexes_size('harvest_task_queue') / 1048576.0)::float8 AS index_mb",
    )
    .get_result(conn)
    .await
    .expect("table state");
    TableState {
        live: row.live,
        dead: row.dead,
        heap_mb: row.heap_mb,
        index_mb: row.index_mb,
    }
}

fn stats_cells(stats: LatencyStats) -> String {
    if stats.count == 0 {
        return "0 | n/a | n/a | n/a".to_string();
    }
    format!(
        "{} | {:.2} | {:.2} | {:.2}",
        stats.count, stats.p50_ms, stats.p99_ms, stats.max_ms
    )
}

fn print_row(arm: &Arm, state: &TableState, report: &ClaimReport) {
    println!(
        "| {}{} | {} | {} | {:.1} | {:.1} | {} | {:.0} |",
        arm.name,
        if report.truncated { " ⚠" } else { "" },
        state.live,
        state.dead,
        state.heap_mb,
        state.index_mb,
        stats_cells(report.stats),
        report.claims_per_sec(),
    );
}

fn sweep_note(sweep: Option<&SweepRun>) {
    let Some(sweep) = sweep else {
        return;
    };
    println!();
    println!(
        "> Janitor in `after-swept`: deleted {} rows in {} passes ({} rows per batch, \
         up to {} batches per pass) in {:.1} s.",
        sweep.deleted,
        sweep.passes,
        SWEEP_BATCH,
        autumn_harvest::queue::MAX_TERMINAL_TASK_SWEEP_BATCHES_PER_TICK,
        sweep.secs,
    );
    println!("> `⚠` marks an arm cut short by the scenario wall-clock budget.");
}

async fn exec(conn: &mut AsyncPgConnection, sql: &str) {
    conn.batch_execute(sql)
        .await
        .unwrap_or_else(|e| panic!("bench SQL failed: {e}\n--- sql ---\n{sql}"));
}
