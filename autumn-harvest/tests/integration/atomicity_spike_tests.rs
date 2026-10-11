#![cfg(feature = "atomicity-spike")]
//! Database tests for the atomicity spike (issue #2012).
//!
//! Set `HARVEST_TEST_DATABASE_URL` to use a running Postgres. Otherwise each
//! test boots one testcontainers Postgres. Each test creates its own tables
//! and drops them.
//!
//! `measure_the_full_matrix` is `#[ignore]`. It runs the pre-registered
//! matrix for about 6 minutes and prints the report tables:
//!
//! ```text
//! cargo test --release -p autumn-harvest --features atomicity-spike \
//!   --test integration \
//!   atomicity_spike_tests::measure_the_full_matrix -- --ignored --nocapture
//! ```

use std::time::Duration;

use diesel::sql_types::{BigInt, Text};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};

use autumn_harvest::atomicity::backout::run_backout;
use autumn_harvest::atomicity::harness::{
    self, CellConfig, CellResult, Contention, Order, RetryCounter, RunOutcome, Totals, Workload,
};
use autumn_harvest::atomicity::rule::{Atomicity, choose};
use autumn_harvest::atomicity::verdict::{self, ArmResult, Cell, Criterion};
use autumn_harvest::error::{HarvestError, HarvestResult};
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::tx_retry::TxRetryPolicy;
use autumn_harvest::worker::DbPool;

use crate::integration_e2e::{build_test_pool, setup_test_database_url_or_env};

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url).await.expect("connect")
}

#[derive(diesel::QueryableByName)]
struct Label {
    #[diesel(sql_type = Text)]
    label: String,
}

/// A scratch table for the savepoint tests.
///
/// It is a temporary table, so it goes away with the connection, even
/// after a failed assertion.
async fn scratch_table(conn: &mut AsyncPgConnection) -> String {
    let name = format!("atomicity_scratch_{}", uuid::Uuid::new_v4().simple());
    conn.batch_execute(&format!(
        "CREATE TEMPORARY TABLE {name} (label TEXT PRIMARY KEY)"
    ))
    .await
    .expect("create the scratch table");
    name
}

async fn labels(conn: &mut AsyncPgConnection, table: &str) -> Vec<String> {
    diesel::sql_query(format!("SELECT label FROM {table} ORDER BY label"))
        .load::<Label>(conn)
        .await
        .expect("read labels")
        .into_iter()
        .map(|row| row.label)
        .collect()
}

async fn insert(conn: &mut AsyncPgConnection, table: &str, label: &str) -> HarvestResult<()> {
    diesel::sql_query(format!("INSERT INTO {table} (label) VALUES ($1)"))
        .bind::<Text, _>(label)
        .execute(conn)
        .await?;
    Ok(())
}

fn declined() -> HarvestError {
    HarvestError::workflow_failed_untyped("test", "declined")
}

#[tokio::test]
async fn a_failed_step_rolls_back_to_its_savepoint_only() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let table = scratch_table(&mut conn).await;
    let t = table.as_str();

    let result = run_backout(
        &mut conn,
        &NoOpMetrics,
        TxRetryPolicy::DEFAULT,
        async |steps| {
            steps.step(async |c| insert(c, t, "a").await).await?;
            let failed = steps
                .step(async |c| {
                    insert(c, t, "b").await?;
                    Err::<(), _>(declined())
                })
                .await;
            assert!(failed.is_err(), "the step error reaches the body");
            steps.step(async |c| insert(c, t, "c").await).await
        },
    )
    .await;

    result.expect("the run commits");
    assert_eq!(labels(&mut conn, &table).await, ["a", "c"]);
}

#[tokio::test]
async fn a_failed_run_leaves_no_row() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let table = scratch_table(&mut conn).await;
    let t = table.as_str();

    let result = run_backout(
        &mut conn,
        &NoOpMetrics,
        TxRetryPolicy::DEFAULT,
        async |steps| {
            steps.step(async |c| insert(c, t, "a").await).await?;
            steps
                .step(async |c| {
                    insert(c, t, "b").await?;
                    Err::<(), _>(declined())
                })
                .await
        },
    )
    .await;

    assert!(
        matches!(result, Err(HarvestError::WorkflowFailed { ref reason, .. }) if reason == "declined"),
        "the body error is returned verbatim: {result:?}"
    );
    assert_eq!(labels(&mut conn, &table).await, Vec::<String>::new());
}

#[tokio::test]
async fn a_sql_error_in_a_step_does_not_poison_the_run() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let table = scratch_table(&mut conn).await;
    let t = table.as_str();

    run_backout(
        &mut conn,
        &NoOpMetrics,
        TxRetryPolicy::DEFAULT,
        async |steps| {
            steps.step(async |c| insert(c, t, "a").await).await?;
            let duplicate = steps.step(async |c| insert(c, t, "a").await).await;
            assert!(duplicate.is_err(), "a duplicate key fails the step");
            steps.step(async |c| insert(c, t, "b").await).await
        },
    )
    .await
    .expect("a later step still runs");

    assert_eq!(labels(&mut conn, &table).await, ["a", "b"]);
}

#[derive(diesel::QueryableByName)]
struct Txid {
    #[diesel(sql_type = BigInt)]
    id: i64,
}

async fn txid(conn: &mut AsyncPgConnection) -> HarvestResult<i64> {
    let row = diesel::sql_query("SELECT txid_current() AS id")
        .get_result::<Txid>(conn)
        .await?;
    Ok(row.id)
}

#[tokio::test]
async fn a_body_runs_inside_one_transaction() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;

    let ids = run_backout(
        &mut conn,
        &NoOpMetrics,
        TxRetryPolicy::DEFAULT,
        async |steps| {
            let first = steps.step(async |c| txid(c).await).await?;
            let second = steps.step(async |c| txid(c).await).await?;
            Ok((first, second))
        },
    )
    .await
    .expect("the run commits");

    assert_eq!(ids.0, ids.1, "both steps see one top-level transaction");
}

/// The history rows that one run writes under each arm.
const fn history_rows(arm: Atomicity, outcome: RunOutcome) -> i64 {
    match (arm, outcome) {
        (Atomicity::Backout, RunOutcome::Committed) => 1,
        (Atomicity::Backout, RunOutcome::RolledBack) => 0,
        // Reserve, debit and place commit. On a decline, place rolls back,
        // and two compensations commit.
        (Atomicity::Saga, RunOutcome::Committed) => 3,
        (Atomicity::Saga, RunOutcome::RolledBack) => 4,
        // The escrow step and the backout transaction commit. On a decline,
        // the backout rolls back, and the restock commits.
        (Atomicity::Hybrid, RunOutcome::Committed | RunOutcome::RolledBack) => 2,
    }
}

async fn one_order(url: &str, pool: &DbPool, arm: Atomicity, decline: bool) -> Totals {
    let mut conn = connect(url).await;
    let workload = Workload::create(&mut conn, Contention::High)
        .await
        .expect("create the workload");
    let order = Order {
        sku: 0,
        account: 7,
        amount: 125,
        decline,
    };

    let outcome = harness::run_order(pool, &workload, arm, order, Duration::ZERO).await;
    let totals = workload.totals(&mut conn).await;
    workload.drop_tables(&mut conn).await.expect("drop");

    let outcome = outcome.expect("the run ends without a database error");
    let expected = if decline {
        RunOutcome::RolledBack
    } else {
        RunOutcome::Committed
    };
    assert_eq!(outcome, expected, "{arm:?}");
    let totals = totals.expect("totals");
    assert_eq!(totals.history_rows, history_rows(arm, outcome), "{arm:?}");
    totals
}

#[tokio::test]
async fn each_arm_commits_an_order() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    for arm in Atomicity::ALL {
        let totals = one_order(&url, &pool, arm, false).await;
        assert_eq!(
            (
                totals.stock_taken,
                totals.orders,
                totals.money_taken,
                totals.order_total
            ),
            (1, 1, 125, 125),
            "{arm:?}"
        );
    }
}

#[tokio::test]
async fn each_arm_undoes_a_declined_order() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    for arm in Atomicity::ALL {
        let totals = one_order(&url, &pool, arm, true).await;
        assert_eq!(
            (
                totals.stock_taken,
                totals.orders,
                totals.money_taken,
                totals.order_total
            ),
            (0, 0, 0, 0),
            "{arm:?}"
        );
    }
}

#[tokio::test]
async fn commit_latency_is_positive() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let latency = harness::commit_latency(&mut conn, 5)
        .await
        .expect("measure the commit latency");
    assert!(latency > Duration::ZERO);
    assert!(latency < Duration::from_secs(1));
}

#[tokio::test]
async fn a_short_hot_cell_keeps_the_invariants_for_each_arm() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    for arm in Atomicity::ALL {
        let result = harness::run_cell(
            &pool,
            CellConfig {
                arm,
                contention: Contention::High,
                clients: 4,
                duration: Duration::from_millis(400),
                step_work: Duration::from_millis(1),
                fail_rate: 0.3,
                seed: 2012,
            },
        )
        .await
        .expect("run the cell");
        assert_eq!(result.errors, 0, "{arm:?}");
        assert!(result.committed > 0, "{arm:?} committed no run");
        assert!(result.rolled_back > 0, "{arm:?} declined no run");
        assert!(result.totals.consistent(), "{arm:?}: {:?}", result.totals);
        assert_eq!(
            result.totals.orders,
            i64::try_from(result.committed).expect("fits"),
            "{arm:?}"
        );
        let committed = i64::try_from(result.committed).expect("fits");
        let rolled_back = i64::try_from(result.rolled_back).expect("fits");
        assert_eq!(
            result.totals.history_rows,
            committed * history_rows(arm, RunOutcome::Committed)
                + rolled_back * history_rows(arm, RunOutcome::RolledBack),
            "{arm:?} commits exactly its own transactions"
        );
        assert!(result.committed_in_window <= result.committed, "{arm:?}");
    }
}

#[derive(diesel::QueryableByName)]
struct Count {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

async fn bump(conn: &mut AsyncPgConnection, table: &str, label: &str) -> HarvestResult<()> {
    diesel::sql_query(format!("UPDATE {table} SET n = n + 1 WHERE label = $1"))
        .bind::<Text, _>(label)
        .execute(conn)
        .await?;
    Ok(())
}

/// One side of the deadlock test. It locks `first`, waits for the other
/// side on its first attempt, then locks `second`. It ignores the error of
/// the second step, as a body with an optional step does.
async fn lock_both(
    conn: &mut AsyncPgConnection,
    table: &str,
    first: &str,
    second: &str,
    barrier: &tokio::sync::Barrier,
    retries: &RetryCounter,
) -> HarvestResult<()> {
    let attempts = std::sync::atomic::AtomicU32::new(0);
    run_backout(conn, retries, TxRetryPolicy::DEFAULT, async |steps| {
        let attempt = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        steps.step(async |c| bump(c, table, first).await).await?;
        if attempt == 0 {
            barrier.wait().await;
        }
        let _ = steps.step(async |c| bump(c, table, second).await).await;
        Ok(())
    })
    .await
}

#[tokio::test]
async fn a_deadlock_in_an_ignored_step_still_retries_the_whole_run() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut setup = connect(&url).await;
    let table = format!("atomicity_locks_{}", uuid::Uuid::new_v4().simple());
    setup
        .batch_execute(&format!(
            "CREATE TABLE {table} (label TEXT PRIMARY KEY, n BIGINT NOT NULL);
             INSERT INTO {table} VALUES ('a', 0), ('b', 0);"
        ))
        .await
        .expect("create the lock table");
    let mut left = connect(&url).await;
    let mut right = connect(&url).await;
    let barrier = tokio::sync::Barrier::new(2);
    let retries = RetryCounter::default();

    // A side that fails before the barrier would leave the other side
    // waiting. The timeout turns that hang into a failure.
    let (l, r) = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(
            lock_both(&mut left, &table, "a", "b", &barrier, &retries),
            lock_both(&mut right, &table, "b", "a", &barrier, &retries),
        )
    })
    .await
    .expect("the deadlock resolves within 30 s");
    let counts = diesel::sql_query(format!("SELECT n FROM {table} ORDER BY label"))
        .load::<Count>(&mut setup)
        .await
        .map(|rows| rows.into_iter().map(|row| row.n).collect::<Vec<_>>());
    setup
        .batch_execute(&format!("DROP TABLE {table}"))
        .await
        .expect("drop the lock table");

    l.expect("the left run commits");
    r.expect("the right run commits");
    assert!(
        retries.get() >= 1,
        "Postgres broke the cycle, so one run retried"
    );
    assert_eq!(
        counts.expect("read the counts"),
        [2, 2],
        "each run updated both rows, so the victim did not commit half a run"
    );
}

#[tokio::test]
async fn a_run_inside_an_open_transaction_rolls_back_only_its_savepoint() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let table = scratch_table(&mut conn).await;
    let t = table.as_str();

    Box::pin(conn.transaction::<(), HarvestError, _>(async |tx| {
        let inner = run_backout(tx, &NoOpMetrics, TxRetryPolicy::DEFAULT, async |steps| {
            steps.step(async |c| insert(c, t, "a").await).await?;
            Err::<(), _>(declined())
        })
        .await;
        assert!(inner.is_err(), "the body error reaches the caller");
        insert(tx, t, "b").await
    }))
    .await
    .expect("the outer transaction commits");

    assert_eq!(labels(&mut conn, &table).await, ["b"]);
}

/// Repetitions per cell in the full measurement.
const REPETITIONS: usize = 3;

/// Every run of one arm in one cell, across the repetitions.
struct ArmSummary {
    config: CellConfig,
    runs: Vec<CellResult>,
}

impl ArmSummary {
    fn goodputs(&self) -> Vec<f64> {
        let mut values: Vec<f64> = self.runs.iter().map(CellResult::goodput).collect();
        values.sort_by(f64::total_cmp);
        values
    }

    /// The run with the median goodput.
    fn median(&self) -> CellResult {
        let mut runs = self.runs.clone();
        runs.sort_by(|a, b| a.goodput().total_cmp(&b.goodput()));
        runs[runs.len() / 2]
    }

    fn invariants_held(&self) -> bool {
        self.runs.iter().all(|run| run.totals.consistent())
    }
}

const fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1e3
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)] // one linear report: setup, runs, two tables
#[ignore = "runs for about 6 minutes; see the module docs"]
async fn measure_the_full_matrix() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let clients = 16;
    let pool = build_test_pool(&url);
    assert_eq!(
        pool.status().max_size,
        clients + 4,
        "the pool keeps four spare connections"
    );
    let mut conn = connect(&url).await;

    // Open every pool connection before the first timed cell.
    let mut warm = Vec::new();
    for _ in 0..clients + 4 {
        warm.push(pool.get().await.expect("warm the pool"));
    }
    drop(warm);

    let commit = harness::commit_latency(&mut conn, 200)
        .await
        .expect("measure the commit latency");
    println!("commit latency (median of 200): {commit:?}");
    for setting in [
        "server_version",
        "fsync",
        "synchronous_commit",
        "shared_buffers",
    ] {
        #[derive(diesel::QueryableByName)]
        struct Setting {
            #[diesel(sql_type = Text)]
            value: String,
        }
        let row = diesel::sql_query(format!("SELECT current_setting('{setting}') AS value"))
            .get_result::<Setting>(&mut conn)
            .await
            .expect("read a setting");
        println!("{setting}: {}", row.value);
    }
    println!();

    // Each repetition runs every cell, and rotates the arm order.
    let configs = harness::matrix(clients, Duration::from_secs(10), 2012);
    let mut summaries: Vec<ArmSummary> = configs
        .iter()
        .map(|config| ArmSummary {
            config: *config,
            runs: Vec::new(),
        })
        .collect();
    for rep in 0..REPETITIONS {
        for group in (0..summaries.len()).step_by(Atomicity::ALL.len()) {
            for offset in 0..Atomicity::ALL.len() {
                let index = group + (offset + rep) % Atomicity::ALL.len();
                let config = CellConfig {
                    seed: summaries[index].config.seed + rep as u64,
                    ..summaries[index].config
                };
                // CHECKPOINT needs a superuser or `pg_checkpoint`. Without
                // either, the run goes on and the dirty pages stay.
                if let Err(error) = conn.batch_execute("CHECKPOINT").await {
                    println!("CHECKPOINT skipped: {error}");
                }
                let run = harness::run_cell(&pool, config)
                    .await
                    .expect("run the cell");
                summaries[index].runs.push(run);
            }
        }
    }

    println!(
        "| contention | step work | arm | goodput (runs/s) median [min, max] | P50 ms | P90 ms | declined P90 ms | hot hold P50 ms | declined | errors | retries | drain ms | invariants |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|---|");
    let mut cells: Vec<Cell> = Vec::new();
    for summary in &summaries {
        let config = summary.config;
        let goodputs = summary.goodputs();
        let median = summary.median();
        let held = summary.invariants_held();
        println!(
            "| {} | {} ms | `{}` | {:.1} [{:.1}, {:.1}] | {:.1} | {:.1} | {:.1} | {:.2} | {} | {} | {} | {:.0} | {} |",
            config.contention.as_str(),
            config.step_work.as_millis(),
            config.arm.as_str(),
            median.goodput(),
            goodputs[0],
            goodputs[goodputs.len() - 1],
            ms(median.p50),
            ms(median.p90),
            ms(median.declined_p90),
            ms(median.hot_hold_p50),
            median.rolled_back,
            summary.runs.iter().map(|run| run.errors).sum::<u64>(),
            summary.runs.iter().map(|run| run.retries).sum::<u64>(),
            ms(median.drain),
            if held { "held" } else { "BROKEN" },
        );
        let long_steps = !config.step_work.is_zero();
        let result = ArmResult {
            arm: config.arm,
            goodput: median.goodput(),
            min: goodputs[0],
            max: goodputs[goodputs.len() - 1],
            invariants_held: held,
        };
        match cells
            .iter_mut()
            .find(|cell| cell.contention == config.contention && cell.long_steps == long_steps)
        {
            Some(cell) => cell.arms.push(result),
            None => cells.push(Cell {
                contention: config.contention,
                long_steps,
                arms: vec![result],
                pick: choose(&config.profile(commit)).atomicity,
            }),
        }
    }

    println!(
        "\n| contention | step work | rule pick | reason | best arm | best range overlaps runner-up |"
    );
    println!("|---|---|---|---|---|---|");
    for group in summaries.chunks(Atomicity::ALL.len()) {
        let config = group[0].config;
        let choice = choose(&config.profile(commit));
        let mut ranked: Vec<&ArmSummary> = group.iter().collect();
        ranked.sort_by(|a, b| b.median().goodput().total_cmp(&a.median().goodput()));
        let best = ranked[0].goodputs();
        let next = ranked[1].goodputs();
        let overlap = best[0] <= next[next.len() - 1];
        println!(
            "| {} | {} ms | `{}` | `{:?}` | `{}` | {} |",
            config.contention.as_str(),
            config.step_work.as_millis(),
            choice.atomicity.as_str(),
            choice.reason,
            ranked[0].config.arm.as_str(),
            if overlap { "yes" } else { "no" },
        );
    }

    let verdict = verdict::judge(&cells).expect("the matrix is complete");
    println!("\n{verdict:?}");
    assert_ne!(verdict.g4, Criterion::Fails, "an arm broke an invariant");
}
