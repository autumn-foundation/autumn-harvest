#![cfg(feature = "db")]
//! Database tests for the atomicity spike (issue #2012).
//!
//! Set `HARVEST_TEST_DATABASE_URL` to use a running Postgres. Otherwise each
//! test boots a testcontainers Postgres. Each test creates its own tables.
//!
//! `measure_the_full_matrix` is `#[ignore]`. It runs the pre-registered
//! matrix for about 6 minutes and prints the report tables:
//!
//! ```text
//! cargo test --release -p autumn-harvest --test integration \
//!   atomicity_spike_tests::measure_the_full_matrix -- --ignored --nocapture
//! ```

use std::time::Duration;

use diesel::sql_types::{BigInt, Text};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use autumn_harvest::atomicity::backout::run_backout;
use autumn_harvest::atomicity::harness::{
    self, CellConfig, Contention, Order, RunOutcome, Totals, Workload,
};
use autumn_harvest::atomicity::rule::{Atomicity, choose};
use autumn_harvest::atomicity::verdict::{self, ArmResult, Cell};
use autumn_harvest::error::{HarvestError, HarvestResult};
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::tx_retry::TxRetryPolicy;
use autumn_harvest::worker::DbPool;

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("start the Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url).await.expect("connect")
}

fn make_pool(url: &str, size: usize) -> DbPool {
    let manager =
        diesel_async::pooled_connection::AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            url,
        );
    deadpool::managed::Pool::builder(manager)
        .max_size(size)
        .build()
        .expect("build the pool")
}

#[derive(diesel::QueryableByName)]
struct Label {
    #[diesel(sql_type = Text)]
    label: String,
}

/// A scratch table for the savepoint tests, with a unique name.
async fn scratch_table(conn: &mut AsyncPgConnection) -> String {
    let name = format!("atomicity_scratch_{}", uuid::Uuid::new_v4().simple());
    conn.batch_execute(&format!("CREATE TABLE {name} (label TEXT PRIMARY KEY)"))
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
    let (url, _container) = setup_db().await;
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
    let (url, _container) = setup_db().await;
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
    assert!(labels(&mut conn, &table).await.is_empty());
}

#[tokio::test]
async fn a_sql_error_in_a_step_does_not_poison_the_run() {
    let (url, _container) = setup_db().await;
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
    let (url, _container) = setup_db().await;
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
fn history_rows(arm: Atomicity, outcome: RunOutcome) -> i64 {
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

async fn one_order(arm: Atomicity, decline: bool) -> Totals {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let pool = make_pool(&url, 4);
    let workload = Workload::create(&mut conn, Contention::High)
        .await
        .expect("create the workload");
    let order = Order {
        sku: 0,
        account: 7,
        amount: 125,
        decline,
    };

    let outcome = harness::run_order(&pool, &workload, arm, order, Duration::ZERO)
        .await
        .expect("the run ends without a database error");
    let expected = if decline {
        RunOutcome::RolledBack
    } else {
        RunOutcome::Committed
    };
    assert_eq!(outcome, expected, "{arm:?}");

    let totals = workload.totals(&mut conn).await.expect("totals");
    assert_eq!(totals.history_rows, history_rows(arm, outcome), "{arm:?}");
    workload.drop_tables(&mut conn).await.expect("drop");
    totals
}

#[tokio::test]
async fn each_arm_commits_an_order() {
    for arm in Atomicity::ALL {
        let totals = one_order(arm, false).await;
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
    for arm in Atomicity::ALL {
        let totals = one_order(arm, true).await;
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
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let latency = harness::commit_latency(&mut conn, 5)
        .await
        .expect("measure the commit latency");
    assert!(latency > Duration::ZERO);
    assert!(latency < Duration::from_secs(1));
}

#[tokio::test]
async fn a_short_hot_cell_keeps_the_invariants_for_each_arm() {
    let (url, _container) = setup_db().await;
    let pool = make_pool(&url, 8);
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
        assert!(result.p50 <= result.p90, "{arm:?}");
    }
}

/// Repetitions per cell in the full measurement.
const REPETITIONS: usize = 3;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "runs for about 6 minutes; see the module docs"]
async fn measure_the_full_matrix() {
    let (url, _container) = setup_db().await;
    let clients = 16;
    let pool = make_pool(&url, clients + 4);
    let mut conn = connect(&url).await;
    let commit = harness::commit_latency(&mut conn, 200)
        .await
        .expect("measure the commit latency");
    println!("commit latency (median of 200): {commit:?}\n");

    println!(
        "| contention | step work | arm | goodput (runs/s) median [min, max] | P50 ms | P90 ms | declined | errors | invariants |"
    );
    println!("|---|---|---|---|---|---|---|---|---|");
    let mut cells: Vec<Cell> = Vec::new();
    let configs = harness::matrix(clients, Duration::from_secs(10), 2012);
    for config in &configs {
        let mut runs = Vec::new();
        for rep in 0..REPETITIONS {
            let seeded = CellConfig {
                seed: config.seed + rep as u64,
                ..*config
            };
            runs.push(
                harness::run_cell(&pool, seeded)
                    .await
                    .expect("run the cell"),
            );
        }
        runs.sort_by(|a, b| a.goodput().total_cmp(&b.goodput()));
        let median = runs[REPETITIONS / 2];
        let held = runs
            .iter()
            .all(|run| run.errors == 0 && run.totals.consistent());
        println!(
            "| {} | {} ms | {} | {:.1} [{:.1}, {:.1}] | {:.1} | {:.1} | {} | {} | {} |",
            config.contention.as_str(),
            config.step_work.as_millis(),
            config.arm.as_str(),
            median.goodput(),
            runs[0].goodput(),
            runs[REPETITIONS - 1].goodput(),
            median.p50.as_secs_f64() * 1e3,
            median.p90.as_secs_f64() * 1e3,
            median.rolled_back,
            runs.iter().map(|run| run.errors).sum::<u64>(),
            if held { "held" } else { "BROKEN" },
        );
        let long_steps = !config.step_work.is_zero();
        let pick = choose(&config.profile(commit)).atomicity;
        let result = ArmResult {
            arm: config.arm,
            goodput: median.goodput(),
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
                pick,
            }),
        }
    }

    println!("\n| contention | step work | rule pick | reason | best arm |");
    println!("|---|---|---|---|---|");
    for config in configs.iter().filter(|c| c.arm == Atomicity::Backout) {
        let choice = choose(&config.profile(commit));
        let cell = cells
            .iter()
            .find(|cell| {
                cell.contention == config.contention
                    && cell.long_steps == !config.step_work.is_zero()
            })
            .expect("cell");
        let best = cell
            .arms
            .iter()
            .max_by(|a, b| a.goodput.total_cmp(&b.goodput))
            .expect("arms");
        println!(
            "| {} | {} ms | {} | {:?} | {} |",
            config.contention.as_str(),
            config.step_work.as_millis(),
            choice.atomicity.as_str(),
            choice.reason,
            best.arm.as_str(),
        );
    }

    let verdict = verdict::judge(&cells);
    println!("\n{verdict:?}");
    assert!(verdict.g4, "an arm broke an invariant");
}
