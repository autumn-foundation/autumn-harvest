//! The measurement harness: one order workflow, three arms (issue #2012).
//!
//! One run reserves a unit of stock, debits an account and places an order.
//! The place step declines a seeded share of the orders. Each arm then undoes
//! the run in its own way. Each step holds its transaction open for
//! `step_work` after its write, which stands for application work.
//!
//! Each committed transaction inserts one history row. That stands for the
//! `ActivityCompleted` event that Harvest appends per step. The harness
//! omits the rest of the engine cost: claims, workflow tasks and poll gaps.
//! That cost falls on the saga, which commits once per step, so the omission
//! favours the saga.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use diesel::sql_types::BigInt;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use rand::{Rng, SeedableRng, rngs::StdRng};

use super::backout::{StepFn, run_backout};
use super::rule::{Atomicity, WorkloadProfile};
use crate::context::WorkflowContext;
use crate::error::{HarvestError, HarvestResult};
use crate::pool::acquire_within_pool_bound;
use crate::saga::Saga;
use crate::telemetry::MetricsRecorder;
use crate::tx_retry::TxRetryPolicy;
use crate::types::ExecutionId;
use crate::worker::DbPool;

/// The stock of each SKU at the start of a cell.
pub const INITIAL_QTY: i64 = 1_000_000_000;
/// The balance of each account at the start of a cell.
pub const INITIAL_BALANCE: i64 = 1_000_000_000_000;
/// The number of accounts.
pub const ACCOUNTS: i64 = 1_000;
/// The reason that a declined order carries.
pub const DECLINED: &str = "order declined";
/// The number of steps in one run.
pub const STEPS: u32 = 3;
/// The highest order amount.
pub const MAX_AMOUNT: i64 = 100;
/// The share of orders that the place step declines in the matrix.
pub const FAIL_RATE: f64 = 0.1;

/// How many SKUs the runs share.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Contention {
    /// 1,000 SKUs, picked uniformly.
    Low,
    /// One SKU.
    High,
}

impl Contention {
    /// The number of SKUs.
    #[must_use]
    pub const fn skus(self) -> i64 {
        match self {
            Self::Low => 1_000,
            Self::High => 1,
        }
    }

    /// The report label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::High => "high",
        }
    }
}

/// One order run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Order {
    /// The SKU to reserve.
    pub sku: i64,
    /// The account to debit.
    pub account: i64,
    /// The price.
    pub amount: i64,
    /// The place step declines the order.
    pub decline: bool,
}

/// How one run ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    /// Every step committed.
    Committed,
    /// The place step declined the order, and the arm undid every effect.
    RolledBack,
}

/// The tables of one cell. Each cell gets fresh tables.
#[derive(Debug)]
pub struct Workload {
    prefix: String,
    retries: RetryCounter,
}

/// Counts the conflict retries of the backout transactions.
#[derive(Debug, Default)]
pub struct RetryCounter(AtomicU64);

impl RetryCounter {
    /// The retries so far.
    #[must_use]
    pub fn get(&self) -> u64 {
        AtomicU64::load(&self.0, Ordering::Relaxed)
    }
}

impl MetricsRecorder for RetryCounter {
    fn record_db_transaction_retry(&self, _site: &str, _reason: &str) {
        AtomicU64::fetch_add(&self.0, 1, Ordering::Relaxed);
    }
}

/// The sums that the invariants compare.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Totals {
    /// Units taken from stock.
    pub stock_taken: i64,
    /// Order rows.
    pub orders: i64,
    /// Money taken from accounts.
    pub money_taken: i64,
    /// The sum of the order amounts.
    pub order_total: i64,
    /// History rows: one per committed transaction.
    pub history_rows: i64,
}

impl Totals {
    /// Stock taken equals orders placed, and money taken equals order totals.
    #[must_use]
    pub const fn consistent(&self) -> bool {
        self.stock_taken == self.orders && self.money_taken == self.order_total
    }
}

#[derive(diesel::QueryableByName)]
struct TotalsRow {
    #[diesel(sql_type = BigInt)]
    stock_taken: i64,
    #[diesel(sql_type = BigInt)]
    orders: i64,
    #[diesel(sql_type = BigInt)]
    money_taken: i64,
    #[diesel(sql_type = BigInt)]
    order_total: i64,
    #[diesel(sql_type = BigInt)]
    history_rows: i64,
}

impl Workload {
    /// Create and fill fresh tables for `contention`.
    ///
    /// # Errors
    ///
    /// Returns a database error.
    pub async fn create(
        conn: &mut AsyncPgConnection,
        contention: Contention,
    ) -> HarvestResult<Self> {
        let prefix = format!("atomicity_{}", uuid::Uuid::new_v4().simple());
        let skus = contention.skus();
        conn.batch_execute(&format!(
            "CREATE TABLE {prefix}_inventory (sku BIGINT PRIMARY KEY, qty BIGINT NOT NULL);
             CREATE TABLE {prefix}_accounts (id BIGINT PRIMARY KEY, balance BIGINT NOT NULL);
             CREATE TABLE {prefix}_orders (id BIGSERIAL PRIMARY KEY, sku BIGINT NOT NULL,
                 account BIGINT NOT NULL, amount BIGINT NOT NULL);
             CREATE TABLE {prefix}_history (id BIGSERIAL PRIMARY KEY, step TEXT NOT NULL);
             INSERT INTO {prefix}_inventory
                 SELECT g, {INITIAL_QTY} FROM generate_series(0, {skus} - 1) AS g;
             INSERT INTO {prefix}_accounts
                 SELECT g, {INITIAL_BALANCE} FROM generate_series(0, {ACCOUNTS} - 1) AS g;"
        ))
        .await?;
        Ok(Self {
            prefix,
            retries: RetryCounter::default(),
        })
    }

    /// The conflict retries of the backout transactions so far.
    #[must_use]
    pub fn retries(&self) -> u64 {
        self.retries.get()
    }

    /// Drop the tables.
    ///
    /// # Errors
    ///
    /// Returns a database error.
    pub async fn drop_tables(&self, conn: &mut AsyncPgConnection) -> HarvestResult<()> {
        let p = &self.prefix;
        conn.batch_execute(&format!(
            "DROP TABLE IF EXISTS {p}_inventory, {p}_accounts, {p}_orders, {p}_history"
        ))
        .await?;
        Ok(())
    }

    /// Read the invariant sums.
    ///
    /// # Errors
    ///
    /// Returns a database error.
    pub async fn totals(&self, conn: &mut AsyncPgConnection) -> HarvestResult<Totals> {
        let p = &self.prefix;
        let row = diesel::sql_query(format!(
            "SELECT
                (SELECT COALESCE(SUM({INITIAL_QTY} - qty), 0)::BIGINT FROM {p}_inventory) AS stock_taken,
                (SELECT COUNT(*) FROM {p}_orders) AS orders,
                (SELECT COALESCE(SUM({INITIAL_BALANCE} - balance), 0)::BIGINT FROM {p}_accounts) AS money_taken,
                (SELECT COALESCE(SUM(amount), 0)::BIGINT FROM {p}_orders) AS order_total,
                (SELECT COUNT(*) FROM {p}_history) AS history_rows"
        ))
        .get_result::<TotalsRow>(conn)
        .await?;
        Ok(Totals {
            stock_taken: row.stock_taken,
            orders: row.orders,
            money_taken: row.money_taken,
            order_total: row.order_total,
            history_rows: row.history_rows,
        })
    }

    /// Reserve one unit, then work. Returns the instant of the hot write.
    async fn reserve(
        &self,
        conn: &mut AsyncPgConnection,
        order: Order,
        work: Duration,
    ) -> HarvestResult<Instant> {
        let rows = diesel::sql_query(format!(
            "UPDATE {}_inventory SET qty = qty - 1 WHERE sku = $1 AND qty > 0",
            self.prefix
        ))
        .bind::<BigInt, _>(order.sku)
        .execute(conn)
        .await?;
        let written = Instant::now();
        if rows == 0 {
            return Err(declined());
        }
        do_work(work).await;
        Ok(written)
    }

    /// Put the unit back. This is the compensation of [`Self::reserve`].
    async fn restock(&self, conn: &mut AsyncPgConnection, order: Order) -> HarvestResult<()> {
        diesel::sql_query(format!(
            "UPDATE {}_inventory SET qty = qty + 1 WHERE sku = $1",
            self.prefix
        ))
        .bind::<BigInt, _>(order.sku)
        .execute(conn)
        .await?;
        Ok(())
    }

    /// Debit the account, then work.
    async fn debit(
        &self,
        conn: &mut AsyncPgConnection,
        order: Order,
        work: Duration,
    ) -> HarvestResult<()> {
        let rows = diesel::sql_query(format!(
            "UPDATE {}_accounts SET balance = balance - $2 WHERE id = $1 AND balance >= $2",
            self.prefix
        ))
        .bind::<BigInt, _>(order.account)
        .bind::<BigInt, _>(order.amount)
        .execute(conn)
        .await?;
        if rows == 0 {
            return Err(declined());
        }
        do_work(work).await;
        Ok(())
    }

    /// Credit the amount back. This is the compensation of [`Self::debit`].
    async fn credit(&self, conn: &mut AsyncPgConnection, order: Order) -> HarvestResult<()> {
        diesel::sql_query(format!(
            "UPDATE {}_accounts SET balance = balance + $2 WHERE id = $1",
            self.prefix
        ))
        .bind::<BigInt, _>(order.account)
        .bind::<BigInt, _>(order.amount)
        .execute(conn)
        .await?;
        Ok(())
    }

    /// Insert the order, work, then decline it if the draw says so.
    async fn place(
        &self,
        conn: &mut AsyncPgConnection,
        order: Order,
        work: Duration,
    ) -> HarvestResult<()> {
        diesel::sql_query(format!(
            "INSERT INTO {}_orders (sku, account, amount) VALUES ($1, $2, $3)",
            self.prefix
        ))
        .bind::<BigInt, _>(order.sku)
        .bind::<BigInt, _>(order.account)
        .bind::<BigInt, _>(order.amount)
        .execute(conn)
        .await?;
        do_work(work).await;
        if order.decline {
            return Err(declined());
        }
        Ok(())
    }

    /// Insert one history row for the transaction that commits.
    async fn record(&self, conn: &mut AsyncPgConnection, step: &str) -> HarvestResult<()> {
        diesel::sql_query(format!(
            "INSERT INTO {}_history (step) VALUES ($1)",
            self.prefix
        ))
        .bind::<diesel::sql_types::Text, _>(step)
        .execute(conn)
        .await?;
        Ok(())
    }
}

fn declined() -> HarvestError {
    HarvestError::workflow_failed_untyped("atomicity_order", DECLINED)
}

fn is_declined(error: &HarvestError) -> bool {
    matches!(error, HarvestError::WorkflowFailed { reason, .. } if reason == DECLINED)
}

async fn do_work(work: Duration) {
    if !work.is_zero() {
        tokio::time::sleep(work).await;
    }
}

/// Run `body` in its own transaction on a pooled connection, and commit it.
async fn committed<T, F>(pool: &DbPool, body: F) -> HarvestResult<T>
where
    for<'r> F: StepFn<&'r mut AsyncPgConnection, HarvestResult<T>, Fut: Send> + Send,
    T: Send,
{
    let mut conn = acquire_within_pool_bound(pool).await?;
    Box::pin(conn.transaction::<T, HarvestError, _>(async move |tx| body(tx).await)).await
}

/// The backout transaction: reserve, debit and place, or only the last two.
///
/// With the reserve step, returns the hot-lock hold: the time from the hot
/// write to the commit.
async fn backout_run(
    pool: &DbPool,
    workload: &Workload,
    order: Order,
    work: Duration,
    with_reserve: bool,
) -> HarvestResult<Option<Duration>> {
    let mut conn = acquire_within_pool_bound(pool).await?;
    let written = run_backout(
        &mut conn,
        &workload.retries,
        TxRetryPolicy::DEFAULT,
        async |steps| {
            let written = if with_reserve {
                Some(
                    steps
                        .step(async move |c| workload.reserve(c, order, work).await)
                        .await?,
                )
            } else {
                None
            };
            steps
                .step(async move |c| workload.debit(c, order, work).await)
                .await?;
            steps
                .step(async move |c| {
                    workload.place(c, order, work).await?;
                    workload.record(c, "backout").await
                })
                .await?;
            Ok(written)
        },
    )
    .await?;
    Ok(written.map(|at| at.elapsed()))
}

/// The reserve step in its own transaction: the escrow step.
///
/// Returns the hot-lock hold: the time from the hot write to the commit.
async fn reserve_committed(
    pool: &DbPool,
    workload: &Workload,
    order: Order,
    work: Duration,
) -> HarvestResult<Duration> {
    let written = committed(pool, async move |c| {
        let written = workload.reserve(c, order, work).await?;
        workload.record(c, "reserve").await?;
        Ok(written)
    })
    .await?;
    Ok(written.elapsed())
}

/// The restock compensation in its own transaction.
async fn restock_committed(pool: &DbPool, workload: &Workload, order: Order) -> HarvestResult<()> {
    committed(pool, async move |c| {
        workload.restock(c, order).await?;
        workload.record(c, "restock").await
    })
    .await
}

async fn saga_run(
    pool: &DbPool,
    workload: &Workload,
    order: Order,
    work: Duration,
) -> HarvestResult<Duration> {
    let ctx = WorkflowContext::for_replay(ExecutionId::new(), Vec::new());
    let mut saga = Saga::new(&ctx);
    let hold = saga
        .step(
            || reserve_committed(pool, workload, order, work),
            move |_| restock_committed(pool, workload, order),
        )
        .await?;
    saga.step(
        || {
            committed(pool, async move |c| {
                workload.debit(c, order, work).await?;
                workload.record(c, "debit").await
            })
        },
        move |()| {
            committed(pool, async move |c| {
                workload.credit(c, order).await?;
                workload.record(c, "credit").await
            })
        },
    )
    .await?;
    saga.step(
        || {
            committed(pool, async move |c| {
                workload.place(c, order, work).await?;
                workload.record(c, "place").await
            })
        },
        |()| async { Ok::<(), HarvestError>(()) },
    )
    .await?;
    Ok(hold)
}

async fn hybrid_run(
    pool: &DbPool,
    workload: &Workload,
    order: Order,
    work: Duration,
) -> HarvestResult<Duration> {
    let ctx = WorkflowContext::for_replay(ExecutionId::new(), Vec::new());
    let mut saga = Saga::new(&ctx);
    let hold = saga
        .step(
            || reserve_committed(pool, workload, order, work),
            move |_| restock_committed(pool, workload, order),
        )
        .await?;
    saga.step(
        || backout_run(pool, workload, order, work, false),
        |_| async { Ok::<(), HarvestError>(()) },
    )
    .await?;
    Ok(hold)
}

/// Run one order under `arm`.
///
/// # Errors
///
/// Returns a database or pool error. A declined order is not an error.
pub async fn run_order(
    pool: &DbPool,
    workload: &Workload,
    arm: Atomicity,
    order: Order,
    step_work: Duration,
) -> HarvestResult<RunOutcome> {
    Ok(run_order_timed(pool, workload, arm, order, step_work)
        .await?
        .0)
}

/// Run one order under `arm`, and time its hot-lock hold.
///
/// The hold is the time from the hot write to the commit that releases the
/// hot row. Only a committed run reports it.
async fn run_order_timed(
    pool: &DbPool,
    workload: &Workload,
    arm: Atomicity,
    order: Order,
    step_work: Duration,
) -> HarvestResult<(RunOutcome, Option<Duration>)> {
    let result = match arm {
        Atomicity::Backout => backout_run(pool, workload, order, step_work, true).await,
        Atomicity::Saga => saga_run(pool, workload, order, step_work).await.map(Some),
        Atomicity::Hybrid => hybrid_run(pool, workload, order, step_work).await.map(Some),
    };
    match result {
        Ok(hold) => Ok((RunOutcome::Committed, hold)),
        Err(error) if is_declined(&error) => Ok((RunOutcome::RolledBack, None)),
        Err(error) => Err(error),
    }
}

/// One cell of the measurement.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CellConfig {
    /// The arm.
    pub arm: Atomicity,
    /// The contention level.
    pub contention: Contention,
    /// Concurrent clients. Each runs orders back to back.
    pub clients: usize,
    /// How long the clients run.
    pub duration: Duration,
    /// How long each step holds its transaction open.
    pub step_work: Duration,
    /// The share of orders that the place step declines.
    pub fail_rate: f64,
    /// The seed of the order draws.
    pub seed: u64,
}

impl CellConfig {
    /// Check the config before a run.
    ///
    /// # Errors
    ///
    /// Returns a config error for a `fail_rate` outside `[0, 1]` or zero
    /// clients.
    pub fn validate(&self) -> HarvestResult<()> {
        if !(0.0..=1.0).contains(&self.fail_rate) {
            return Err(HarvestError::Config(format!(
                "fail_rate must be in [0, 1], got {}",
                self.fail_rate
            )));
        }
        if self.clients == 0 {
            return Err(HarvestError::Config(
                "a cell needs one client or more".into(),
            ));
        }
        Ok(())
    }

    /// The number of runs that want the hottest SKU at once.
    ///
    /// The clients form a closed loop, and the SKU draw is uniform. Each
    /// client is always in a run, so this is clients over SKUs.
    #[must_use]
    pub const fn hot_key_concurrency(&self) -> f64 {
        #[allow(clippy::cast_precision_loss)] // a client or SKU count far below 2^53
        let ratio = self.clients as f64 / self.contention.skus() as f64;
        ratio
    }

    /// The rule inputs for this cell.
    ///
    /// The reserve step is first and hot. Backout holds its lock through the
    /// two later steps, so `hold_after_hot_step` is two step lengths.
    #[must_use]
    pub fn profile(&self, commit_latency: Duration) -> WorkloadProfile {
        WorkloadProfile {
            effects_outside_database: false,
            total_hold: self.step_work * STEPS,
            hot_key_concurrency: self.hot_key_concurrency(),
            hold_after_hot_step: self.step_work * (STEPS - 1),
            commit_latency,
            hot_step_commutative: true,
        }
    }
}

/// The measured result of one cell.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CellResult {
    /// The cell.
    pub config: CellConfig,
    /// Committed runs, the drain included.
    pub committed: u64,
    /// Runs that committed before the deadline. Goodput counts only these.
    pub committed_in_window: u64,
    /// Declined runs, fully undone.
    pub rolled_back: u64,
    /// Runs that ended in a database or pool error.
    pub errors: u64,
    /// Conflict retries of backout transactions.
    pub retries: u64,
    /// The time from the deadline to the end of the last run.
    pub drain: Duration,
    /// The median latency of a committed run.
    pub p50: Duration,
    /// The 90th percentile latency of a committed run.
    pub p90: Duration,
    /// The 90th percentile latency of a declined run, its undo included.
    pub declined_p90: Duration,
    /// The median hot-lock hold of a committed run: hot write to commit.
    pub hot_hold_p50: Duration,
    /// The invariant sums after the drain.
    pub totals: Totals,
}

impl CellResult {
    /// Runs that committed before the deadline, per second of the window.
    ///
    /// The window excludes the drain, so a slow arm gains no time from it.
    #[must_use]
    pub const fn goodput(&self) -> f64 {
        let seconds = self.config.duration.as_secs_f64();
        if seconds > 0.0 {
            #[allow(clippy::cast_precision_loss)] // a run count far below 2^53
            let committed = self.committed_in_window as f64;
            committed / seconds
        } else {
            0.0
        }
    }
}

#[derive(Default)]
struct ClientStats {
    committed: u64,
    committed_in_window: u64,
    rolled_back: u64,
    errors: u64,
    latencies: Vec<Duration>,
    declined_latencies: Vec<Duration>,
    hot_holds: Vec<Duration>,
}

impl ClientStats {
    fn merge(&mut self, other: Self) {
        self.committed += other.committed;
        self.committed_in_window += other.committed_in_window;
        self.rolled_back += other.rolled_back;
        self.errors += other.errors;
        self.latencies.extend(other.latencies);
        self.declined_latencies.extend(other.declined_latencies);
        self.hot_holds.extend(other.hot_holds);
    }
}

/// Run the clients of one cell until the deadline, then wait for them.
async fn drive(
    pool: &DbPool,
    workload: &Arc<Workload>,
    config: CellConfig,
) -> HarvestResult<(ClientStats, Duration)> {
    let skus = config.contention.skus();
    let start = Instant::now();
    let deadline = start + config.duration;
    let mut clients = tokio::task::JoinSet::new();
    for client in 0..config.clients {
        let pool = pool.clone();
        let workload = Arc::clone(workload);
        let seed = config.seed ^ (client as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        clients.spawn(async move {
            let mut rng = StdRng::seed_from_u64(seed);
            let mut stats = ClientStats::default();
            while Instant::now() < deadline {
                let order = Order {
                    sku: rng.gen_range(0..skus),
                    account: rng.gen_range(0..ACCOUNTS),
                    amount: rng.gen_range(1..=MAX_AMOUNT),
                    decline: rng.gen_bool(config.fail_rate),
                };
                let began = Instant::now();
                let outcome =
                    run_order_timed(&pool, &workload, config.arm, order, config.step_work).await;
                let ended = Instant::now();
                match outcome {
                    Ok((RunOutcome::Committed, hold)) => {
                        stats.committed += 1;
                        if ended <= deadline {
                            stats.committed_in_window += 1;
                        }
                        stats.latencies.push(ended - began);
                        stats.hot_holds.extend(hold);
                    }
                    Ok((RunOutcome::RolledBack, _)) => {
                        stats.rolled_back += 1;
                        stats.declined_latencies.push(ended - began);
                    }
                    Err(error) => {
                        tracing::warn!(%error, "atomicity spike run failed");
                        stats.errors += 1;
                    }
                }
            }
            stats
        });
    }

    let mut total = ClientStats::default();
    let mut join_error = None;
    while let Some(joined) = clients.join_next().await {
        match joined {
            Ok(stats) => total.merge(stats),
            Err(error) => join_error = Some(error),
        }
    }
    let drain = Instant::now().saturating_duration_since(deadline);
    join_error.map_or_else(
        || Ok((total, drain)),
        |error| {
            Err(HarvestError::Dispatch(format!(
                "atomicity client task: {error}"
            )))
        },
    )
}

/// Run one cell on fresh tables, then drop them.
///
/// `run_cell` keeps one connection from the setup to the drop. So an error
/// after the setup still drops the tables. A cancelled future does not.
/// The clients share the rest of the pool.
///
/// # Errors
///
/// Returns a config error from [`CellConfig::validate`]. Returns a database
/// or pool error from the setup, a client or the totals. A failed run counts
/// in [`CellResult::errors`].
pub async fn run_cell(pool: &DbPool, config: CellConfig) -> HarvestResult<CellResult> {
    config.validate()?;
    let mut conn = acquire_within_pool_bound(pool).await?;
    let workload = Arc::new(Workload::create(&mut conn, config.contention).await?);

    let driven = drive(pool, &workload, config).await;
    let totals = match driven {
        Ok(_) => workload.totals(&mut conn).await,
        Err(_) => Ok(Totals::default()),
    };
    workload.drop_tables(&mut conn).await?;
    let (mut total, drain) = driven?;
    let totals = totals?;

    total.latencies.sort_unstable();
    total.declined_latencies.sort_unstable();
    total.hot_holds.sort_unstable();
    Ok(CellResult {
        config,
        committed: total.committed,
        committed_in_window: total.committed_in_window,
        rolled_back: total.rolled_back,
        errors: total.errors,
        retries: workload.retries(),
        drain,
        p50: percentile(&total.latencies, 50.0),
        p90: percentile(&total.latencies, 90.0),
        declined_p90: percentile(&total.declined_latencies, 90.0),
        hot_hold_p50: percentile(&total.hot_holds, 50.0),
        totals,
    })
}

/// The median time of one small commit, over `samples` commits.
///
/// Each sample inserts one row into a logged table and commits, so it pays
/// the WAL flush that a real step pays.
///
/// # Errors
///
/// Returns a database error. The probe drops its table on every path.
pub async fn commit_latency(
    conn: &mut AsyncPgConnection,
    samples: usize,
) -> HarvestResult<Duration> {
    let table = format!("atomicity_probe_{}", uuid::Uuid::new_v4().simple());
    conn.batch_execute(&format!("CREATE TABLE {table} (id BIGSERIAL PRIMARY KEY)"))
        .await?;
    let insert = format!("INSERT INTO {table} DEFAULT VALUES");
    let mut times = Vec::with_capacity(samples.max(1));
    let mut probe = Ok(());
    for _ in 0..samples.max(1) {
        let began = Instant::now();
        let sql = insert.as_str();
        probe = Box::pin(conn.transaction::<(), HarvestError, _>(async |tx| {
            tx.batch_execute(sql).await?;
            Ok(())
        }))
        .await;
        if probe.is_err() {
            break;
        }
        times.push(began.elapsed());
    }
    conn.batch_execute(&format!("DROP TABLE {table}")).await?;
    probe?;
    times.sort_unstable();
    Ok(percentile(&times, 50.0))
}

/// The pre-registered matrix: each contention level, each step length, each arm.
#[must_use]
pub fn matrix(clients: usize, duration: Duration, seed: u64) -> Vec<CellConfig> {
    let mut cells = Vec::new();
    for contention in [Contention::Low, Contention::High] {
        for step_work in [Duration::ZERO, Duration::from_millis(20)] {
            for arm in Atomicity::ALL {
                cells.push(CellConfig {
                    arm,
                    contention,
                    clients,
                    duration,
                    step_work,
                    fail_rate: FAIL_RATE,
                    seed,
                });
            }
        }
    }
    cells
}

/// The `p`th percentile of `sorted`, by the nearest-rank method.
///
/// Returns zero for an empty slice.
#[must_use]
pub fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    // The rank is in [0, len] for p in [0, 100], and the clamp covers the rest.
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn config(contention: Contention) -> CellConfig {
        CellConfig {
            arm: Atomicity::Backout,
            contention,
            clients: 16,
            duration: Duration::from_secs(10),
            step_work: Duration::from_millis(20),
            fail_rate: 0.1,
            seed: 7,
        }
    }

    #[test]
    fn validate_rejects_a_bad_fail_rate_or_no_clients() {
        assert!(config(Contention::Low).validate().is_ok());
        for fail_rate in [f64::NAN, -0.1, 1.5] {
            let bad = CellConfig {
                fail_rate,
                ..config(Contention::Low)
            };
            assert!(
                matches!(bad.validate(), Err(HarvestError::Config(_))),
                "{fail_rate}"
            );
        }
        let idle = CellConfig {
            clients: 0,
            ..config(Contention::Low)
        };
        assert!(matches!(idle.validate(), Err(HarvestError::Config(_))));
    }

    #[test]
    fn hot_key_concurrency_is_clients_over_skus() {
        assert!((config(Contention::Low).hot_key_concurrency() - 0.016).abs() < 1e-9);
        assert!((config(Contention::High).hot_key_concurrency() - 16.0).abs() < 1e-9);
    }

    #[test]
    fn the_profile_maps_the_cell_to_the_rule_inputs() {
        let commit = Duration::from_millis(2);
        let profile = config(Contention::High).profile(commit);
        assert_eq!(
            profile,
            WorkloadProfile {
                effects_outside_database: false,
                total_hold: Duration::from_millis(60),
                hot_key_concurrency: 16.0,
                hold_after_hot_step: Duration::from_millis(40),
                commit_latency: commit,
                hot_step_commutative: true,
            }
        );
    }

    #[test]
    fn the_matrix_is_two_levels_by_two_lengths_by_three_arms() {
        let cells = matrix(16, Duration::from_secs(10), 1);
        assert_eq!(cells.len(), 12);
        for contention in [Contention::Low, Contention::High] {
            for work in [Duration::ZERO, Duration::from_millis(20)] {
                for arm in Atomicity::ALL {
                    assert!(
                        cells.iter().any(|cell| cell.contention == contention
                            && cell.step_work == work
                            && cell.arm == arm
                            && cell.clients == 16
                            && (cell.fail_rate - FAIL_RATE).abs() < 1e-9),
                        "missing {contention:?} {work:?} {arm:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn percentile_uses_the_nearest_rank() {
        let values: Vec<Duration> = (1..=10).map(Duration::from_millis).collect();
        assert_eq!(percentile(&values, 50.0), Duration::from_millis(5));
        assert_eq!(percentile(&values, 90.0), Duration::from_millis(9));
        assert_eq!(percentile(&values, 100.0), Duration::from_millis(10));
        assert_eq!(percentile(&[], 50.0), Duration::ZERO);
    }

    #[test]
    fn goodput_counts_only_runs_inside_the_window() {
        let result = CellResult {
            config: config(Contention::Low),
            committed: 520,
            committed_in_window: 500,
            rolled_back: 50,
            errors: 0,
            retries: 0,
            drain: Duration::from_millis(80),
            p50: Duration::ZERO,
            p90: Duration::ZERO,
            declined_p90: Duration::ZERO,
            hot_hold_p50: Duration::ZERO,
            totals: Totals::default(),
        };
        assert!(
            (result.goodput() - 50.0).abs() < 1e-9,
            "500 runs in the 10 s window; the drain does not count"
        );
    }

    #[test]
    fn totals_are_consistent_only_when_both_sums_match() {
        let good = Totals {
            stock_taken: 3,
            orders: 3,
            money_taken: 300,
            order_total: 300,
            history_rows: 9,
        };
        assert!(good.consistent());
        assert!(
            !Totals {
                stock_taken: 4,
                ..good
            }
            .consistent()
        );
        assert!(
            !Totals {
                money_taken: 200,
                ..good
            }
            .consistent()
        );
    }
}
