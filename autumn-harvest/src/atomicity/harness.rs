//! The measurement harness: one order workflow, three arms (issue #2012).

use std::time::Duration;

use diesel_async::AsyncPgConnection;

use super::rule::{Atomicity, WorkloadProfile};
use crate::error::{HarvestError, HarvestResult};
use crate::worker::DbPool;

/// The stock of each SKU at the start of a cell.
pub const INITIAL_QTY: i64 = 1_000_000_000;
/// The balance of each account at the start of a cell.
pub const INITIAL_BALANCE: i64 = 1_000_000_000_000;
/// The number of accounts.
pub const ACCOUNTS: i64 = 1_000;
/// The reason that a declined order carries.
pub const DECLINED: &str = "order declined";

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
    /// The order was declined, and every effect is undone.
    RolledBack,
}

/// The tables of one cell. Each cell gets fresh tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Workload {
    prefix: String,
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

impl Workload {
    /// Create and fill fresh tables for `contention`.
    ///
    /// # Errors
    ///
    /// Returns a database error.
    pub async fn create(conn: &mut AsyncPgConnection, contention: Contention) -> HarvestResult<Self> {
        let _ = (conn, contention);
        Err(HarvestError::Config("not implemented".into()))
    }

    /// Drop the tables.
    ///
    /// # Errors
    ///
    /// Returns a database error.
    pub async fn drop_tables(&self, conn: &mut AsyncPgConnection) -> HarvestResult<()> {
        let _ = conn;
        Err(HarvestError::Config("not implemented".into()))
    }

    /// Read the invariant sums.
    ///
    /// # Errors
    ///
    /// Returns a database error.
    pub async fn totals(&self, conn: &mut AsyncPgConnection) -> HarvestResult<Totals> {
        let _ = conn;
        Err(HarvestError::Config("not implemented".into()))
    }
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
    let _ = (pool, workload, arm, order, step_work);
    Err(HarvestError::Config("not implemented".into()))
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
    /// The number of runs that want the hottest SKU at once.
    #[must_use]
    pub fn hot_key_concurrency(&self) -> f64 {
        0.0
    }

    /// The rule inputs for this cell.
    #[must_use]
    pub fn profile(&self, commit_latency: Duration) -> WorkloadProfile {
        let _ = commit_latency;
        WorkloadProfile {
            effects_outside_database: true,
            total_hold: Duration::ZERO,
            hot_key_concurrency: 0.0,
            hold_after_hot_step: Duration::ZERO,
            commit_latency: Duration::ZERO,
            hot_step_commutative: false,
        }
    }
}

/// The measured result of one cell.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CellResult {
    /// The cell.
    pub config: CellConfig,
    /// Committed runs.
    pub committed: u64,
    /// Declined runs, fully undone.
    pub rolled_back: u64,
    /// Runs that ended in a database or pool error.
    pub errors: u64,
    /// The wall time of the cell.
    pub elapsed: Duration,
    /// The median latency of a committed run.
    pub p50: Duration,
    /// The 90th percentile latency of a committed run.
    pub p90: Duration,
    /// The invariant sums after the drain.
    pub totals: Totals,
}

impl CellResult {
    /// Committed runs per second.
    #[must_use]
    pub fn goodput(&self) -> f64 {
        0.0
    }
}

/// Run one cell on fresh tables, then drop them.
///
/// # Errors
///
/// Returns a database or pool error from the setup or the totals.
pub async fn run_cell(pool: &DbPool, config: CellConfig) -> HarvestResult<CellResult> {
    let _ = (pool, config);
    Err(HarvestError::Config("not implemented".into()))
}

/// The median time of one small commit, over `samples` commits.
///
/// # Errors
///
/// Returns a database error.
pub async fn commit_latency(conn: &mut AsyncPgConnection, samples: usize) -> HarvestResult<Duration> {
    let _ = (conn, samples);
    Err(HarvestError::Config("not implemented".into()))
}

/// The pre-registered matrix: each contention level, each step length, each arm.
#[must_use]
pub fn matrix(clients: usize, duration: Duration, seed: u64) -> Vec<CellConfig> {
    let _ = (clients, duration, seed);
    Vec::new()
}

/// The `p`th percentile of sorted `values`, by the nearest-rank method.
#[must_use]
pub fn percentile(sorted: &[Duration], p: f64) -> Duration {
    let _ = (sorted, p);
    Duration::ZERO
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(contention: Contention) -> CellConfig {
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
                            && (cell.fail_rate - 0.1).abs() < 1e-9),
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
    fn goodput_is_committed_runs_per_second() {
        let result = CellResult {
            config: config(Contention::Low),
            committed: 500,
            rolled_back: 50,
            errors: 0,
            elapsed: Duration::from_secs(2),
            p50: Duration::ZERO,
            p90: Duration::ZERO,
            totals: Totals::default(),
        };
        assert!((result.goodput() - 250.0).abs() < 1e-9);
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
        assert!(!Totals { stock_taken: 4, ..good }.consistent());
        assert!(!Totals { money_taken: 200, ..good }.consistent());
    }
}
