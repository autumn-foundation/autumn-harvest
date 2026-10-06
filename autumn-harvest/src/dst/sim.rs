//! The seeded simulation of workers and the orphan reclaimer.

use super::invariant::{Invariant, Violation};
use super::store::{ClaimStore, Fencing, Op, OracleStore, Outcome, Row};

/// The parameters of one simulated run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimConfig {
    /// The seed. Equal configs give equal runs.
    pub seed: u64,
    /// The owner-write guard of the store.
    pub fencing: Fencing,
    /// The number of worker processes.
    pub workers: usize,
    /// The activity slots per worker.
    pub slots: usize,
    /// The number of activity tasks.
    pub tasks: usize,
    /// The step limit.
    pub max_steps: usize,
    /// A worker whose last beat is this old is dead to the reclaimer.
    pub stale_after_ms: u64,
    /// The liveness beat period of a running worker.
    pub beat_every_ms: u64,
    /// The orphan scan period.
    pub scan_every_ms: u64,
    /// The largest clock advance per step.
    pub max_tick_ms: u64,
    /// The invariants to check.
    pub checks: Vec<Invariant>,
}

impl SimConfig {
    /// The default config for `seed`: 3 workers, 2 slots each, 3 tasks.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            fencing: Fencing::ClaimEpoch,
            workers: 3,
            slots: 2,
            tasks: 3,
            max_steps: 400,
            stale_after_ms: 10_000,
            beat_every_ms: 2_000,
            scan_every_ms: 3_000,
            max_tick_ms: 1_000,
            checks: Invariant::ALL.to_vec(),
        }
    }

    /// This config with `fencing`.
    #[must_use]
    pub const fn with_fencing(mut self, fencing: Fencing) -> Self {
        self.fencing = fencing;
        self
    }

    /// This config, checking only `checks`.
    #[must_use]
    pub fn checking(mut self, checks: &[Invariant]) -> Self {
        self.checks = checks.to_vec();
        self
    }

    /// A fresh oracle store for this config.
    #[must_use]
    pub fn oracle(&self) -> OracleStore {
        OracleStore::new(self.tasks, self.fencing, self.stale_after_ms)
    }
}

/// One store operation of a run, its outcome, and the rows after it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepRecord {
    /// The step index. It matches the trace line.
    pub step: usize,
    /// The operation.
    pub op: Op,
    /// The outcome that the store returned.
    pub outcome: Outcome,
    /// Every task row after the operation.
    pub rows: Vec<Row>,
}

/// Counters that show what a run covered.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SimStats {
    /// Claims that took a row.
    pub claims: u64,
    /// Start fences that took effect.
    pub starts: u64,
    /// Heartbeats that took effect.
    pub heartbeats: u64,
    /// Terminal writes that took effect.
    pub completes: u64,
    /// Owner writes that a stale claim lost.
    pub stale_rejected: u64,
    /// Terminal writes that a stale claim lost.
    pub stale_completes_rejected: u64,
    /// Orphan requeues that moved a row.
    pub reclaims: u64,
    /// Injected stalls.
    pub stalls: u64,
    /// Injected crashes.
    pub crashes: u64,
    /// The most distinct workers that claimed a row in one run.
    pub claimers: usize,
}

impl SimStats {
    /// Add the counters of `other`. `claimers` keeps the larger value.
    pub fn merge(&mut self, other: &Self) {
        self.claims += other.claims;
        self.starts += other.starts;
        self.heartbeats += other.heartbeats;
        self.completes += other.completes;
        self.stale_rejected += other.stale_rejected;
        self.stale_completes_rejected += other.stale_completes_rejected;
        self.reclaims += other.reclaims;
        self.stalls += other.stalls;
        self.crashes += other.crashes;
        self.claimers = self.claimers.max(other.claimers);
    }
}

/// The result of one run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimReport {
    /// The config of the run.
    pub config: SimConfig,
    /// Every store operation, in order.
    pub steps: Vec<StepRecord>,
    /// One line per step, faults included.
    pub trace: Vec<String>,
    /// The first failed invariant, if any. The run stops there.
    pub violation: Option<Violation>,
    /// Coverage counters.
    pub stats: SimStats,
}

impl SimReport {
    /// The last `lines` trace lines, joined by newlines.
    #[must_use]
    pub fn trace_tail(&self, lines: usize) -> String {
        let start = self.trace.len().saturating_sub(lines);
        self.trace[start..].join("\n")
    }
}

/// Run `config` against the oracle store.
#[must_use]
pub fn run(config: &SimConfig) -> SimReport {
    run_with(config, config.oracle())
}

/// Run `config` against `store`.
pub fn run_with<S: ClaimStore>(config: &SimConfig, store: S) -> SimReport {
    let _ = (config, store);
    todo!("issue #1830")
}
