//! Seed sweeps, the run-twice determinism check, and local replay.

use std::fmt;
use std::ops::Range;

use super::sim::{SimConfig, SimReport, SimStats, run};
use super::store::Fencing;

/// Run one seed, the default store, and only this seed.
pub const SEED_VAR: &str = "HARVEST_DST_SEED";
/// The number of seeds in a sweep.
pub const SEEDS_VAR: &str = "HARVEST_DST_SEEDS";
/// The first seed of a sweep.
pub const SEED_BASE_VAR: &str = "HARVEST_DST_SEED_BASE";
/// The owner-write guard: `claim-epoch` (the default) or `state-only`.
pub const FENCING_VAR: &str = "HARVEST_DST_FENCING";

/// Two runs of one seed gave different traces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Nondeterminism {
    /// The seed.
    pub seed: u64,
    /// The index of the first trace line that differs.
    pub line: usize,
    /// That line in the first run, or empty when the run ended first.
    pub first: String,
    /// That line in the second run, or empty when the run ended first.
    pub second: String,
}

impl fmt::Display for Nondeterminism {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "seed {} is not deterministic: line {} differs\n  run 1: {}\n  run 2: {}",
            self.seed, self.line, self.first, self.second
        )
    }
}

/// The index of the first line where `a` and `b` differ.
///
/// Returns `None` when they are equal.
#[must_use]
pub fn first_divergence(a: &[String], b: &[String]) -> Option<usize> {
    let _ = (a, b);
    todo!("issue #1830")
}

/// Run `config` twice and compare the traces and the operation logs.
///
/// # Errors
///
/// Returns [`Nondeterminism`] when the two runs differ.
pub fn run_twice(config: &SimConfig) -> Result<SimReport, Nondeterminism> {
    let _ = config;
    todo!("issue #1830")
}

/// The seeds of a sweep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeedPlan {
    /// The first seed.
    pub first: u64,
    /// The number of seeds.
    pub count: u64,
}

impl SeedPlan {
    /// Build a plan from the values of the three seed variables.
    ///
    /// `seed` gives a plan of that one seed. Otherwise the plan starts at
    /// `base` (default 0) and has `seeds` seeds (default `default_count`).
    ///
    /// # Errors
    ///
    /// Returns a message when a value is not a decimal `u64`.
    pub fn parse(
        seed: Option<&str>,
        seeds: Option<&str>,
        base: Option<&str>,
        default_count: u64,
    ) -> Result<Self, String> {
        let _ = (seed, seeds, base, default_count);
        todo!("issue #1830")
    }

    /// Build a plan from [`SEED_VAR`], [`SEEDS_VAR`] and [`SEED_BASE_VAR`].
    ///
    /// # Errors
    ///
    /// Returns a message when a value is not a decimal `u64`.
    pub fn from_env(default_count: u64) -> Result<Self, String> {
        let seed = std::env::var(SEED_VAR).ok();
        let seeds = std::env::var(SEEDS_VAR).ok();
        let base = std::env::var(SEED_BASE_VAR).ok();
        Self::parse(
            seed.as_deref(),
            seeds.as_deref(),
            base.as_deref(),
            default_count,
        )
    }

    /// The seeds, in order.
    #[must_use]
    pub const fn seeds(&self) -> Range<u64> {
        self.first..self.first.saturating_add(self.count)
    }
}

/// The fencing that [`FENCING_VAR`] names, or [`Fencing::ClaimEpoch`].
///
/// # Errors
///
/// Returns a message when the value is not a known name.
pub fn fencing_from_env() -> Result<Fencing, String> {
    std::env::var(FENCING_VAR).map_or(Ok(Fencing::ClaimEpoch), |name| Fencing::parse(&name))
}

/// The shell command that replays `seed` locally.
#[must_use]
pub fn repro_command(seed: u64, fencing: Fencing) -> String {
    let _ = (seed, fencing);
    todo!("issue #1830")
}

/// A seed that failed a sweep.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweepFailure {
    /// The seed.
    pub seed: u64,
    /// The fencing of the run.
    pub fencing: Fencing,
    /// The failed invariant or the determinism error.
    pub reason: String,
    /// The last trace lines of the run.
    pub trace_tail: String,
}

impl fmt::Display for SweepFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "seed {} failed: {}\nreproduce: {}\nlast steps:\n{}",
            self.seed,
            self.reason,
            repro_command(self.seed, self.fencing),
            self.trace_tail
        )
    }
}

/// The result of a sweep with no failure.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepSummary {
    /// The number of seeds that ran.
    pub seeds: u64,
    /// The merged coverage counters.
    pub stats: SimStats,
}

/// The number of trace lines that a failure prints.
pub const TAIL_LINES: usize = 40;

/// Run every seed of `plan` twice, with the config that `config` builds.
///
/// # Errors
///
/// Returns the first seed that breaks an invariant or is not deterministic.
pub fn sweep(
    plan: &SeedPlan,
    config: impl Fn(u64) -> SimConfig,
) -> Result<SweepSummary, SweepFailure> {
    let _ = (plan, config, run);
    todo!("issue #1830")
}
