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
    a.iter()
        .zip(b)
        .position(|(x, y)| x != y)
        .or_else(|| (a.len() != b.len()).then(|| a.len().min(b.len())))
}

/// Run `config` twice and compare the traces and the operation logs.
///
/// # Errors
///
/// Returns [`Nondeterminism`] when the two runs differ.
pub fn run_twice(config: &SimConfig) -> Result<SimReport, Nondeterminism> {
    let first = run(config);
    let second = run(config);
    if let Some(line) = first_divergence(&first.trace, &second.trace) {
        let at = |trace: &[String]| trace.get(line).cloned().unwrap_or_default();
        return Err(Nondeterminism {
            seed: config.seed,
            line,
            first: at(&first.trace),
            second: at(&second.trace),
        });
    }
    if first != second {
        return Err(Nondeterminism {
            seed: config.seed,
            line: first.trace.len(),
            first: "equal trace, different operation log or stats".to_string(),
            second: String::new(),
        });
    }
    Ok(first)
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
        let number = |name: &str, value: Option<&str>| -> Result<Option<u64>, String> {
            value
                .map(|text| {
                    text.trim()
                        .parse::<u64>()
                        .map_err(|error| format!("{name}={text:?} is not a decimal u64: {error}"))
                })
                .transpose()
        };
        if let Some(seed) = number(SEED_VAR, seed)? {
            return Ok(Self {
                first: seed,
                count: 1,
            });
        }
        Ok(Self {
            first: number(SEED_BASE_VAR, base)?.unwrap_or(0),
            count: number(SEEDS_VAR, seeds)?.unwrap_or(default_count),
        })
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
    format!(
        "{SEED_VAR}={seed} {FENCING_VAR}={} cargo test -p autumn-harvest \
         --no-default-features --test dst replay_one_seed -- --nocapture",
        fencing.as_str()
    )
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
    let mut summary = SweepSummary::default();
    for seed in plan.seeds() {
        let config = config(seed);
        let fencing = config.fencing;
        let report = run_twice(&config).map_err(|error| SweepFailure {
            seed,
            fencing,
            reason: error.to_string(),
            trace_tail: String::new(),
        })?;
        if let Some(violation) = &report.violation {
            return Err(SweepFailure {
                seed,
                fencing,
                reason: violation.to_string(),
                trace_tail: report.trace_tail(TAIL_LINES),
            });
        }
        summary.seeds += 1;
        summary.stats.merge(&report.stats);
    }
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repro_command_names_the_seed_the_fencing_and_the_test() {
        let command = repro_command(42, Fencing::StateOnly);
        assert!(command.starts_with("HARVEST_DST_SEED=42 HARVEST_DST_FENCING=state-only "));
        assert!(command.contains("--test dst replay_one_seed"), "{command}");
        assert!(!command.contains('\\'), "one line: {command}");
    }

    #[test]
    fn seeds_saturate_at_the_top_of_the_range() {
        let plan = SeedPlan {
            first: u64::MAX,
            count: 5,
        };
        assert_eq!(plan.seeds().count(), 0);
    }
}
