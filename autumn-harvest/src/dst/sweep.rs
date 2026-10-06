//! Seed sweeps, the run-twice determinism check, and local replay.

use std::fmt;
use std::ops::RangeInclusive;

use super::invariant::Invariant;
use super::sim::{SimConfig, SimReport, SimStats, run};
use super::store::Fencing;

/// The one seed to run. It overrides [`SEEDS_VAR`] and [`SEED_BASE_VAR`].
pub const SEED_VAR: &str = "HARVEST_DST_SEED";
/// The number of seeds in a sweep.
pub const SEEDS_VAR: &str = "HARVEST_DST_SEEDS";
/// The first seed of a sweep.
pub const SEED_BASE_VAR: &str = "HARVEST_DST_SEED_BASE";
/// The owner-write guard: `claim-epoch` (the default) or `state-only`.
pub const FENCING_VAR: &str = "HARVEST_DST_FENCING";
/// A comma list of invariant names to check. The default is all of them.
pub const CHECKS_VAR: &str = "HARVEST_DST_CHECKS";

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
    run_twice_with(config, run)
}

/// [`run_twice`] with `runner` in place of [`run`].
///
/// A test passes a runner that changes its output to prove the check.
///
/// # Errors
///
/// Returns [`Nondeterminism`] when the two runs differ.
pub fn run_twice_with(
    config: &SimConfig,
    mut runner: impl FnMut(&SimConfig) -> SimReport,
) -> Result<SimReport, Nondeterminism> {
    let first = runner(config);
    let second = runner(config);
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
    /// Returns a message when a value is not a decimal `u64`, when the plan
    /// has no seeds, or when its last seed is past `u64::MAX`. A plan that
    /// runs no seed would pass with no test.
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
        let plan = Self {
            first: number(SEED_BASE_VAR, base)?.unwrap_or(0),
            count: number(SEEDS_VAR, seeds)?.unwrap_or(default_count),
        };
        if plan.count == 0 {
            return Err(format!("{SEEDS_VAR} is 0, so the sweep would run no seed"));
        }
        if plan.first.checked_add(plan.count - 1).is_none() {
            return Err(format!(
                "{SEED_BASE_VAR}={} with {SEEDS_VAR}={} ends past u64::MAX",
                plan.first, plan.count
            ));
        }
        Ok(plan)
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

    /// The seeds, in order. The range is empty when `count` is 0.
    #[must_use]
    pub const fn seeds(&self) -> RangeInclusive<u64> {
        if self.count == 0 {
            return RangeInclusive::new(1, 0);
        }
        self.first..=self.first.saturating_add(self.count - 1)
    }
}

/// The config for `seed` with the values of [`FENCING_VAR`] and
/// [`CHECKS_VAR`]. A missing value keeps the default of [`SimConfig::new`].
///
/// # Errors
///
/// Returns a message when a value is not a known name.
pub fn config_from_vars(
    seed: u64,
    fencing: Option<&str>,
    checks: Option<&str>,
) -> Result<SimConfig, String> {
    let mut config = SimConfig::new(seed);
    if let Some(name) = fencing {
        config.fencing = Fencing::parse(name.trim())?;
    }
    if let Some(list) = checks {
        config.checks = list
            .split(',')
            .map(|name| Invariant::parse(name.trim()))
            .collect::<Result<_, _>>()?;
    }
    Ok(config)
}

/// [`config_from_vars`] with the values from the environment.
///
/// # Errors
///
/// Returns a message when a value is not a known name.
pub fn config_from_env(seed: u64) -> Result<SimConfig, String> {
    let fencing = std::env::var(FENCING_VAR).ok();
    let checks = std::env::var(CHECKS_VAR).ok();
    config_from_vars(seed, fencing.as_deref(), checks.as_deref())
}

/// The shell command that replays `config` locally.
///
/// It sets every variable that [`config_from_env`] reads, so a stale
/// variable in the shell cannot change the replay.
#[must_use]
pub fn repro_command(config: &SimConfig) -> String {
    let checks: Vec<&str> = config.checks.iter().map(|i| i.name()).collect();
    format!(
        "{SEED_VAR}={} {FENCING_VAR}={} {CHECKS_VAR}={} cargo test -p autumn-harvest \
         --no-default-features --test dst replay_one_seed -- --nocapture",
        config.seed,
        config.fencing.as_str(),
        checks.join(",")
    )
}

/// A seed that failed a sweep.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweepFailure {
    /// The config of the failed run.
    pub config: SimConfig,
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
            self.config.seed,
            self.reason,
            repro_command(&self.config),
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
) -> Result<SweepSummary, Box<SweepFailure>> {
    let mut summary = SweepSummary::default();
    for seed in plan.seeds() {
        let config = config(seed);
        let report = match run_twice(&config) {
            Ok(report) => report,
            Err(error) => {
                return Err(Box::new(SweepFailure {
                    config,
                    reason: error.to_string(),
                    trace_tail: String::new(),
                }));
            }
        };
        if let Some(violation) = &report.violation {
            return Err(Box::new(SweepFailure {
                config,
                reason: violation.to_string(),
                trace_tail: report.trace_tail(TAIL_LINES),
            }));
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
        let config = SimConfig::new(42).with_fencing(Fencing::StateOnly);
        let command = repro_command(&config);
        assert!(command.starts_with("HARVEST_DST_SEED=42 HARVEST_DST_FENCING=state-only "));
        assert!(command.contains("--test dst replay_one_seed"), "{command}");
        assert!(
            command.contains("HARVEST_DST_CHECKS=AtMostOneTerminal,"),
            "all checks are named: {command}"
        );
        assert!(!command.contains('\\'), "one line: {command}");
    }

    #[test]
    fn repro_command_names_a_subset_of_checks() {
        let checks = [
            Invariant::TerminalByCurrentClaim,
            Invariant::AtMostOneTerminal,
        ];
        let config = SimConfig::new(3).checking(&checks);
        let command = repro_command(&config);
        assert!(
            command.contains("HARVEST_DST_CHECKS=TerminalByCurrentClaim,AtMostOneTerminal "),
            "{command}"
        );
    }

    #[test]
    fn config_from_vars_round_trips_the_repro_command() {
        let config = SimConfig::new(9)
            .with_fencing(Fencing::StateOnly)
            .checking(&[Invariant::HeartbeatByCurrentClaim]);
        let parsed = config_from_vars(9, Some("state-only"), Some("HeartbeatByCurrentClaim"))
            .expect("valid");
        assert_eq!(parsed, config);
        assert_eq!(config_from_vars(9, None, None), Ok(SimConfig::new(9)));
        assert!(config_from_vars(9, None, Some("NoSuchInvariant")).is_err());
        assert!(config_from_vars(9, Some("none"), None).is_err());
    }

    #[test]
    fn the_top_seed_runs() {
        let plan = SeedPlan::parse(Some("18446744073709551615"), None, None, 5).expect("valid");
        assert_eq!(plan.seeds().collect::<Vec<_>>(), [u64::MAX]);
        let top = SeedPlan::parse(None, Some("2"), Some("18446744073709551614"), 5).expect("valid");
        assert_eq!(top.seeds().count(), 2);
    }

    #[test]
    fn empty_and_overflowing_plans_are_rejected() {
        assert!(SeedPlan::parse(None, Some("0"), None, 5).is_err());
        assert!(SeedPlan::parse(None, None, None, 0).is_err());
        assert!(SeedPlan::parse(None, Some("3"), Some("18446744073709551614"), 5).is_err());
        let empty = SeedPlan { first: 4, count: 0 };
        assert_eq!(empty.seeds().count(), 0);
    }

    #[test]
    fn run_twice_reports_a_runner_that_changes_its_trace() {
        let config = SimConfig::new(1);
        let mut calls = 0;
        let flaky = |config: &SimConfig| {
            calls += 1;
            let mut report = run(config);
            if calls == 2 {
                report.trace[5].push_str(" (changed)");
            }
            report
        };
        let error = run_twice_with(&config, flaky).expect_err("the traces differ");
        assert_eq!((error.seed, error.line), (1, 5));
        assert!(error.second.ends_with("(changed)"), "{error}");
    }

    #[test]
    fn run_twice_reports_a_change_outside_the_trace() {
        let config = SimConfig::new(1);
        let mut calls = 0;
        let flaky = |config: &SimConfig| {
            calls += 1;
            let mut report = run(config);
            if calls == 2 {
                report.stats.claims += 1;
            }
            report
        };
        assert!(run_twice_with(&config, flaky).is_err());
    }
}
