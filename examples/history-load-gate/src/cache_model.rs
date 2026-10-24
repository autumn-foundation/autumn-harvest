//! The cold share that the entry cap of `WorkflowCache` alone causes.
//!
//! Under owner mode, an owner keeps the runs of its partitions until a
//! restart or a change of owner. A decision then replays cold only after an
//! eviction. This model drives the real `WorkflowCache` as the worker does:
//! a decision takes the entry of its run, and the commit puts it back. It
//! counts the decisions that find no entry. For a given access pattern,
//! restarts and owner changes only add cold decisions. A skewed load misses
//! less than a uniform one.

use autumn_harvest::cache::{CachedWorkflowState, WorkflowCache};
use uuid::Uuid;

/// The order in which the live runs of one owner decide.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pattern {
    /// Each run decides in turn: run 0, 1, …, n − 1, then run 0 again.
    RoundRobin,
    /// Each decision picks a run at random, with equal odds.
    Uniform,
}

/// The cold share of `decisions` decisions over `live_runs` runs and a
/// cache of `capacity` entries.
///
/// The first decision of each run is always cold. The model counts only
/// the decisions after one warm-up pass over every run. Returns 0 when
/// `decisions` is 0.
#[must_use]
pub fn cold_share(
    live_runs: usize,
    capacity: usize,
    decisions: usize,
    pattern: Pattern,
    seed: u64,
) -> f64 {
    if decisions == 0 || live_runs == 0 {
        return 0.0;
    }
    let runs: Vec<Uuid> = (0..live_runs)
        .map(|index| Uuid::from_u128(index as u128 + 1))
        .collect();
    let mut cache = WorkflowCache::new(capacity);
    let mut rng = XorShift(seed | 1);
    let mut decide = |run: Uuid| {
        let hit = cache.remove(&run).is_some();
        cache.insert(
            run,
            CachedWorkflowState {
                events: Vec::new(),
                next_event_id: 0,
            },
        );
        hit
    };
    for run in &runs {
        decide(*run);
    }
    let mut cold = 0_usize;
    for step in 0..decisions {
        let index = match pattern {
            Pattern::RoundRobin => step % live_runs,
            Pattern::Uniform => rng.below(live_runs),
        };
        if !decide(runs[index]) {
            cold += 1;
        }
    }
    #[allow(
        clippy::cast_precision_loss,
        reason = "decision counts here are far below 2^53"
    )]
    let share = cold as f64 / decisions as f64;
    share
}

/// A small seeded generator. The model needs repeatable draws, not strong
/// ones.
struct XorShift(u64);

impl XorShift {
    /// A draw in `0..bound`. `bound` is above zero.
    fn below(&mut self, bound: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        let bound = u64::try_from(bound).unwrap_or(u64::MAX);
        usize::try_from(self.0 % bound).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runs_that_fit_the_cache_never_replay_cold() {
        assert!(cold_share(1_000, 1_000, 20_000, Pattern::RoundRobin, 1).abs() < f64::EPSILON);
        assert!(cold_share(500, 1_000, 20_000, Pattern::Uniform, 1).abs() < f64::EPSILON);
    }

    #[test]
    fn one_run_past_the_cap_thrashes_a_round_robin() {
        // LRU evicts each run just before its next decision.
        let share = cold_share(1_001, 1_000, 20_000, Pattern::RoundRobin, 1);
        assert!((share - 1.0).abs() < f64::EPSILON, "{share}");
    }

    #[test]
    fn a_uniform_load_misses_about_one_minus_capacity_over_runs() {
        let share = cold_share(2_000, 1_000, 200_000, Pattern::Uniform, 7);
        assert!((share - 0.5).abs() < 0.02, "{share}");
    }

    #[test]
    fn no_decisions_give_a_zero_share() {
        assert!(cold_share(10, 5, 0, Pattern::Uniform, 1).abs() < f64::EPSILON);
    }

    #[test]
    fn the_same_seed_gives_the_same_share() {
        let a = cold_share(3_000, 1_000, 30_000, Pattern::Uniform, 42);
        let b = cold_share(3_000, 1_000, 30_000, Pattern::Uniform, 42);
        assert!((a - b).abs() < f64::EPSILON);
    }
}
