//! The selection rule: physical backout, saga or hybrid (issue #2012).
//!
//! The rule is a sketch. It takes model inputs, not live metrics. Section
//! 2.3 of `DESIGN-2012.md` fixed it before the measurement.

use std::time::Duration;

/// The longest hold that backout accepts.
///
/// `run_transactional` asks for a closure under 5 s. A longer transaction
/// holds a pool connection and blocks vacuum.
pub const MAX_HOLD: Duration = Duration::from_secs(5);

/// Below this many concurrent runs on the hottest key, the key counts as cold.
pub const COLD_KEY_CONCURRENCY: f64 = 1.0;

/// How a workflow undoes a failed run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Atomicity {
    /// One transaction, one savepoint per step. A failure rolls back.
    Backout,
    /// One transaction per step. A failure runs compensations.
    Saga,
    /// The hot step commits alone as an escrow step. The rest backs out.
    Hybrid,
}

impl Atomicity {
    /// Every arm, in report order.
    pub const ALL: [Self; 3] = [Self::Backout, Self::Saga, Self::Hybrid];

    /// The report label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Backout => "backout",
            Self::Saga => "saga",
            Self::Hybrid => "hybrid",
        }
    }
}

/// Why the rule picked an arm. Each variant is one rule branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    /// A step writes outside the database. A rollback cannot undo it.
    ExternalEffects,
    /// The steps take longer than [`MAX_HOLD`].
    LongHold,
    /// Fewer than [`COLD_KEY_CONCURRENCY`] runs want the hottest key at once.
    ColdKey,
    /// Backout holds the hot lock for less extra time than one commit takes.
    CheapHold,
    /// The hot step is a bounded add or subtract, so it can commit early.
    CommutativeHotStep,
    /// No other branch matched.
    Fallback,
}

/// The rule inputs for one workflow type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WorkloadProfile {
    /// A step writes outside the Harvest Postgres.
    pub effects_outside_database: bool,
    /// The sum of the step durations.
    pub total_hold: Duration,
    /// The number of runs that want the hottest key at the same time.
    ///
    /// In an open system, use the arrival rate on the key times the run
    /// duration. In a closed loop of `C` clients, use `C` times the share of
    /// runs that touch the key.
    pub hot_key_concurrency: f64,
    /// The time that backout holds the hot lock longer than a saga does.
    ///
    /// That is the sum of the step durations after the hot step.
    pub hold_after_hot_step: Duration,
    /// The time of one small commit.
    pub commit_latency: Duration,
    /// The hot step is a bounded add or subtract.
    pub hot_step_commutative: bool,
}

/// The arm that the rule picks, and the branch that picked it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Choice {
    /// The arm.
    pub atomicity: Atomicity,
    /// The branch.
    pub reason: Reason,
}

/// Pick an arm for `profile`. The first matching branch wins.
#[must_use]
pub fn choose(profile: &WorkloadProfile) -> Choice {
    let _ = profile;
    Choice {
        atomicity: Atomicity::Saga,
        reason: Reason::Fallback,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A short, cold, same-database workflow.
    fn cold() -> WorkloadProfile {
        WorkloadProfile {
            effects_outside_database: false,
            total_hold: Duration::from_millis(3),
            hot_key_concurrency: 0.016,
            hold_after_hot_step: Duration::from_millis(1),
            commit_latency: Duration::from_millis(2),
            hot_step_commutative: true,
        }
    }

    /// A hot workflow whose backout hold costs more than a commit.
    fn hot_and_slow() -> WorkloadProfile {
        WorkloadProfile {
            hot_key_concurrency: 16.0,
            total_hold: Duration::from_millis(60),
            hold_after_hot_step: Duration::from_millis(40),
            ..cold()
        }
    }

    fn pick(profile: &WorkloadProfile) -> (Atomicity, Reason) {
        let choice = choose(profile);
        (choice.atomicity, choice.reason)
    }

    #[test]
    fn an_external_effect_forces_a_saga() {
        let profile = WorkloadProfile {
            effects_outside_database: true,
            ..cold()
        };
        assert_eq!(pick(&profile), (Atomicity::Saga, Reason::ExternalEffects));
    }

    #[test]
    fn a_hold_over_the_limit_forces_a_saga() {
        let profile = WorkloadProfile {
            total_hold: MAX_HOLD + Duration::from_millis(1),
            ..cold()
        };
        assert_eq!(pick(&profile), (Atomicity::Saga, Reason::LongHold));
    }

    #[test]
    fn a_hold_at_the_limit_is_allowed() {
        let profile = WorkloadProfile {
            total_hold: MAX_HOLD,
            ..cold()
        };
        assert_eq!(pick(&profile), (Atomicity::Backout, Reason::ColdKey));
    }

    #[test]
    fn a_cold_key_picks_backout() {
        assert_eq!(pick(&cold()), (Atomicity::Backout, Reason::ColdKey));
    }

    #[test]
    fn one_concurrent_run_is_not_cold() {
        let profile = WorkloadProfile {
            hot_key_concurrency: COLD_KEY_CONCURRENCY,
            ..hot_and_slow()
        };
        assert_eq!(
            pick(&profile),
            (Atomicity::Hybrid, Reason::CommutativeHotStep)
        );
    }

    #[test]
    fn a_hot_key_with_a_cheap_hold_picks_backout() {
        let profile = WorkloadProfile {
            hold_after_hot_step: Duration::from_millis(2),
            ..hot_and_slow()
        };
        assert_eq!(pick(&profile), (Atomicity::Backout, Reason::CheapHold));
    }

    #[test]
    fn a_hot_commutative_step_picks_hybrid() {
        assert_eq!(
            pick(&hot_and_slow()),
            (Atomicity::Hybrid, Reason::CommutativeHotStep)
        );
    }

    #[test]
    fn a_hot_step_that_does_not_commute_picks_a_saga() {
        let profile = WorkloadProfile {
            hot_step_commutative: false,
            ..hot_and_slow()
        };
        assert_eq!(pick(&profile), (Atomicity::Saga, Reason::Fallback));
    }

    #[test]
    fn an_unknown_concurrency_counts_as_hot() {
        let profile = WorkloadProfile {
            hot_key_concurrency: f64::NAN,
            ..hot_and_slow()
        };
        assert_eq!(
            pick(&profile),
            (Atomicity::Hybrid, Reason::CommutativeHotStep)
        );
    }

    #[test]
    fn the_external_effect_branch_wins_over_every_other() {
        let profile = WorkloadProfile {
            effects_outside_database: true,
            total_hold: MAX_HOLD * 2,
            ..hot_and_slow()
        };
        assert_eq!(pick(&profile), (Atomicity::Saga, Reason::ExternalEffects));
    }
}
