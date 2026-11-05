//! The fixed budget of a refine run (issue #2380).
//!
//! For each source run, both modes run 3k sessions. A session is one
//! proposer or composer stage. Each candidate stage adds one probe. A
//! `KeptBase` or `FellBack` stage adds none. [`RefineLedger`] counts what
//! the run recorded. [`RefineLedger::check`] compares the counts with the
//! identity. `formal/verus/src/agent_refine.rs` proves the kernel
//! [`identity_holds`], and that each plan of each source count meets the
//! identity. The proptest `each_plan_meets_the_identity` checks the same
//! counts on a model of the plan.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::propose::StageOutcome;
use crate::mesh_control::Spend;
use crate::message::TokenUsage;

/// The highest `k` of a refine run.
pub const MAX_REFINE_K: u32 = 8;

/// The most source runs of a refine run.
pub const MAX_SOURCE_RUNS: usize = 16;

/// How a refine run moves through its steps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefineMode {
    /// k INV and k DOM stages from one base, k compositions over all of
    /// their probes, then one selection.
    Parallel,
    /// k steps. Each step carries its composition to the next step.
    Sequential,
}

/// The counts and the spend of one refine run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct RefineLedger {
    /// The proposer and composer stages.
    pub sessions: u64,
    /// The probe evaluations.
    pub probes: u64,
    /// The stages that ended as `KeptBase`.
    pub kept_base: u64,
    /// The stages that ended as `FellBack`.
    pub fell_back: u64,
    /// The live re-probes that ran. They are not in the identity.
    pub live_reprobes: u64,
    /// The selections. They are not in the identity.
    pub selections: u64,
    /// The provider-reported tokens of every recorded step.
    pub tokens: u64,
    /// The cost of every recorded step, in millionths of a US dollar. An
    /// unpriced call adds none.
    pub cost_usd_micros: u64,
}

impl RefineLedger {
    /// Count one stage that ended as `outcome`.
    pub const fn stage(&mut self, outcome: &StageOutcome) {
        self.sessions = self.sessions.saturating_add(1);
        match outcome {
            StageOutcome::Proposed(_) => {}
            StageOutcome::KeptBase => self.kept_base = self.kept_base.saturating_add(1),
            StageOutcome::FellBack { .. } => self.fell_back = self.fell_back.saturating_add(1),
        }
    }

    /// Count one probe.
    pub const fn probe(&mut self) {
        self.probes = self.probes.saturating_add(1);
    }

    /// Add the spend of one step.
    pub fn spend(&mut self, usage: &TokenUsage, cost_usd_micros: u64) {
        self.tokens = self
            .tokens
            .saturating_add(u64::from(usage.input_tokens))
            .saturating_add(u64::from(usage.output_tokens));
        self.cost_usd_micros = self.cost_usd_micros.saturating_add(cost_usd_micros);
    }

    /// The spend so far, as a mesh budget reads it.
    #[must_use]
    pub const fn spent(&self) -> Spend {
        Spend {
            tokens: self.tokens,
            cost_usd_micros: self.cost_usd_micros,
        }
    }

    /// Check the counts of a run that ran its whole plan over `sources`
    /// source runs with `k`.
    ///
    /// # Errors
    ///
    /// Returns [`RefineError::BudgetIdentity`] when a count does not match
    /// the identity.
    pub const fn check(&self, k: u32, sources: u32) -> Result<(), RefineError> {
        let skipped = self.kept_base.saturating_add(self.fell_back);
        let expected_sessions = planned_sessions(k, sources);
        if identity_holds(expected_sessions, self.sessions, self.probes, skipped) {
            return Ok(());
        }
        Err(RefineError::BudgetIdentity {
            expected_sessions,
            sessions: self.sessions,
            expected_probes: self.sessions.saturating_sub(skipped),
            probes: self.probes,
        })
    }
}

/// The sessions of a whole plan: 3k for each source run.
#[must_use]
pub const fn planned_sessions(k: u32, sources: u32) -> u64 {
    (k as u64).saturating_mul(3).saturating_mul(sources as u64)
}

/// Whether the counts meet the identity of a plan of `planned` sessions.
///
/// Each session that is not a skip adds one probe.
/// `formal/verus/src/agent_refine.rs` proves it.
pub(crate) const fn identity_holds(planned: u64, sessions: u64, probes: u64, skipped: u64) -> bool {
    sessions == planned && skipped <= sessions && probes == sessions - skipped
}

/// Why a refine run fails.
///
/// [`code`](Self::code) is the error type of the failure payload. Each
/// source error comes before any model call.
#[derive(Debug, Clone, PartialEq, Eq, Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "snake_case")]
#[non_exhaustive]
pub enum RefineError {
    /// The input is not valid. The text says why.
    #[error("the refine input is not valid: {0}")]
    InvalidInput(String),
    /// No run has this id.
    #[error("the source run {run} does not exist")]
    SourceNotFound {
        /// The source run.
        run: String,
    },
    /// The source is not an `agent_loop` or `agent` run.
    #[error("the source run {run} is a {workflow} run, not an agent_loop or agent run")]
    SourceNotAgentRun {
        /// The source run.
        run: String,
        /// Its workflow type.
        workflow: String,
    },
    /// The source has not ended.
    #[error("the source run {run} has not ended")]
    SourceInFlight {
        /// The source run.
        run: String,
    },
    /// The source is of another kind, or it names no kind.
    #[error("the source run {run} is of the kind {found:?}, not {kind:?}")]
    SourceOtherKind {
        /// The source run.
        run: String,
        /// The kind of the refine run.
        kind: String,
        /// The kind of the source, if it names one.
        found: Option<String>,
    },
    /// The replay probe cannot read the source. The text says why.
    #[error("the source run {run} cannot be probed: {reason}")]
    SourceNotReplayable {
        /// The source run.
        run: String,
        /// Why.
        reason: String,
    },
    /// A count does not match the identity.
    #[error(
        "the refine ledger breaks its identity: {sessions} sessions of {expected_sessions}, {probes} probes of {expected_probes}"
    )]
    BudgetIdentity {
        /// The planned sessions.
        expected_sessions: u64,
        /// The recorded sessions.
        sessions: u64,
        /// The probes that the recorded sessions plan.
        expected_probes: u64,
        /// The recorded probes.
        probes: u64,
    },
    /// A stage failed for good, for example with a bad key. The text names
    /// the stage and the error.
    #[error("a refine stage failed: {0}")]
    Stage(String),
}

impl RefineError {
    /// The error type of the failure payload.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidInput(_) => "RefineInvalidInput",
            Self::SourceNotFound { .. } => "RefineSourceNotFound",
            Self::SourceNotAgentRun { .. } => "RefineSourceNotAgentRun",
            Self::SourceInFlight { .. } => "RefineSourceInFlight",
            Self::SourceOtherKind { .. } => "RefineSourceOtherKind",
            Self::SourceNotReplayable { .. } => "RefineSourceNotReplayable",
            Self::BudgetIdentity { .. } => "RefineBudgetIdentity",
            Self::Stage(_) => "RefineStageFailed",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::overlay::ProfileOverlay;
    use proptest::prelude::*;

    /// The stage outcomes of one plan: `true` is a candidate.
    fn run_plan(mode: RefineMode, k: u32, outcomes: &[bool]) -> RefineLedger {
        let mut ledger = RefineLedger::default();
        let mut at = 0;
        let mut stage = |ledger: &mut RefineLedger| {
            let candidate = outcomes.get(at).copied().unwrap_or(true);
            at += 1;
            let outcome = match (candidate, at % 2 == 0) {
                (true, _) => StageOutcome::Proposed(ProfileOverlay::default()),
                (false, true) => StageOutcome::KeptBase,
                (false, false) => StageOutcome::FellBack {
                    error: "no".to_owned(),
                },
            };
            ledger.stage(&outcome);
            if candidate {
                ledger.probe();
            }
        };
        match mode {
            RefineMode::Parallel => {
                for _ in 0..3 * k {
                    stage(&mut ledger);
                }
            }
            RefineMode::Sequential => {
                for _ in 0..k {
                    for _ in 0..3 {
                        stage(&mut ledger);
                    }
                }
            }
        }
        ledger
    }

    #[test]
    fn k_two_gives_six_sessions_and_six_probes() {
        for mode in [RefineMode::Parallel, RefineMode::Sequential] {
            let ledger = run_plan(mode, 2, &[]);
            assert_eq!((ledger.sessions, ledger.probes), (6, 6));
            assert_eq!(ledger.check(2, 1), Ok(()));
        }
    }

    #[test]
    fn a_kept_stage_adds_a_session_and_no_probe() {
        let ledger = run_plan(RefineMode::Parallel, 2, &[false]);
        assert_eq!((ledger.sessions, ledger.probes), (6, 5));
        assert_eq!(ledger.kept_base + ledger.fell_back, 1);
        assert_eq!(ledger.check(2, 1), Ok(()));
    }

    #[test]
    fn a_missing_probe_breaks_the_identity() {
        let mut ledger = run_plan(RefineMode::Sequential, 2, &[]);
        ledger.probes -= 1;
        let err = ledger.check(2, 1).unwrap_err();
        assert_eq!(err.code(), "RefineBudgetIdentity");
        assert_eq!(
            err,
            RefineError::BudgetIdentity {
                expected_sessions: 6,
                sessions: 6,
                expected_probes: 6,
                probes: 5,
            }
        );
        assert_eq!(
            ledger.check(3, 1).unwrap_err().code(),
            "RefineBudgetIdentity"
        );
    }

    #[test]
    fn the_spend_adds_input_and_output_tokens() {
        let mut ledger = RefineLedger::default();
        ledger.spend(&TokenUsage::new(10, 5), 3);
        ledger.spend(&TokenUsage::new(1, 1), 0);
        assert_eq!(
            ledger.spent(),
            Spend {
                tokens: 17,
                cost_usd_micros: 3
            }
        );
    }

    #[test]
    fn the_error_round_trips_with_its_name() {
        let err = RefineError::SourceOtherKind {
            run: "r".to_owned(),
            kind: "a".to_owned(),
            found: None,
        };
        let value = serde_json::to_value(&err).unwrap();
        assert_eq!(value["error"], "source_other_kind");
        assert_eq!(serde_json::from_value::<RefineError>(value).unwrap(), err);
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            failure_persistence: None,
            ..ProptestConfig::default()
        })]

        #[test]
        fn each_plan_meets_the_identity(
            k in 1..=MAX_REFINE_K,
            outcomes in proptest::collection::vec(any::<bool>(), 0..30),
            parallel in any::<bool>(),
        ) {
            let mode = if parallel { RefineMode::Parallel } else { RefineMode::Sequential };
            let ledger = run_plan(mode, k, &outcomes);
            prop_assert_eq!(ledger.sessions, planned_sessions(k, 1));
            prop_assert_eq!(ledger.check(k, 1), Ok(()));
        }
    }
}
