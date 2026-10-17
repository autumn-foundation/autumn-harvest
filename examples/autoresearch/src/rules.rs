//! The keep rule: one pure function decides each experiment.
//!
//! Upstream keeps a commit with a lower `val_bpb` and resets everything else.
//! The simplicity criterion weighs the gain against the lines that the change
//! adds. A change that deletes lines earns credit, so an equal result from
//! less code is a keep.

use serde::{Deserialize, Serialize};

/// The status column of `results.tsv`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    /// The commit advances the branch.
    Keep,
    /// The commit is reset.
    Discard,
    /// The run produced no `val_bpb`. The commit is reset.
    Crash,
}

impl Status {
    /// The word that `results.tsv` uses.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Keep => "keep",
            Self::Discard => "discard",
            Self::Crash => "crash",
        }
    }
}

/// The thresholds of the keep rule.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct KeepPolicy {
    /// The adjusted gain must be larger than this value.
    pub min_improvement: f64,
    /// The `val_bpb` cost of one net added line.
    pub line_cost: f64,
    /// A result above this peak memory is a discard.
    pub max_memory_gb: f64,
}

impl Default for KeepPolicy {
    fn default() -> Self {
        Self {
            min_improvement: 0.0,
            line_cost: 0.0001,
            max_memory_gb: 80.0,
        }
    }
}

impl KeepPolicy {
    /// Check that every field is finite and not negative.
    ///
    /// # Errors
    ///
    /// Returns the name of the first field that is not valid.
    pub fn validate(&self) -> Result<(), String> {
        let _ = self;
        unimplemented!("green phase")
    }
}

/// The best kept result so far: the base of the next experiment.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Best {
    /// The short commit hash.
    pub commit: String,
    /// The validation bits per byte.
    pub val_bpb: f64,
    /// The peak memory in GB.
    pub memory_gb: f64,
}

/// What one training run measured, and the size of its change.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Measured {
    /// `None` when the run crashed.
    pub val_bpb: Option<f64>,
    /// The peak memory in GB. Zero for a crash.
    pub memory_gb: f64,
    /// Lines that the change added to `train.py`.
    pub lines_added: u32,
    /// Lines that the change removed from `train.py`.
    pub lines_removed: u32,
}

/// The decision on one experiment.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Verdict {
    /// The status column.
    pub status: Status,
    /// The gain minus the line cost. Zero for a crash and for a baseline.
    pub score: f64,
    /// Why, in one short sentence.
    pub reason: String,
}

/// Decide one experiment against `best`. `None` means a baseline.
///
/// - No `val_bpb`: crash.
/// - A baseline that ran: keep.
/// - Memory over the cap: discard.
/// - `gain - line_cost * net_added > min_improvement`: keep.
/// - Else: discard.
#[must_use]
pub fn decide(policy: &KeepPolicy, best: Option<&Best>, measured: &Measured) -> Verdict {
    let _ = (policy, best, measured);
    unimplemented!("green phase")
}

/// Convert `peak_vram_mb` to GB, rounded to one decimal as upstream does.
#[must_use]
pub fn memory_gb(peak_vram_mb: f64) -> f64 {
    let _ = peak_vram_mb;
    unimplemented!("green phase")
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::float_cmp)]

    use proptest::prelude::*;

    use super::*;

    fn best(val_bpb: f64) -> Best {
        Best {
            commit: "a1b2c3d".into(),
            val_bpb,
            memory_gb: 44.0,
        }
    }

    fn ran(val_bpb: f64, added: u32, removed: u32) -> Measured {
        Measured {
            val_bpb: Some(val_bpb),
            memory_gb: 44.0,
            lines_added: added,
            lines_removed: removed,
        }
    }

    #[test]
    fn a_lower_val_bpb_with_no_new_lines_is_a_keep() {
        let verdict = decide(&KeepPolicy::default(), Some(&best(0.9979)), &ran(0.9932, 1, 1));
        assert_eq!(verdict.status, Status::Keep);
        assert!(verdict.score > 0.0);
    }

    #[test]
    fn an_equal_or_worse_val_bpb_is_a_discard() {
        let policy = KeepPolicy::default();
        assert_eq!(
            decide(&policy, Some(&best(0.9979)), &ran(0.9979, 1, 1)).status,
            Status::Discard
        );
        assert_eq!(
            decide(&policy, Some(&best(0.9979)), &ran(1.0050, 1, 1)).status,
            Status::Discard
        );
    }

    #[test]
    fn a_crash_is_a_crash_with_no_score() {
        let crashed = Measured {
            val_bpb: None,
            memory_gb: 0.0,
            lines_added: 3,
            lines_removed: 0,
        };
        let verdict = decide(&KeepPolicy::default(), Some(&best(0.9979)), &crashed);
        assert_eq!(verdict.status, Status::Crash);
        assert_eq!(verdict.score, 0.0);
        assert_eq!(
            decide(&KeepPolicy::default(), None, &crashed).status,
            Status::Crash
        );
    }

    #[test]
    fn a_baseline_that_ran_is_a_keep() {
        let verdict = decide(&KeepPolicy::default(), None, &ran(0.9979, 0, 0));
        assert_eq!(verdict.status, Status::Keep);
    }

    #[test]
    fn a_small_gain_that_adds_twenty_lines_is_a_discard() {
        // Upstream: "A 0.001 val_bpb improvement that adds 20 lines of hacky
        // code? Probably not worth it."
        let verdict = decide(&KeepPolicy::default(), Some(&best(0.9979)), &ran(0.9969, 20, 0));
        assert_eq!(verdict.status, Status::Discard);
    }

    #[test]
    fn an_equal_result_from_less_code_is_a_keep() {
        // Upstream: "removing something and getting equal or better results
        // is a great outcome".
        let verdict = decide(&KeepPolicy::default(), Some(&best(0.9979)), &ran(0.9979, 0, 3));
        assert_eq!(verdict.status, Status::Keep);
    }

    #[test]
    fn memory_over_the_cap_is_a_discard() {
        let mut measured = ran(0.9000, 1, 1);
        measured.memory_gb = 90.0;
        let verdict = decide(&KeepPolicy::default(), Some(&best(0.9979)), &measured);
        assert_eq!(verdict.status, Status::Discard);
    }

    #[test]
    fn memory_is_rounded_to_one_decimal_gb() {
        assert_eq!(memory_gb(45_060.2), 44.0);
        assert_eq!(memory_gb(0.0), 0.0);
        assert_eq!(memory_gb(1_536.0), 1.5);
    }

    #[test]
    fn a_policy_with_a_negative_or_non_finite_field_is_refused() {
        assert!(KeepPolicy::default().validate().is_ok());
        let bad = KeepPolicy {
            line_cost: -1.0,
            ..KeepPolicy::default()
        };
        assert!(bad.validate().unwrap_err().contains("line_cost"));
        let bad = KeepPolicy {
            min_improvement: f64::NAN,
            ..KeepPolicy::default()
        };
        assert!(bad.validate().unwrap_err().contains("min_improvement"));
    }

    proptest! {
        #[test]
        fn a_keep_always_has_a_score_over_the_threshold(
            base in 0.5_f64..2.0,
            delta in -0.05_f64..0.05,
            added in 0_u32..50,
            removed in 0_u32..50,
        ) {
            let policy = KeepPolicy::default();
            let verdict = decide(&policy, Some(&best(base)), &ran(base + delta, added, removed));
            if verdict.status == Status::Keep {
                prop_assert!(verdict.score > policy.min_improvement);
            } else {
                prop_assert_eq!(verdict.status, Status::Discard);
            }
        }

        #[test]
        fn a_better_result_never_loses_to_a_worse_one(
            base in 0.5_f64..2.0,
            better in 0.0_f64..0.05,
            worse in 0.0_f64..0.05,
            lines in 0_u32..10,
        ) {
            let policy = KeepPolicy::default();
            let good = decide(&policy, Some(&best(base)), &ran(base - better, lines, 0));
            let bad = decide(&policy, Some(&best(base)), &ran(base + worse, lines, 0));
            prop_assert!(good.score >= bad.score);
            if bad.status == Status::Keep {
                prop_assert_eq!(good.status, Status::Keep);
            }
        }
    }
}
