//! The fan-out plan of one model turn (issue #2483).
//!
//! The plan gives each call a [`Step`]. A batch of delegations runs at the
//! same time. Any other delegation runs alone, and its step names why, so a
//! fan-out never falls back in silence. `formal/verus/src/agent_fan_out.rs`
//! proves the plan for every turn and every bound.

pub use crate::mesh::SerialReason;

/// What one call is to a fan-out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fit {
    /// An allowed delegation to an inline peer. It can join a batch.
    Batchable,
    /// A delegation that the policy gated.
    Gated,
    /// A delegation to a durable peer.
    Durable,
    /// Any other call. The plan does not touch it.
    Other,
}

/// The plan for one call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// The call leads a batch of this many calls.
    Lead(usize),
    /// The call is in the batch of an earlier lead.
    Member,
    /// The delegation runs alone, for this reason.
    Alone(SerialReason),
    /// The plan does not touch the call.
    Skip,
}

/// Can the call join a batch? It takes a reference, because the Verus copy
/// cannot copy an enum. `formal/verus/src/agent_fan_out.rs` proves it.
#[allow(clippy::trivially_copy_pass_by_ref)]
pub const fn can_join(fit: &Fit) -> bool {
    matches!(fit, Fit::Batchable)
}

/// The step of each call of one turn, in call order.
///
/// A batch is a run of 2 to `bound` calls that can join. A bound under 2
/// turns the fan-out off, so each step is [`Step::Skip`].
/// `formal/verus/src/agent_fan_out.rs` proves it for every input.
pub fn fan_out_plan(fits: &[Fit], bound: usize) -> Vec<Step> {
    let mut steps: Vec<Step> = Vec::new();
    if bound < 2 {
        let mut i = 0;
        while i < fits.len() {
            steps.push(Step::Skip);
            i += 1;
        }
        return steps;
    }
    let mut joining: usize = 0;
    let mut i = 0;
    while i < fits.len() {
        if can_join(&fits[i]) {
            joining += 1;
        }
        i += 1;
    }
    let mut at = 0;
    while at < fits.len() {
        match fits[at] {
            Fit::Batchable => {
                let mut end = at + 1;
                while end < fits.len() && end - at < bound && can_join(&fits[end]) {
                    end += 1;
                }
                if end - at >= 2 {
                    steps.push(Step::Lead(end - at));
                    let mut k = at + 1;
                    while k < end {
                        steps.push(Step::Member);
                        k += 1;
                    }
                } else {
                    let step = if at > 0 && can_join(&fits[at - 1]) {
                        Step::Alone(SerialReason::BoundReached)
                    } else if joining > 1 {
                        Step::Alone(SerialReason::NotConsecutive)
                    } else {
                        Step::Alone(SerialReason::SingleCall)
                    };
                    steps.push(step);
                }
                at = end;
            }
            Fit::Gated => {
                steps.push(Step::Alone(SerialReason::Gated));
                at += 1;
            }
            Fit::Durable => {
                steps.push(Step::Alone(SerialReason::DurablePeer));
                at += 1;
            }
            Fit::Other => {
                steps.push(Step::Skip);
                at += 1;
            }
        }
    }
    steps
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    use Fit::{Batchable as B, Durable as D, Gated as G, Other as O};

    fn alone(reason: SerialReason) -> Step {
        Step::Alone(reason)
    }

    /// The spec `step_ok` of the Verus proof, as code.
    fn step_ok(fits: &[Fit], bound: usize, steps: &[Step], i: usize) -> bool {
        let joins = |j: usize| fits.get(j).is_some_and(can_join);
        let left = i > 0 && joins(i - 1);
        let right = joins(i + 1);
        let others = (0..fits.len()).any(|j| j != i && joins(j));
        if bound < 2 {
            return steps[i] == Step::Skip;
        }
        match steps[i] {
            Step::Skip => fits[i] == Fit::Other,
            Step::Lead(n) => {
                (2..=bound).contains(&n)
                    && i + n <= steps.len()
                    && (i..i + n).all(joins)
                    && (i + 1..i + n).all(|j| steps[j] == Step::Member)
            }
            Step::Member => {
                joins(i) && (0..i).any(|k| matches!(steps[k], Step::Lead(n) if i < k + n))
            }
            Step::Alone(reason) => match reason {
                SerialReason::Gated => fits[i] == Fit::Gated,
                SerialReason::DurablePeer => fits[i] == Fit::Durable,
                SerialReason::BoundReached => joins(i) && left && !right,
                SerialReason::NotConsecutive => joins(i) && !left && !right && others,
                SerialReason::SingleCall => joins(i) && !others,
            },
        }
    }

    /// The batches of the plan before issue #2483, as `(start, len)` pairs.
    fn old_batches(fits: &[Fit], bound: usize) -> Vec<(usize, usize)> {
        let batchable = |position: usize| fits[position] == Fit::Batchable;
        let mut batches = Vec::new();
        let mut start = 0;
        while start < fits.len() {
            if !batchable(start) {
                start += 1;
                continue;
            }
            let mut end = start + 1;
            while end < fits.len() && end - start < bound && batchable(end) {
                end += 1;
            }
            if end - start >= 2 {
                batches.push((start, end - start));
            }
            start = end;
        }
        batches
    }

    fn batches(steps: &[Step]) -> Vec<(usize, usize)> {
        steps
            .iter()
            .enumerate()
            .filter_map(|(at, step)| match step {
                Step::Lead(n) => Some((at, *n)),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn only_an_allowed_inline_delegation_can_join() {
        assert!(can_join(&B));
        assert!(!can_join(&G));
        assert!(!can_join(&D));
        assert!(!can_join(&O));
    }

    #[test]
    fn a_lone_delegation_is_a_single_call() {
        assert_eq!(
            fan_out_plan(&[O, B, O], 3),
            [Step::Skip, alone(SerialReason::SingleCall), Step::Skip]
        );
    }

    #[test]
    fn a_split_pair_is_not_consecutive() {
        assert_eq!(
            fan_out_plan(&[B, O, B], 3),
            [
                alone(SerialReason::NotConsecutive),
                Step::Skip,
                alone(SerialReason::NotConsecutive),
            ]
        );
    }

    #[test]
    fn the_call_after_a_full_batch_names_the_bound() {
        assert_eq!(
            fan_out_plan(&[B, B, B], 2),
            [
                Step::Lead(2),
                Step::Member,
                alone(SerialReason::BoundReached)
            ]
        );
    }

    #[test]
    fn a_gated_and_a_durable_delegation_name_their_kind() {
        assert_eq!(
            fan_out_plan(&[G, D], 4),
            [alone(SerialReason::Gated), alone(SerialReason::DurablePeer)]
        );
    }

    #[test]
    fn a_full_run_batches_with_no_reason() {
        assert_eq!(
            fan_out_plan(&[B, B, B], 3),
            [Step::Lead(3), Step::Member, Step::Member]
        );
    }

    #[test]
    fn a_bound_under_two_names_no_reason() {
        for bound in [0, 1] {
            assert_eq!(fan_out_plan(&[B, G, D, O, B], bound), [Step::Skip; 5]);
        }
    }

    #[test]
    fn an_empty_turn_has_no_step() {
        assert_eq!(fan_out_plan(&[], 3), Vec::<Step>::new());
    }

    fn fit() -> impl Strategy<Value = Fit> {
        prop_oneof![Just(B), Just(G), Just(D), Just(O)]
    }

    proptest! {
        /// Each step meets the spec that the Verus proof states.
        #[test]
        fn each_step_meets_the_spec(fits in prop::collection::vec(fit(), 0..24), bound in 0usize..6) {
            let steps = fan_out_plan(&fits, bound);
            prop_assert_eq!(steps.len(), fits.len());
            for i in 0..fits.len() {
                prop_assert!(step_ok(&fits, bound, &steps, i), "{fits:?} {bound} {steps:?} at {i}");
            }
        }

        /// The plan batches exactly as the plan before issue #2483 did, so
        /// a recorded run replays the same.
        #[test]
        fn the_plan_batches_as_before(fits in prop::collection::vec(fit(), 0..24), bound in 0usize..6) {
            let steps = fan_out_plan(&fits, bound);
            prop_assert_eq!(batches(&steps), old_batches(&fits, bound));
        }
    }
}
