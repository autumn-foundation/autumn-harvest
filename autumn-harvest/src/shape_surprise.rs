//! Shape surprise — score each step of a run against a corpus (Nova).
//!
//! # Overview
//!
//! [`shape_census`](crate::shape_census) matches whole paths. A run with a new
//! path is simply unseen, and the census cannot say which step made it odd.
//! This module learns the transitions between event types over a corpus. It is
//! a first-order Markov model of the event log.
//!
//! The model then scores a new run step by step. Each step gets a surprise in
//! bits, `-log2(p)`. A usual step costs a fraction of a bit. A step that the
//! corpus never took costs many bits. The step with the highest surprise is the
//! place where the run went strange, even when its whole path is unique.
//!
//! ```rust
//! use autumn_harvest::event::WorkflowEvent;
//! use autumn_harvest::shape_surprise::{Step, TransitionModel};
//!
//! let done = WorkflowEvent::WorkflowCompleted { output: serde_json::json!(1) };
//! let mut model = TransitionModel::new();
//! for _ in 0..9 {
//!     model.observe(&[done.clone()]);
//! }
//!
//! let usual = model.score(&[done.clone()]);
//! let odd = model.score(&[done.clone(), done]);
//! assert!(odd.total_bits > usual.total_bits);
//! assert_eq!(odd.peak.map(|s| s.index), Some(1));
//! assert_eq!(
//!     model.predict_next(Step::Start).map(|p| p.0),
//!     Some(Step::Event("WorkflowCompleted")),
//! );
//! ```
//!
//! # Smoothing
//!
//! The model uses add-one (Laplace) smoothing. Each known next state and one
//! extra slot for an unknown state get one pseudo count. So an unseen step has
//! a finite cost, and the totals of two runs stay comparable. A larger corpus
//! makes an unseen step cost more bits, because the corpus is surer of itself.
//!
//! # Scope
//!
//! The module is pure. It reads events and writes nothing. It adds no
//! `WorkflowEvent` variant, no SQL and no migration, so replay is unaffected.
//! It reads event type names only. Payload changes do not move a score.
//!
//! Every order is deterministic. The maps are ordered, and a tie on count keeps
//! the state order.

use std::collections::{BTreeMap, BTreeSet};

use crate::event::WorkflowEvent;

/// One state of the transition model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Step {
    /// The state before the first event of a history.
    Start,
    /// The state after an event with this type name.
    Event(&'static str),
    /// The state after the last event of a history.
    End,
}

/// The surprise of one transition, from [`TransitionModel::score`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StepSurprise {
    /// The index of the event that the transition reaches.
    ///
    /// The transition to [`Step::End`] has the index of the history length.
    pub index: usize,
    /// The state that the transition leaves.
    pub from: Step,
    /// The state that the transition reaches.
    pub to: Step,
    /// The smoothed probability of the transition, in `0.0..=1.0`.
    pub probability: f64,
    /// The surprise of the transition in bits, `-log2(probability)`.
    pub bits: f64,
    /// `true` when the corpus never took this transition.
    pub novel: bool,
}

/// The surprise of one history, from [`TransitionModel::score`].
#[derive(Debug, Clone, PartialEq)]
pub struct SurpriseReport {
    /// The surprise of each transition, in history order.
    pub steps: Vec<StepSurprise>,
    /// The sum of the step surprises, in bits.
    pub total_bits: f64,
    /// The mean step surprise in bits.
    pub mean_bits: f64,
    /// The step with the highest surprise. A tie keeps the earliest step.
    pub peak: Option<StepSurprise>,
    /// The number of transitions that the corpus never took.
    pub novel_steps: usize,
}

/// A first-order transition model of event type names over a corpus.
#[derive(Debug, Clone, Default)]
pub struct TransitionModel {
    transitions: BTreeMap<Step, BTreeMap<Step, usize>>,
    targets: BTreeSet<Step>,
    histories: usize,
}

fn steps_of(events: &[WorkflowEvent]) -> Vec<Step> {
    std::iter::once(Step::Start)
        .chain(events.iter().map(|e| Step::Event(e.type_name())))
        .chain(std::iter::once(Step::End))
        .collect()
}

#[allow(clippy::cast_precision_loss)]
fn ratio(numerator: usize, denominator: usize) -> f64 {
    numerator as f64 / denominator as f64
}

impl TransitionModel {
    /// Return an empty model.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Count the transitions of one history.
    pub fn observe(&mut self, events: &[WorkflowEvent]) {
        for pair in steps_of(events).windows(2) {
            let (from, to) = (pair[0], pair[1]);
            *self
                .transitions
                .entry(from)
                .or_default()
                .entry(to)
                .or_insert(0) += 1;
            self.targets.insert(to);
        }
        self.histories += 1;
    }

    /// Return the number of observed histories.
    #[must_use]
    pub const fn histories(&self) -> usize {
        self.histories
    }

    /// Return the number of times that the corpus took `from` to `to`.
    #[must_use]
    pub fn count(&self, from: Step, to: Step) -> usize {
        self.transitions
            .get(&from)
            .and_then(|exits| exits.get(&to))
            .copied()
            .unwrap_or(0)
    }

    fn exits_from(&self, from: Step) -> usize {
        self.transitions
            .get(&from)
            .map_or(0, |exits| exits.values().sum())
    }

    /// Return the smoothed probability of the transition `from` to `to`.
    #[must_use]
    pub fn probability(&self, from: Step, to: Step) -> f64 {
        // Each known target and one unknown slot get one pseudo count.
        let slots = self.targets.len() + 1;
        ratio(self.count(from, to) + 1, self.exits_from(from) + slots)
    }

    /// Return the most likely next state after `from`, with its probability.
    ///
    /// It is `None` when the corpus never left `from`.
    #[must_use]
    pub fn predict_next(&self, from: Step) -> Option<(Step, f64)> {
        let exits = self.transitions.get(&from)?;
        // A strict comparison keeps the first state of a count tie.
        let mut best: Option<(Step, usize)> = None;
        for (&to, &count) in exits {
            if best.is_none_or(|(_, top)| count > top) {
                best = Some((to, count));
            }
        }
        best.map(|(to, _)| (to, self.probability(from, to)))
    }

    /// Score one history against the model. The model does not change.
    #[must_use]
    pub fn score(&self, events: &[WorkflowEvent]) -> SurpriseReport {
        let steps: Vec<StepSurprise> = steps_of(events)
            .windows(2)
            .enumerate()
            .map(|(index, pair)| {
                let (from, to) = (pair[0], pair[1]);
                let probability = self.probability(from, to);
                StepSurprise {
                    index,
                    from,
                    to,
                    probability,
                    bits: -probability.log2(),
                    novel: self.count(from, to) == 0,
                }
            })
            .collect();
        let total_bits: f64 = steps.iter().map(|s| s.bits).sum();
        // `steps_of` always yields Start and End, so `steps` is never empty.
        #[allow(clippy::cast_precision_loss)]
        let mean_bits = total_bits / steps.len() as f64;
        // A strict comparison keeps the earliest step of a tie.
        let mut peak: Option<StepSurprise> = None;
        for step in &steps {
            if peak.is_none_or(|top| step.bits > top.bits) {
                peak = Some(*step);
            }
        }
        SurpriseReport {
            novel_steps: steps.iter().filter(|s| s.novel).count(),
            total_bits,
            mean_bits,
            peak,
            steps,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TimerId;

    fn done() -> WorkflowEvent {
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!(null),
        }
    }

    fn failed() -> WorkflowEvent {
        WorkflowEvent::WorkflowFailed {
            error: "boom".into(),
            error_type: None,
            details: None,
            non_retryable: Some(false),
        }
    }

    fn fired() -> WorkflowEvent {
        WorkflowEvent::TimerFired {
            timer_id: TimerId::new("t-1"),
        }
    }

    const DONE: Step = Step::Event("WorkflowCompleted");
    const FAILED: Step = Step::Event("WorkflowFailed");
    const FIRED: Step = Step::Event("TimerFired");

    fn corpus() -> TransitionModel {
        let mut model = TransitionModel::new();
        for _ in 0..8 {
            model.observe(&[fired(), done()]);
        }
        model.observe(&[fired(), failed()]);
        model
    }

    #[test]
    fn observe_counts_start_and_end_transitions() {
        let model = corpus();
        assert_eq!(model.histories(), 9);
        assert_eq!(model.count(Step::Start, FIRED), 9);
        assert_eq!(model.count(FIRED, DONE), 8);
        assert_eq!(model.count(FIRED, FAILED), 1);
        assert_eq!(model.count(DONE, Step::End), 8);
        assert_eq!(model.count(DONE, FAILED), 0);
    }

    #[test]
    fn an_empty_history_is_one_start_to_end_step() {
        let mut model = TransitionModel::new();
        model.observe(&[]);
        assert_eq!(model.count(Step::Start, Step::End), 1);
        let report = model.score(&[]);
        assert_eq!(report.steps.len(), 1);
        assert_eq!(report.steps[0].index, 0);
        assert_eq!(report.novel_steps, 0);
    }

    #[test]
    fn probabilities_are_smoothed_and_sum_to_at_most_one() {
        let model = corpus();
        // Targets: FIRED, DONE, FAILED, End, plus one unknown slot. Nine exits.
        let usual = model.probability(FIRED, DONE);
        assert!((usual - 9.0 / 14.0).abs() < 1e-12);
        let unseen = model.probability(FIRED, Step::End);
        assert!((unseen - 1.0 / 14.0).abs() < 1e-12);
        let known: f64 = [FIRED, DONE, FAILED, Step::End]
            .into_iter()
            .map(|to| model.probability(FIRED, to))
            .sum();
        assert!(known < 1.0);
        assert!((model.probability(FAILED, DONE) - 1.0 / 6.0).abs() < 1e-12);
    }

    #[test]
    fn an_empty_model_spreads_probability_evenly() {
        let model = TransitionModel::new();
        assert!((model.probability(Step::Start, DONE) - 1.0).abs() < 1e-12);
        let report = model.score(&[done()]);
        assert_eq!(report.steps.len(), 2);
        assert!(report.total_bits.abs() < 1e-12);
        assert_eq!(report.novel_steps, 2);
        assert_eq!(model.predict_next(Step::Start), None);
    }

    #[test]
    fn a_usual_run_costs_less_than_a_rare_one() {
        let model = corpus();
        let usual = model.score(&[fired(), done()]);
        let rare = model.score(&[fired(), failed()]);
        assert_eq!(usual.novel_steps, 0);
        assert_eq!(rare.novel_steps, 0);
        assert!(rare.total_bits > usual.total_bits);
        assert_eq!(rare.peak.map(|s| s.to), Some(FAILED));
    }

    #[test]
    fn the_peak_names_the_step_where_a_novel_run_went_strange() {
        let model = corpus();
        let report = model.score(&[fired(), fired(), done()]);
        assert_eq!(report.steps.len(), 4);
        let peak = report.peak.expect("a peak step");
        assert_eq!(peak.index, 1);
        assert_eq!((peak.from, peak.to), (FIRED, FIRED));
        assert!(peak.novel);
        assert_eq!(report.novel_steps, 1);
        let bits: f64 = report.steps.iter().map(|s| s.bits).sum();
        assert!((report.total_bits - bits).abs() < 1e-12);
        assert!((report.mean_bits - bits / 4.0).abs() < 1e-12);
    }

    #[test]
    fn surprise_is_minus_log2_of_probability() {
        let model = corpus();
        let report = model.score(&[fired(), done()]);
        for step in &report.steps {
            assert!((step.bits + step.probability.log2()).abs() < 1e-12);
        }
    }

    #[test]
    fn predict_next_picks_the_most_common_target() {
        let model = corpus();
        let (next, p) = model.predict_next(FIRED).expect("a prediction");
        assert_eq!(next, DONE);
        assert!((p - model.probability(FIRED, DONE)).abs() < 1e-12);
        assert_eq!(model.predict_next(DONE).map(|n| n.0), Some(Step::End));
        assert_eq!(model.predict_next(Step::End), None);
    }

    #[test]
    fn a_count_tie_keeps_the_state_order() {
        let mut model = TransitionModel::new();
        model.observe(&[failed()]);
        model.observe(&[done()]);
        let mut reversed = TransitionModel::new();
        reversed.observe(&[done()]);
        reversed.observe(&[failed()]);
        assert_eq!(
            model.predict_next(Step::Start),
            reversed.predict_next(Step::Start)
        );
        assert_eq!(model.predict_next(Step::Start).map(|n| n.0), Some(DONE));
    }

    #[test]
    fn payload_changes_do_not_move_a_score() {
        let model = corpus();
        let a = model.score(&[fired(), done()]);
        let b = model.score(&[
            fired(),
            WorkflowEvent::WorkflowCompleted {
                output: serde_json::json!({"x": 1}),
            },
        ]);
        assert_eq!(a, b);
    }
}
