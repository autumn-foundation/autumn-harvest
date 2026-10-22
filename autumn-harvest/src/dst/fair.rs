//! Weak fairness for the drain phase of a simulation.
//!
//! A liveness check holds only under a fair scheduler. A weighted draw alone
//! can skip a rare action for a whole drain (issue #2217). [`Waits`] records
//! when each enabled action began to wait. An action that waits a set number
//! of steps is overdue, and the scheduler runs it with no draw.
//!
//! The world drain (issue #2217) and the owner-lease drain (issue #2294) use
//! it.

use std::collections::BTreeMap;

/// The step at which each enabled action began to wait for a run.
#[derive(Debug, Clone)]
pub(super) struct Waits<A> {
    since: BTreeMap<A, usize>,
}

impl<A: Ord + Copy> Waits<A> {
    /// No action waits.
    pub(super) const fn new() -> Self {
        Self {
            since: BTreeMap::new(),
        }
    }

    /// Start the wait of each newly enabled action at `step`. Drop the wait
    /// of each disabled action, so a disabled action starts a new wait.
    pub(super) fn track(&mut self, step: usize, enabled: &[A]) {
        self.since.retain(|action, _| enabled.contains(action));
        for action in enabled {
            self.since.entry(*action).or_insert(step);
        }
    }

    /// The action of `enabled` that waited longest, if it waited
    /// `fair_steps` steps at `step`. A tie goes to the first action in
    /// `enabled`.
    ///
    /// Overdue actions run one per step. With `k` enabled actions, a wait is
    /// thus at most `fair_steps + k - 1` steps.
    pub(super) fn overdue(&self, step: usize, fair_steps: usize, enabled: &[A]) -> Option<A> {
        enabled
            .iter()
            .filter_map(|action| Some((*action, *self.since.get(action)?)))
            .filter(|(_, since)| step - since >= fair_steps)
            .min_by_key(|(_, since)| *since)
            .map(|(action, _)| action)
    }

    /// `action` ran, so its wait ends.
    pub(super) fn ran(&mut self, action: &A) {
        self.since.remove(action);
    }

    /// The step at which `action` began to wait, if it waits.
    #[cfg(test)]
    pub(super) fn since(&self, action: &A) -> Option<usize> {
        self.since.get(action).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FAIR: usize = 10;
    const ACTIONS: [char; 3] = ['a', 'b', 'c'];

    /// Waits whose `ACTIONS` began at step 0.
    fn from_zero() -> Waits<char> {
        let mut waits = Waits::new();
        waits.track(0, &ACTIONS);
        waits
    }

    #[test]
    fn an_action_is_overdue_at_fair_steps_and_not_before() {
        let waits = from_zero();
        assert_eq!(waits.overdue(FAIR - 1, FAIR, &ACTIONS), None);
        // Equal waits go to the first action in the list.
        assert_eq!(waits.overdue(FAIR, FAIR, &ACTIONS), Some('a'));
    }

    #[test]
    fn the_longest_wait_runs_first_and_a_run_ends_the_wait() {
        let mut waits = Waits::new();
        waits.track(0, &ACTIONS[1..]);
        waits.track(5, &ACTIONS);
        assert_eq!(waits.overdue(FAIR + 5, FAIR, &ACTIONS), Some('b'));

        waits.ran(&'b');
        waits.track(FAIR + 5, &ACTIONS);
        assert_eq!(waits.overdue(FAIR + 5, FAIR, &ACTIONS), Some('c'));
        assert_eq!(waits.since(&'b'), Some(FAIR + 5));
    }

    #[test]
    fn a_disabled_action_starts_a_new_wait() {
        let mut waits = from_zero();
        waits.track(FAIR, &ACTIONS[..1]);
        waits.track(FAIR, &ACTIONS);
        assert_eq!(waits.overdue(FAIR, FAIR, &ACTIONS), Some('a'));
        assert_eq!(waits.since(&'c'), Some(FAIR));
    }

    #[test]
    fn a_wait_ends_within_fair_steps_plus_k_minus_one() {
        // Every action waits from step 0, and each step runs one overdue
        // action. The last one runs at `FAIR + k - 1`.
        let mut waits = from_zero();
        let mut step = FAIR;
        let mut last = 0;
        while let Some(action) = waits.overdue(step, FAIR, &ACTIONS) {
            waits.ran(&action);
            last = step;
            step += 1;
        }
        assert_eq!(last, FAIR + ACTIONS.len() - 1);
    }
}
