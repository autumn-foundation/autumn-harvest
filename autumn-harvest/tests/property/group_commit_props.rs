//! Property tests for the group commit planner (issue #2256).
//!
//! `group_commit::plan_batch` picks the decisions of one batch. These
//! properties are its spec:
//!
//! 1. The batch starts with the leader's own entry.
//! 2. It holds at most `max_batch` entries.
//! 3. Every entry has the leader's tenure.
//! 4. No two entries share an execution.
//! 5. The other entries keep their queue order.
//! 6. The plan skips an admissible entry only when the batch is full.
//! 7. Repeated plans drain any queue, so no entry waits forever.
//! 8. The plan equals `plan` of `formal/verus/src/group_commit.rs`.

use std::collections::BTreeSet;

use autumn_harvest::group_commit::{self, Queued};
use proptest::prelude::*;

use super::prop_config::config;

/// A queue of up to 40 entries over 3 tenures and 8 executions.
fn queue() -> impl Strategy<Value = Vec<Queued<u8, u8>>> {
    prop::collection::vec(
        (0u8..3, 0u8..8).prop_map(|(tenure, execution)| Queued { tenure, execution }),
        0..40,
    )
}

/// Plan the head of `queue` until it is empty. Returns the number of plans.
fn drain(mut queue: Vec<Queued<u8, u8>>, max_batch: usize) -> usize {
    let mut plans = 0;
    while !queue.is_empty() {
        let batch = group_commit::plan_batch(&queue, 0, max_batch);
        assert!(!batch.is_empty(), "a plan always takes the leader");
        let taken: BTreeSet<usize> = batch.into_iter().collect();
        queue = queue
            .into_iter()
            .enumerate()
            .filter(|(at, _)| !taken.contains(at))
            .map(|(_, entry)| entry)
            .collect();
        plans += 1;
    }
    plans
}

/// `plan_from` of `formal/verus/src/group_commit.rs`, one step per entry.
fn spec_plan_from(
    queue: &[Queued<u8, u8>],
    own: usize,
    max_batch: usize,
    i: usize,
    mut batch: Vec<usize>,
) -> Vec<usize> {
    if i >= queue.len() {
        return batch;
    }
    let shared = batch
        .iter()
        .any(|&at| queue[at].execution == queue[i].execution);
    let same = queue[i].tenure == queue[own].tenure;
    if i != own && group_commit::accepts(batch.len(), max_batch, same, shared) {
        batch.push(i);
    }
    spec_plan_from(queue, own, max_batch, i + 1, batch)
}

/// `plan` of `formal/verus/src/group_commit.rs`.
fn spec_plan(queue: &[Queued<u8, u8>], own: usize, max_batch: usize) -> Vec<usize> {
    if own < queue.len() && max_batch >= 1 {
        spec_plan_from(queue, own, max_batch, 0, vec![own])
    } else {
        Vec::new()
    }
}

proptest! {
    #![proptest_config(config())]

    #[test]
    fn the_plan_equals_the_verus_spec(
        queue in queue(),
        own in 0usize..40,
        max_batch in 0usize..10,
    ) {
        prop_assert_eq!(
            group_commit::plan_batch(&queue, own, max_batch),
            spec_plan(&queue, own, max_batch)
        );
    }

    #[test]
    fn a_plan_keeps_every_rule(
        queue in queue(),
        own in 0usize..40,
        max_batch in 1usize..10,
    ) {
        let batch = group_commit::plan_batch(&queue, own, max_batch);
        if own >= queue.len() {
            prop_assert!(batch.is_empty());
            return Ok(());
        }
        prop_assert_eq!(batch.first().copied(), Some(own));
        prop_assert!(batch.len() <= max_batch);
        let tenure = queue[own].tenure;
        let mut executions = BTreeSet::new();
        for &at in &batch {
            prop_assert_eq!(queue[at].tenure, tenure);
            prop_assert!(executions.insert(queue[at].execution), "two entries share an execution");
        }
        prop_assert!(batch[1..].windows(2).all(|pair| pair[0] < pair[1]), "queue order");
        // An entry that the plan skipped is either inadmissible or late.
        let full = batch.len() == max_batch;
        for (at, entry) in queue.iter().enumerate() {
            if batch.contains(&at) {
                continue;
            }
            let admissible = entry.tenure == tenure && !executions.contains(&entry.execution);
            prop_assert!(!admissible || full, "entry {} was admissible", at);
        }
    }

    #[test]
    fn repeated_plans_drain_every_queue(queue in queue(), max_batch in 1usize..10) {
        let len = queue.len();
        let plans = drain(queue, max_batch);
        prop_assert!(plans <= len);
    }

    #[test]
    fn a_batch_of_one_entry_is_a_lone_decision(queue in queue(), own in 0usize..40) {
        let batch = group_commit::plan_batch(&queue, own, 1);
        prop_assert_eq!(batch, if own < queue.len() { vec![own] } else { vec![] });
    }

    #[test]
    fn an_acknowledgement_needs_its_stage_and_the_commit(stage in any::<bool>(), commit in any::<bool>()) {
        prop_assert_eq!(group_commit::acknowledges(stage, commit), stage && commit);
    }
}

#[test]
fn accepts_matches_the_plan_rule() {
    assert!(group_commit::accepts(0, 1, true, false));
    assert!(!group_commit::accepts(1, 1, true, false), "a full batch");
    assert!(!group_commit::accepts(0, 4, false, false), "another tenure");
    assert!(
        !group_commit::accepts(0, 4, true, true),
        "a shared execution"
    );
}
