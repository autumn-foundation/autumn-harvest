//! Property tests for [`autumn_harvest::queue_fairness`] (Tier A target 4).
//!
//! Contract under test (from the module docs):
//! - `weighted_queue_order` returns a **permutation** of the input queues (every
//!   queue exactly once, no dupes, no drops) — the no-starvation guarantee;
//! - zero-weight queues are always placed **after** every positive-weight queue
//!   (fallthrough-only draining);
//! - neither function panics on empty / degenerate inputs;
//! - `effective_queue_weights` defaults an absent queue to weight `1` and keeps
//!   input order.

use std::collections::{HashMap, HashSet};

use autumn_harvest::queue_fairness::{effective_queue_weights, weighted_queue_order};
use proptest::prelude::*;
use rand::SeedableRng;
use rand::rngs::StdRng;

use super::prop_config::config;

/// Generate a set of *distinct* queue names (`q0..qN`) each paired with a weight,
/// with zeros over-represented so the zero-handling paths are exercised often.
fn queues_with_weights() -> impl Strategy<Value = Vec<(String, u32)>> {
    proptest::collection::vec(prop_oneof![Just(0u32), 1u32..=1000], 0..=12).prop_map(|weights| {
        weights
            .into_iter()
            .enumerate()
            .map(|(i, w)| (format!("q{i}"), w))
            .collect()
    })
}

proptest! {
    #![proptest_config(config())]

    /// `weighted_queue_order` output is a permutation of the input queue names:
    /// same length, every input name present exactly once, no invented names.
    #[test]
    fn weighted_order_is_a_permutation(spec in queues_with_weights(), seed in any::<u64>()) {
        let pairs: Vec<(&str, u32)> = spec.iter().map(|(n, w)| (n.as_str(), *w)).collect();
        let mut rng = StdRng::seed_from_u64(seed);
        let order = weighted_queue_order(&pairs, &mut rng);

        prop_assert_eq!(order.len(), spec.len(), "length changed");

        let input: HashSet<&str> = spec.iter().map(|(n, _)| n.as_str()).collect();
        let output: HashSet<&str> = order.iter().map(String::as_str).collect();
        prop_assert_eq!(&output, &input, "output is not a set-permutation of the input");

        // No duplicates in the output (set len == vec len).
        prop_assert_eq!(output.len(), order.len(), "output contains a duplicate queue");
    }

    /// Every zero-weight queue appears after every positive-weight queue.
    #[test]
    fn zero_weight_queues_are_last(spec in queues_with_weights(), seed in any::<u64>()) {
        let weight_of: HashMap<&str, u32> =
            spec.iter().map(|(n, w)| (n.as_str(), *w)).collect();
        let pairs: Vec<(&str, u32)> = spec.iter().map(|(n, w)| (n.as_str(), *w)).collect();
        let mut rng = StdRng::seed_from_u64(seed);
        let order = weighted_queue_order(&pairs, &mut rng);

        let mut seen_zero = false;
        for name in &order {
            let w = weight_of[name.as_str()];
            if w == 0 {
                seen_zero = true;
            } else {
                prop_assert!(
                    !seen_zero,
                    "positive-weight queue {name:?} appeared after a zero-weight queue"
                );
            }
        }
    }

    /// `effective_queue_weights` keeps input order and defaults absent queues to 1.
    #[test]
    fn effective_weights_default_absent_to_one(
        n in 0usize..=12,
        overrides in proptest::collection::hash_map("q[0-9]", 0u32..=1000, 0..=6),
    ) {
        let queues: Vec<String> = (0..n).map(|i| format!("q{i}")).collect();
        let result = effective_queue_weights(&queues, &overrides);

        prop_assert_eq!(result.len(), queues.len());
        for (i, (name, weight)) in result.iter().enumerate() {
            prop_assert_eq!(*name, queues[i].as_str(), "order not preserved");
            let expected = overrides.get(*name).copied().unwrap_or(1);
            prop_assert_eq!(*weight, expected, "weight mismatch for {}", name);
        }
    }
}
