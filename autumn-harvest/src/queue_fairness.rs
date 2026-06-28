/// Weighted task-queue selection for multi-queue worker fairness (issue #515).
///
/// This module is pure / no-DB. It computes a weighted-random queue ordering
/// that the worker's poll loop uses to decide *which* queue to attempt to
/// claim from on each poll iteration.
///
/// # Semantics
///
/// - A queue's **weight** (a positive `u32`) is its relative probability of
///   being placed first in the ordering (and therefore tried first for a claim).
/// - Under sustained saturation the empirical dispatch share per queue converges
///   to `weight_i / sum(all_weights)` — the classic Sidekiq model.
/// - **No-starvation guarantee:** any queue with `weight >= 1` and available
///   work always makes forward progress, because the ordering is a *permutation*
///   of all non-zero-weight queues followed by all zero-weight queues. A claim
///   attempt walks the permutation until `Some(task)` is returned, so every
///   queue is reachable on every poll (zero-weight queues are reachable only
///   when higher-weight queues have no work).
/// - **Default (empty weight map):** the caller should pass the full queue list
///   directly to `claim_task` with no ordering step, identical to today's
///   `ANY($2)` single-query behaviour. This function is only invoked when the
///   operator has explicitly set at least one weight.
///
/// # Composition with within-queue priority (#249)
///
/// Weights decide **which queue** to claim from. Once a queue is selected,
/// `claim_task` uses its standard `ORDER BY priority DESC, scheduled_at ASC`
/// SQL to pick the best row in that queue — fully unchanged.
use std::collections::HashMap;

/// Pair a queue name with its effective non-negative weight.
///
/// Queues absent from the operator's weight map default to weight **1**
/// (equal share with un-weighted peers).
///
/// # Arguments
///
/// * `queues` — ordered list of queue names the worker is bound to.
/// * `weights` — operator-supplied weight map (`WorkerConfig::queue_weights`).
///
/// Returns a `Vec` in the same order as `queues`. Queues absent from `weights`
/// get weight `1`; queues present get their configured value (including `0`).
///
/// The returned `&str` slices borrow from `queues` — no string clones per poll
/// in the weighted hot path.
#[must_use]
pub fn effective_queue_weights<'a, S: std::hash::BuildHasher>(
    queues: &'a [String],
    weights: &HashMap<String, u32, S>,
) -> Vec<(&'a str, u32)> {
    queues
        .iter()
        .map(|q| (q.as_str(), weights.get(q.as_str()).copied().unwrap_or(1)))
        .collect()
}

/// Produce a weighted-random permutation of queue names.
///
/// The algorithm is weighted-random sampling *without replacement*
/// (Efraimidis-Spirakis key-based reservoir):
/// each queue with `weight > 0` is assigned a key `U^(1/weight)` where `U`
/// is uniform `(0, 1]`; queues are then sorted descending by key. This gives
/// exactly the desired first-position frequency proportional to `weight`.
///
/// Queues with `weight == 0` are never placed before positive-weight queues;
/// they appear at the end in their original (stable) order so the caller can
/// still drain them as a fallback when all positive-weight queues are empty.
///
/// # Arguments
///
/// * `pairs` — output of [`effective_queue_weights`].
/// * `rng` — any `rand::Rng` (typically `rand::thread_rng()` in production,
///   a seeded `StdRng` in tests for reproducibility).
///
/// Returns a permutation of all queue names.
#[must_use]
pub fn weighted_queue_order(pairs: &[(&str, u32)], rng: &mut impl rand::Rng) -> Vec<String> {
    // Separate positive-weight and zero-weight queues.
    let mut positive: Vec<(&str, f64)> = Vec::with_capacity(pairs.len());
    let mut zeros: Vec<&str> = Vec::new();

    for (name, weight) in pairs {
        if *weight == 0 {
            zeros.push(name);
        } else {
            // Efraimidis-Spirakis: key = U^(1/weight).
            // Larger weight -> key closer to 1 on average -> sorts first.
            // Exclusive upper bound so u is never exactly 1.0: if two queues
            // both drew 1.0 they would tie at key=1.0 and sort order would
            // depend on unstable-sort tie-breaking rather than the weights.
            let u: f64 = rng.gen_range(f64::MIN_POSITIVE..1.0);
            let key = u.powf(1.0 / f64::from(*weight));
            positive.push((name, key));
        }
    }

    // Sort descending by key so the highest key (highest-priority draw) comes first.
    positive.sort_by(|a, b| b.1.total_cmp(&a.1));

    let mut result: Vec<String> = positive.iter().map(|(n, _)| (*n).to_owned()).collect();
    result.extend(zeros.iter().map(|n| (*n).to_owned()));
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;
    use std::collections::HashMap;

    fn make_weights(pairs: &[(&str, u32)]) -> HashMap<String, u32> {
        pairs.iter().map(|(k, v)| ((*k).to_owned(), *v)).collect()
    }

    fn make_queues(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| (*s).to_owned()).collect()
    }

    // -----------------------------------------------------------------------
    // effective_queue_weights
    // -----------------------------------------------------------------------

    #[test]
    fn absent_queue_defaults_to_weight_one() {
        let queues = make_queues(&["a", "b"]);
        let weights = HashMap::new();
        let result = effective_queue_weights(&queues, &weights);
        assert_eq!(result, vec![("a", 1u32), ("b", 1u32)]);
    }

    #[test]
    fn configured_weight_is_preserved() {
        let queues = make_queues(&["bulk", "latency"]);
        let weights = make_weights(&[("bulk", 3), ("latency", 1)]);
        let result = effective_queue_weights(&queues, &weights);
        assert_eq!(result, vec![("bulk", 3u32), ("latency", 1u32)]);
    }

    #[test]
    fn zero_weight_is_preserved() {
        let queues = make_queues(&["high", "low"]);
        let weights = make_weights(&[("high", 5), ("low", 0)]);
        let result = effective_queue_weights(&queues, &weights);
        assert_eq!(result, vec![("high", 5u32), ("low", 0u32)]);
    }

    #[test]
    fn empty_queues_returns_empty() {
        let result = effective_queue_weights(&[], &HashMap::new());
        assert!(result.is_empty());
    }

    // -----------------------------------------------------------------------
    // weighted_queue_order — structural properties
    // -----------------------------------------------------------------------

    #[test]
    fn single_queue_returns_same_queue() {
        let pairs: Vec<(&str, u32)> = vec![("only", 1u32)];
        let mut rng = StdRng::seed_from_u64(42);
        let order = weighted_queue_order(&pairs, &mut rng);
        assert_eq!(order, vec!["only"]);
    }

    #[test]
    fn output_is_a_permutation_no_duplicates_no_missing() {
        let pairs: Vec<(&str, u32)> = vec![("a", 3u32), ("b", 1u32), ("c", 2u32)];
        let mut rng = StdRng::seed_from_u64(99);
        for _ in 0..200 {
            let order = weighted_queue_order(&pairs, &mut rng);
            assert_eq!(order.len(), 3, "length must equal input length");
            let mut sorted = order.clone();
            sorted.sort();
            assert_eq!(
                sorted,
                vec!["a", "b", "c"],
                "all queues present exactly once"
            );
        }
    }

    #[test]
    fn zero_weight_queues_always_appear_last() {
        let pairs: Vec<(&str, u32)> = vec![("high", 5u32), ("zero", 0u32), ("med", 2u32)];
        let mut rng = StdRng::seed_from_u64(7);
        for _ in 0..300 {
            let order = weighted_queue_order(&pairs, &mut rng);
            let zero_pos = order
                .iter()
                .position(|q| q == "zero")
                .expect("item must exist in order slice");
            let high_pos = order
                .iter()
                .position(|q| q == "high")
                .expect("item must exist in order slice");
            let med_pos = order
                .iter()
                .position(|q| q == "med")
                .expect("item must exist in order slice");
            assert!(
                zero_pos > high_pos && zero_pos > med_pos,
                "zero-weight queue must come after all positive-weight queues; order={order:?}"
            );
        }
    }

    #[test]
    fn equal_weights_produce_uniform_first_position_distribution() {
        // With two queues of equal weight, each should be first ~50% +/- 10%.
        let pairs: Vec<(&str, u32)> = vec![("a", 1u32), ("b", 1u32)];
        let mut rng = StdRng::seed_from_u64(12345);
        let n = 2000u32;
        let mut first_a = 0u32;
        for _ in 0..n {
            let order = weighted_queue_order(&pairs, &mut rng);
            if order[0] == "a" {
                first_a += 1;
            }
        }
        let ratio = f64::from(first_a) / f64::from(n);
        assert!(
            (0.40..=0.60).contains(&ratio),
            "expected ~50% but got {:.1}%",
            ratio * 100.0
        );
    }

    #[test]
    fn three_to_one_weight_produces_correct_first_position_frequency() {
        // With weight 3:1, the heavy queue should be first ~75% of the time (+/-10%).
        let pairs: Vec<(&str, u32)> = vec![("bulk", 3u32), ("latency", 1u32)];
        let mut rng = StdRng::seed_from_u64(777);
        let n = 4000u32;
        let mut bulk_first = 0u32;
        for _ in 0..n {
            let order = weighted_queue_order(&pairs, &mut rng);
            if order[0] == "bulk" {
                bulk_first += 1;
            }
        }
        let ratio = f64::from(bulk_first) / f64::from(n);
        assert!(
            (0.65..=0.85).contains(&ratio),
            "expected ~75% but got {:.1}%",
            ratio * 100.0
        );
    }

    #[test]
    fn all_zero_weights_returns_queues_in_stable_original_order() {
        let pairs: Vec<(&str, u32)> = vec![("x", 0u32), ("y", 0u32), ("z", 0u32)];
        let mut rng = StdRng::seed_from_u64(1);
        let order = weighted_queue_order(&pairs, &mut rng);
        // All are zero-weight, so they appear in the zero-queue list in stable order.
        assert_eq!(order, vec!["x", "y", "z"]);
    }

    #[test]
    fn empty_pairs_returns_empty_order() {
        let pairs: Vec<(&str, u32)> = vec![];
        let mut rng = StdRng::seed_from_u64(0);
        let order = weighted_queue_order(&pairs, &mut rng);
        assert!(order.is_empty());
    }

    /// No-starvation property: the low-weight queue must appear somewhere in the
    /// permutation (not just last) at least occasionally, and it always appears
    /// — ensuring it can drain when claimed in order.
    #[test]
    fn low_weight_queue_always_present_in_permutation() {
        let pairs: Vec<(&str, u32)> = vec![("heavy", 10u32), ("light", 1u32)];
        let mut rng = StdRng::seed_from_u64(55);
        for _ in 0..500 {
            let order = weighted_queue_order(&pairs, &mut rng);
            assert!(
                order.contains(&"light".to_owned()),
                "'light' must always appear in the permutation"
            );
        }
    }
}
