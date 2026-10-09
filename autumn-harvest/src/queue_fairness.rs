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
/// Weights decide **which queue** to claim from. `claim_task` then picks the
/// best row in that queue by its standard claim order. That order is
/// `priority`, then the claim-order due time (issue #1824).
///
/// # Fairness keys within a queue (issue #1976)
///
/// Queue weights do not help when many tenants share one queue. A fairness
/// key on each task does. [`FairClock`] is the pure model of the fair claim:
/// start-time fair queuing (SFQ) across the keys of one queue. The fair claim
/// splice in `queue.rs` is the SQL form of the same rules. See
/// `DESIGN-1976.md` and `tests/property/fairness_key_props.rs`.
use std::collections::{BTreeMap, HashMap};

use crate::error::{HarvestError, HarvestResult};

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
/// Returns a permutation of all queue names, borrowed from `pairs` — no
/// string allocation on this poll-time hot path. This used to `.to_owned()`
/// every queue name on every poll (issue #515 Bolt follow-up). That
/// happened regardless of how many the caller's claim loop actually tries
/// before it finds work. A dhat profile at 16 queues / 20,000 polls found
/// those clones were 300,000 of the harness's 420,035 total allocations.
/// See `docs/performance-queue-fairness.md`.
#[must_use]
pub fn weighted_queue_order<'a>(
    pairs: &[(&'a str, u32)],
    rng: &mut impl rand::Rng,
) -> Vec<&'a str> {
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

    let mut result: Vec<&'a str> = positive.iter().map(|(n, _)| *n).collect();
    result.extend(zeros.iter().copied());
    result
}

/// The key of a task that has no fairness key.
///
/// The fair claim puts every unkeyed row in this one key. A start rejects an
/// empty key, so no caller can collide with it.
pub const DEFAULT_FAIRNESS_KEY: &str = "";

/// The weight of a key that has no override.
pub const DEFAULT_FAIRNESS_WEIGHT: f64 = 1.0;

/// The smallest weight an override can set.
pub const MIN_FAIRNESS_WEIGHT: f64 = 0.001;

/// The largest weight an override can set.
pub const MAX_FAIRNESS_WEIGHT: f64 = 1000.0;

/// The most weight overrides one queue can hold.
///
/// Temporal uses the same cap. The claim reads one override per claim, so the
/// cap bounds the table, not the claim cost.
pub const MAX_FAIRNESS_OVERRIDES_PER_QUEUE: usize = 1000;

/// The longest fairness key, in bytes.
///
/// It equals the quota key cap, so a valid quota key fits as a fairness key.
pub const MAX_FAIRNESS_KEY_LEN: usize = 256;

/// Check a fairness key that a caller supplies.
///
/// # Errors
///
/// Returns [`HarvestError::Config`] for an empty key or for the key `.` or
/// `..`. It also rejects a key with outer whitespace or a control character,
/// and a key longer than [`MAX_FAIRNESS_KEY_LEN`] bytes. The empty key is
/// [`DEFAULT_FAIRNESS_KEY`].
pub fn validate_fairness_key(key: &str) -> HarvestResult<()> {
    if key.is_empty() {
        return Err(HarvestError::Config(
            "fairness key must not be empty".to_owned(),
        ));
    }
    // The admin routes carry the key in a URL path segment. URL parsing
    // removes a `.` or `..` segment, so no route could address such a key.
    if key == "." || key == ".." {
        return Err(HarvestError::Config(
            "fairness key must not be `.` or `..`".to_owned(),
        ));
    }
    if key.trim() != key {
        return Err(HarvestError::Config(
            "fairness key must not start or end with whitespace".to_owned(),
        ));
    }
    // A control character in a key would reach logs, audit rows and CLI
    // output unescaped.
    if key.chars().any(char::is_control) {
        return Err(HarvestError::Config(
            "fairness key must not contain a control character".to_owned(),
        ));
    }
    if key.len() > MAX_FAIRNESS_KEY_LEN {
        return Err(HarvestError::Config(format!(
            "fairness key is {} bytes; the limit is {MAX_FAIRNESS_KEY_LEN}",
            key.len()
        )));
    }
    Ok(())
}

/// The fairness key of a new run: the explicit key, else the quota key.
///
/// A quota key names a tenant already, so it is the natural default. A quota
/// key that fails [`validate_fairness_key`] is not used. Every stored key is
/// thus one that the weight API can address, and an inherited key passes
/// validation again on a workflow retry. A run with neither key gets `None`,
/// which is [`DEFAULT_FAIRNESS_KEY`].
#[must_use]
pub fn fairness_key_for(explicit: Option<&str>, quota_key: Option<&str>) -> Option<String> {
    explicit
        .or_else(|| quota_key.filter(|k| validate_fairness_key(k).is_ok()))
        .map(str::to_owned)
}

/// Check a weight override.
///
/// # Errors
///
/// Returns [`HarvestError::Config`] when `weight` is not finite, is below
/// [`MIN_FAIRNESS_WEIGHT`], or is above [`MAX_FAIRNESS_WEIGHT`].
pub fn validate_fairness_weight(weight: f64) -> HarvestResult<f64> {
    if weight.is_finite() && (MIN_FAIRNESS_WEIGHT..=MAX_FAIRNESS_WEIGHT).contains(&weight) {
        Ok(weight)
    } else {
        Err(HarvestError::Config(format!(
            "fairness weight must be a finite number from {MIN_FAIRNESS_WEIGHT} to \
             {MAX_FAIRNESS_WEIGHT}; got {weight}"
        )))
    }
}

/// The stored state of one fairness key in one queue.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FairPass {
    /// The virtual time at which the key may next start: its last start plus
    /// `1 / weight`.
    pub pass: f64,
    /// The start tag of the last claim of the key.
    pub last_start: f64,
}

/// Pure model of the fair claim within one queue (issue #1976).
///
/// Each key has a [`FairPass`]. The queue clock `V` is the largest
/// `last_start` of any key. It never decreases.
///
/// - The start tag of a key is `max(pass, V)`. A key with no state starts
///   at `V`. Idle time thus earns no credit.
/// - The claim takes the row whose key has the smallest lag
///   `start - V`. The due time breaks a tie.
/// - A claim charges the key: `last_start = start` and
///   `pass = start + 1 / weight`. See [`fair_charge`].
///
/// The SQL in `queue::splice_fairness` follows the same rules. The DB test
/// `fair_claim_matches_the_model_sequence` compares the two.
#[derive(Debug, Clone, Default)]
pub struct FairClock {
    keys: BTreeMap<String, FairPass>,
}

impl FairClock {
    /// The queue clock `V`: the largest `last_start`, or `0` with no state.
    #[must_use]
    pub fn vclock(&self) -> f64 {
        self.keys.values().map(|p| p.last_start).fold(0.0, f64::max)
    }

    /// The stored state of `key`, if any.
    #[must_use]
    pub fn state(&self, key: &str) -> Option<FairPass> {
        self.keys.get(key).copied()
    }

    /// The start tag of `key`: `max(pass, V)`.
    #[must_use]
    pub fn start_tag(&self, key: &str) -> f64 {
        fair_start(self.keys.get(key).map(|p| p.pass), self.vclock())
    }

    /// The lag of `key`: its start tag minus `V`. The claim sorts on it.
    #[must_use]
    pub fn lag(&self, key: &str) -> f64 {
        fair_lag(self.keys.get(key).map(|p| p.pass), self.vclock())
    }

    /// Pick the key of the next claim from `(key, due)` rows.
    ///
    /// Returns the key with the smallest lag. The smallest due time breaks a
    /// tie. Returns `None` for no rows.
    pub fn pick<'k, D: Ord>(
        &self,
        rows: impl IntoIterator<Item = (&'k str, D)>,
    ) -> Option<&'k str> {
        let v = self.vclock();
        rows.into_iter()
            .map(|(k, due)| (fair_lag(self.keys.get(k).map(|p| p.pass), v), due, k))
            .min_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)))
            .map(|(_, _, k)| k)
    }

    /// Pick the index of the next claim from `(key, due)` rows.
    ///
    /// Same order as [`FairClock::pick`]. The first row wins a full tie.
    #[must_use]
    pub fn pick_row<D: Ord + Copy>(&self, rows: &[(&str, D)]) -> Option<usize> {
        let v = self.vclock();
        rows.iter()
            .enumerate()
            .min_by(|(ia, a), (ib, b)| {
                let la = fair_lag(self.keys.get(a.0).map(|p| p.pass), v);
                let lb = fair_lag(self.keys.get(b.0).map(|p| p.pass), v);
                la.total_cmp(&lb)
                    .then_with(|| a.1.cmp(&b.1))
                    .then_with(|| ia.cmp(ib))
            })
            .map(|(i, _)| i)
    }

    /// Charge one claim to `key` at `weight` and return its new state.
    ///
    /// The start tag comes from the current state, as the SQL upsert does
    /// after it waits for a concurrent charge of the same key.
    pub fn charge(&mut self, key: &str, weight: f64) -> FairPass {
        let v = self.vclock();
        let pass = self.keys.get(key).map(|p| p.pass);
        let next = fair_charge(pass, v, weight);
        self.keys.insert(key.to_owned(), next);
        next
    }

    /// Delete the state of idle keys that the claim cannot tell apart from
    /// no state. Returns the number of keys deleted.
    ///
    /// A key goes when three things hold. `is_active` is false. Its `pass` is
    /// at most `V`, so it has no debt. Its `last_start` is below `V`, so it
    /// does not set `V`. Such a key starts at `V` with or without its row.
    pub fn prune(&mut self, is_active: impl Fn(&str) -> bool) -> usize {
        let v = self.vclock();
        let before = self.keys.len();
        self.keys
            .retain(|k, p| is_active(k) || p.pass > v || p.last_start >= v);
        before - self.keys.len()
    }

    /// Forget every key when the queue has no pending task. Returns the
    /// number of keys deleted.
    ///
    /// This is the idle rule of start-time fair queuing. With no backlog, no
    /// key is behind another, so every debt is forgiven. Keys that claim once
    /// never move `V`. Without this rule [`FairClock::prune`] never deletes
    /// them, and the state grows with each new key.
    pub fn reset_if_idle(&mut self, any_active: bool) -> usize {
        if any_active {
            return 0;
        }
        let n = self.keys.len();
        self.keys.clear();
        n
    }
}

/// The start tag of a key: `max(pass, v)`. A key with no state starts at `v`.
#[must_use]
pub fn fair_start(pass: Option<f64>, v: f64) -> f64 {
    pass.map_or(v, |p| p.max(v))
}

/// The lag of a key: its start tag minus `v`. Never negative.
#[must_use]
pub fn fair_lag(pass: Option<f64>, v: f64) -> f64 {
    pass.map_or(0.0, |p| (p - v).max(0.0))
}

/// The state of a key after one claim (issue #1976).
///
/// `pass` is the key's stored pass, if any, and `v` is the queue clock. The
/// key starts at `max(pass, v)`. Its new pass is that start plus
/// `1 / weight`, and its `last_start` is that start.
///
/// A new key starts at `v`, so its first claim comes before any key in debt.
/// While new keys arrive more slowly than the queue drains, a backlogged key
/// gets every other claim. When new keys arrive as fast as the queue drains,
/// they take every claim, as in any per-key fair queue. See the `Limits`
/// section of `docs/fairness-keys.md`.
#[must_use]
pub fn fair_charge(pass: Option<f64>, v: f64, weight: f64) -> FairPass {
    let start = fair_start(pass, v);
    FairPass {
        pass: start + 1.0 / weight,
        last_start: start,
    }
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
        assert_eq!(result, [] as [(&str, u32); 0]);
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
            sorted.sort_unstable();
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
            let zero_pos = order.iter().position(|q| *q == "zero").unwrap();
            let high_pos = order.iter().position(|q| *q == "high").unwrap();
            let med_pos = order.iter().position(|q| *q == "med").unwrap();
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
        assert_eq!(order, [] as [&str; 0]);
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
                order.contains(&"light"),
                "'light' must always appear in the permutation"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Fairness keys (issue #1976)
    // -----------------------------------------------------------------------

    #[test]
    fn explicit_key_wins_over_the_quota_key() {
        assert_eq!(fairness_key_for(Some("a"), Some("q")).as_deref(), Some("a"));
        assert_eq!(fairness_key_for(None, Some("q")).as_deref(), Some("q"));
        assert_eq!(fairness_key_for(None, None), None);
    }

    #[test]
    fn an_invalid_quota_key_is_not_a_fairness_key() {
        for bad in ["", " acme", "acme\u{1f}", ".", ".."] {
            assert_eq!(fairness_key_for(None, Some(bad)), None, "{bad:?}");
        }
        let at_cap = "q".repeat(MAX_FAIRNESS_KEY_LEN);
        assert_eq!(fairness_key_for(None, Some(&at_cap)), Some(at_cap.clone()));
        // A quota key at the quota cap is a valid fairness key.
        assert_eq!(
            u64::try_from(MAX_FAIRNESS_KEY_LEN).unwrap(),
            crate::quota::MAX_QUOTA_KEY_BYTES
        );
    }

    #[test]
    fn fairness_key_validation_rejects_reserved_and_malformed_keys() {
        assert!(validate_fairness_key("tenant-a").is_ok());
        assert!(validate_fairness_key(&"x".repeat(MAX_FAIRNESS_KEY_LEN)).is_ok());
        assert!(validate_fairness_key(DEFAULT_FAIRNESS_KEY).is_err());
        assert!(validate_fairness_key(" a").is_err());
        assert!(validate_fairness_key("a\n").is_err());
        assert!(validate_fairness_key("a\u{1f}b").is_err());
        assert!(validate_fairness_key(".").is_err());
        assert!(validate_fairness_key("..").is_err());
        assert!(validate_fairness_key("...").is_ok());
        assert!(validate_fairness_key(&"x".repeat(MAX_FAIRNESS_KEY_LEN + 1)).is_err());
    }

    #[test]
    fn fairness_weight_validation_is_inclusive_at_both_bounds() {
        assert!(validate_fairness_weight(MIN_FAIRNESS_WEIGHT).is_ok());
        assert!(validate_fairness_weight(MAX_FAIRNESS_WEIGHT).is_ok());
        assert!(validate_fairness_weight(DEFAULT_FAIRNESS_WEIGHT).is_ok());
        for bad in [0.0, -1.0, 0.000_9, 1000.1, f64::NAN, f64::INFINITY] {
            assert!(validate_fairness_weight(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn a_flood_cannot_hold_a_new_key_for_more_than_one_claim() {
        let mut clock = FairClock::default();
        for _ in 0..1_000 {
            clock.charge("a", 1.0);
        }
        // A new key has lag 0; the flood's key has lag 1.
        assert!(clock.lag("b") < clock.lag("a"));
        assert_eq!(clock.pick([("a", 0u64), ("b", 9)]), Some("b"));
    }

    #[test]
    fn a_heavier_key_gets_its_weight_in_claims() {
        let mut clock = FairClock::default();
        let mut served = [0u32; 2];
        for _ in 0..400 {
            let k = clock.pick([("light", 0u8), ("heavy", 0u8)]).unwrap();
            served[usize::from(k == "heavy")] += 1;
            clock.charge(k, if k == "heavy" { 3.0 } else { 1.0 });
        }
        assert_eq!(served, [100, 300]);
    }
}
