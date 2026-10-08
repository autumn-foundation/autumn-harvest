//! Property tests for the fairness-key model (issue #1976).
//!
//! [`FairClock`] is the pure model of the fair claim. The SQL splice in
//! `queue.rs` mirrors it, and a DB test compares the two. These properties
//! are the spec:
//!
//! 1. A key with no debt is served within `N` claims, where `N` is the
//!    number of other active keys. A flood cannot delay it more.
//! 2. Two backlogged keys stay within `1/w_i + 1/w_j` in `served / weight`.
//! 3. The queue clock `V` never decreases, whatever key is charged.
//! 4. With one key, the claim order is the due order.
//! 5. Prune changes no start tag and does not move `V`.

use std::collections::{BTreeMap, VecDeque};

use autumn_harvest::queue_fairness::FairClock;
use proptest::prelude::*;

use super::prop_config::config;

/// Weights that cover fractions, one and large values.
fn weight() -> impl Strategy<Value = f64> {
    prop_oneof![
        Just(0.001),
        Just(0.1),
        Just(0.5),
        Just(1.0),
        Just(2.0),
        Just(3.0),
        Just(7.0),
        Just(1000.0),
        0.001f64..1000.0,
    ]
}

/// Key names `k0..kN`.
fn key(i: usize) -> String {
    format!("k{i}")
}

/// Claim the next row from per-key backlogs and charge its key.
///
/// Each backlog is a list of due times, oldest first. Returns the served key.
fn claim_next(
    clock: &mut FairClock,
    backlogs: &mut BTreeMap<String, VecDeque<u64>>,
    weights: &BTreeMap<String, f64>,
) -> Option<String> {
    let rows: Vec<(&str, u64)> = backlogs
        .iter()
        .filter_map(|(k, due)| due.front().map(|d| (k.as_str(), *d)))
        .collect();
    let served = clock.pick(rows)?.to_owned();
    backlogs.get_mut(&served)?.pop_front();
    clock.charge(&served, weights[&served]);
    Some(served)
}

proptest! {
    #![proptest_config(config())]

    /// Property 1: a new key waits at most one claim per other active key.
    ///
    /// The flood is older than the new row, so the flood wins every tie.
    #[test]
    fn a_new_key_is_served_within_the_active_key_count(
        weights in proptest::collection::vec(weight(), 1..=6),
        new_weight in weight(),
        warmup in 0usize..300,
    ) {
        let mut clock = FairClock::default();
        let mut backlogs: BTreeMap<String, VecDeque<u64>> = BTreeMap::new();
        let mut w: BTreeMap<String, f64> = BTreeMap::new();
        for (i, wi) in weights.iter().enumerate() {
            backlogs.insert(key(i), (0..10_000).collect());
            w.insert(key(i), *wi);
        }
        for _ in 0..warmup {
            claim_next(&mut clock, &mut backlogs, &w);
        }

        let others = weights.len();
        backlogs.insert("new".to_owned(), VecDeque::from([u64::MAX]));
        w.insert("new".to_owned(), new_weight);

        let mut waited = 0usize;
        loop {
            let served = claim_next(&mut clock, &mut backlogs, &w).unwrap();
            if served == "new" {
                break;
            }
            waited += 1;
            prop_assert!(waited <= others, "new key waited {waited} claims, bound {others}");
        }
    }

    /// Property 1, returning key: idle time earns no credit. A key that
    /// comes back after a gap shares like a new key and gets no burst.
    #[test]
    fn a_returning_key_gets_no_burst(
        wa in weight(),
        wb in weight(),
        before in 0usize..300,
        idle in 0usize..300,
    ) {
        let mut clock = FairClock::default();
        let w: BTreeMap<String, f64> =
            [("a".to_owned(), wa), ("b".to_owned(), wb)].into_iter().collect();
        let mut backlogs: BTreeMap<String, VecDeque<u64>> = BTreeMap::new();
        backlogs.insert("a".to_owned(), (0..10_000).collect());
        backlogs.insert("b".to_owned(), (0..10_000).collect());
        for _ in 0..before {
            claim_next(&mut clock, &mut backlogs, &w);
        }
        // B goes idle. A takes every claim of the gap.
        backlogs.insert("b".to_owned(), VecDeque::new());
        for _ in 0..idle {
            claim_next(&mut clock, &mut backlogs, &w);
        }

        // B returns with a large backlog. Over the next window its lead in
        // `served / weight` must stay within one carried slot plus the pair
        // bound.
        backlogs.insert("b".to_owned(), (0..10_000).collect());
        let mut served_a = 0f64;
        let mut served_b = 0f64;
        for _ in 0..200 {
            match claim_next(&mut clock, &mut backlogs, &w).unwrap().as_str() {
                "a" => served_a += 1.0,
                _ => served_b += 1.0,
            }
        }
        let lead = served_b / wb - served_a / wa;
        prop_assert!(
            lead <= 2.0 / wb + 1.0 / wa + 1e-6,
            "returning key led by {lead} (wa={wa}, wb={wb})"
        );
    }

    /// Property 2: two backlogged keys stay within `1/w_i + 1/w_j`.
    #[test]
    fn backlogged_keys_share_by_weight(
        weights in proptest::collection::vec(weight(), 2..=6),
        claims in 1usize..2_000,
        due_skew in proptest::collection::vec(0u64..1_000, 6),
    ) {
        let mut clock = FairClock::default();
        let mut backlogs: BTreeMap<String, VecDeque<u64>> = BTreeMap::new();
        let mut w: BTreeMap<String, f64> = BTreeMap::new();
        for (i, wi) in weights.iter().enumerate() {
            let skew = due_skew[i];
            backlogs.insert(key(i), (0..5_000u64).map(|d| d + skew).collect());
            w.insert(key(i), *wi);
        }
        let mut served: BTreeMap<String, f64> = BTreeMap::new();
        for _ in 0..claims {
            let k = claim_next(&mut clock, &mut backlogs, &w).unwrap();
            *served.entry(k).or_default() += 1.0;
        }
        for (i, wi) in weights.iter().enumerate() {
            for (j, wj) in weights.iter().enumerate().skip(i + 1) {
                let si = served.get(&key(i)).copied().unwrap_or(0.0);
                let sj = served.get(&key(j)).copied().unwrap_or(0.0);
                let gap = (si / wi - sj / wj).abs();
                prop_assert!(
                    gap <= 1.0 / wi + 1.0 / wj + 1e-6,
                    "keys {i},{j}: gap {gap} > bound (wi={wi}, wj={wj})"
                );
            }
        }
    }

    /// Property 3: `V` never decreases, and no key's pass decreases.
    #[test]
    fn the_clock_never_moves_back(
        ops in proptest::collection::vec((0usize..5, weight()), 1..400),
    ) {
        let mut clock = FairClock::default();
        let mut last_v = clock.vclock();
        let mut last_pass: BTreeMap<String, f64> = BTreeMap::new();
        for (k, w) in ops {
            let name = key(k);
            let charged = clock.charge(&name, w);
            let v = clock.vclock();
            prop_assert!(v >= last_v, "V moved back: {last_v} -> {v}");
            if let Some(p) = last_pass.get(&name) {
                prop_assert!(charged.pass >= *p, "pass moved back for {name}");
            }
            prop_assert!(charged.pass > charged.last_start, "a charge must cost");
            last_pass.insert(name, charged.pass);
            last_v = v;
        }
    }

    /// Property 4: with one key, the claim order is the due order.
    #[test]
    fn one_key_keeps_the_due_order(
        dues in proptest::collection::vec(any::<u32>(), 1..200),
        w in weight(),
    ) {
        let mut clock = FairClock::default();
        let mut pending: Vec<u32> = dues.clone();
        let mut order = Vec::new();
        while !pending.is_empty() {
            let rows: Vec<(&str, u32)> = pending.iter().map(|d| ("only", *d)).collect();
            let picked = clock.pick_row(&rows).unwrap();
            order.push(pending.remove(picked));
            clock.charge("only", w);
        }
        let mut sorted = dues;
        sorted.sort_unstable();
        prop_assert_eq!(order, sorted);
    }

    /// Property 5: prune changes no start tag and does not move `V`.
    #[test]
    fn prune_is_invisible_to_the_claim(
        ops in proptest::collection::vec((0usize..8, weight()), 0..200),
        active in proptest::collection::vec(any::<bool>(), 8),
    ) {
        let mut clock = FairClock::default();
        for (k, w) in ops {
            clock.charge(&key(k), w);
        }
        let before: Vec<f64> = (0..8).map(|k| clock.start_tag(&key(k))).collect();
        let v_before = clock.vclock();
        let pruned = clock.prune(|name| {
            name.strip_prefix('k')
                .and_then(|n| n.parse::<usize>().ok())
                .is_some_and(|n| active[n])
        });
        let after: Vec<f64> = (0..8).map(|k| clock.start_tag(&key(k))).collect();
        prop_assert_eq!(clock.vclock(), v_before);
        prop_assert_eq!(before, after);
        prop_assert!(pruned <= 8);
    }
}

/// A flood of 1,000 rows from tenant A cannot delay tenant B by more than one
/// claim. This is the model form of the DB red test.
#[test]
fn a_flood_delays_a_new_tenant_by_at_most_one_claim() {
    let mut clock = FairClock::default();
    let w: BTreeMap<String, f64> = [("a".to_owned(), 1.0), ("b".to_owned(), 1.0)]
        .into_iter()
        .collect();
    let mut backlogs: BTreeMap<String, VecDeque<u64>> = BTreeMap::new();
    backlogs.insert("a".to_owned(), (0..1_000).collect());
    claim_next(&mut clock, &mut backlogs, &w);
    backlogs.insert("b".to_owned(), VecDeque::from([5_000]));
    let first = claim_next(&mut clock, &mut backlogs, &w).unwrap();
    let second = claim_next(&mut clock, &mut backlogs, &w).unwrap();
    assert!(
        first == "b" || second == "b",
        "B must be served within one claim; got {first}, {second}"
    );
}
