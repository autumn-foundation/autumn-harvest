//! Guards that the corpus retry timing is exact (issue #2517). No Docker.
//!
//! [`RetryPolicy::fixed`] draws each delay with full jitter. The Postgres
//! worker applies the jitter. `SQLite` waits the base delay. A jittered
//! backoff can thus give the two backends different histories.
//!
//! The Postgres worker seeds the jitter from random ids. A guard therefore
//! checks a sample of seeds, not one run.

use std::collections::BTreeSet;
use std::time::Duration;

use autumn_harvest::policy::{RetryPolicy, compute_retry_delay};

use crate::corpus;

/// The number of seeds that each guard checks.
const SEEDS: u64 = 1_000;

/// The corpus source. Each `#[activity]` in it must be in the guard list.
const CORPUS: &str = include_str!("corpus.rs");

/// The function name after each `#[activity(...)]` attribute of the corpus.
fn declared_activities() -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    let mut lines = CORPUS.lines();
    while let Some(line) = lines.next() {
        if !line.trim_start().starts_with("#[activity(") {
            continue;
        }
        let name = lines
            .find_map(|l| l.split_once("fn ").map(|(_, rest)| rest))
            .and_then(|rest| rest.split('(').next())
            .expect("a function follows each `#[activity]` attribute");
        names.insert(name.trim().to_owned());
    }
    names
}

/// Spread sample `i` over the whole `u64` range.
const fn seed(i: u64) -> u64 {
    i.wrapping_mul(0x9e37_79b9_7f4a_7c15)
}

/// The delay before retry `attempt`, with no jitter.
fn base_delay(policy: &RetryPolicy, attempt: u32) -> Duration {
    compute_retry_delay(
        policy.initial_interval,
        policy.backoff_coefficient,
        policy.max_interval,
        attempt,
    )
}

#[test]
fn every_corpus_activity_is_in_the_guard_list() {
    let listed: BTreeSet<String> = corpus::all_activities()
        .iter()
        .map(|info| info.name.to_owned())
        .collect();
    assert_eq!(
        declared_activities(),
        listed,
        "`corpus::all_activities` must name each `#[activity]` of `corpus.rs`"
    );
}

#[test]
fn every_corpus_retry_waits_its_base_delay_for_every_seed() {
    for info in corpus::all_activities() {
        // With no policy, Postgres jitters a one-second retry, and `SQLite`
        // retries at once.
        let policy = info.default_retry_policy.as_ref().unwrap_or_else(|| {
            panic!(
                "activity `{}` needs a retry policy. Without one, the backends differ",
                info.name
            )
        });
        for attempt in 1..policy.max_attempts {
            let want = base_delay(policy, attempt);
            for i in 0..SEEDS {
                assert_eq!(
                    policy.next_delay_with_seed(attempt, seed(i)),
                    Some(want),
                    "activity `{}`, retry {attempt}: the delay must not depend on the seed. \
                     Set `JitterPolicy::None`",
                    info.name
                );
            }
        }
    }
}

/// Regression for issue #2517: the losing activity retried before the timer.
#[test]
fn the_race_loser_retries_after_the_race_timer() {
    let policy = corpus::very_slow_retry_info()
        .default_retry_policy
        .expect("`very_slow_retry` has a retry policy");
    // The corpus rule: a raced timer has one second to spare.
    let earliest = corpus::RACE_TIMER + Duration::from_secs(1);
    for i in 0..SEEDS {
        let delay = policy
            .next_delay_with_seed(1, seed(i))
            .expect("`very_slow_retry` retries once");
        assert!(
            delay >= earliest,
            "seed {i}: the retry comes after {delay:?}, before {earliest:?}"
        );
    }
}
