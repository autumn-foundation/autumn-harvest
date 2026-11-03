//! Guards that the corpus retry timing is exact (issue #2517). No Docker.
//!
//! [`RetryPolicy::fixed`] draws each delay with full jitter. The Postgres
//! worker applies the jitter. `SQLite` waits the base delay. A jittered
//! backoff can thus give the two backends different histories.
//!
//! The Postgres worker seeds the jitter from random ids. A guard therefore
//! checks a sample of seeds, not one run.

use std::time::Duration;

use autumn_harvest::policy::{RetryPolicy, compute_retry_delay};

use crate::corpus;

/// The number of seeds that each guard checks.
const SEEDS: u64 = 1_000;

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
fn every_corpus_retry_waits_its_base_delay_for_every_seed() {
    for info in corpus::all_activities() {
        let Some(policy) = &info.default_retry_policy else {
            continue;
        };
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
