//! Retry envelope analysis: which configured attempts can actually run.
//!
//! A [`RetryPolicy`] promises `max_attempts` attempts. A `schedule_to_close`
//! deadline bounds every attempt and every backoff together. The two settings
//! live in different places, so a policy can promise attempts that the deadline
//! never lets run. This module walks the backoff schedule and reports, for each
//! attempt, the earliest and the latest time it can start.
//!
//! The earliest start assumes that each attempt fails at once and that jitter
//! picks the shortest delay. The latest start assumes that each attempt runs to
//! its `start_to_close` bound and that jitter picks the longest delay. Both
//! bounds hold for every jitter seed, so the report needs no sampling.
//!
//! The analysis covers the delays of the policy only. A failure with
//! `ActivityFailure::with_retry_after` replaces the policy delay with its
//! hint. That hint can be shorter or longer, so the bounds do not apply to it.
//!
//! The module is pure. It sends no SQL and reads no clock.

use std::time::Duration;

use crate::activity_defaults::ActivityDefaults;
use crate::policy::{JitterPolicy, RetryPolicy, compute_retry_delay};

/// The largest number of attempts that one analysis lists.
///
/// A policy with a huge `max_attempts` and zero-length backoffs never reaches
/// a deadline. This cap keeps the report finite.
pub const MAX_LISTED_ATTEMPTS: u32 = 10_000;

/// The timing window of one attempt. Offsets count from the schedule time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttemptWindow {
    /// The 1-based attempt number. Attempt 1 is the first try.
    pub attempt: u32,
    /// The shortest backoff before this attempt. It is zero for attempt 1.
    pub min_backoff: Duration,
    /// The longest backoff before this attempt. It is zero for attempt 1.
    pub max_backoff: Duration,
    /// The earliest time this attempt can start.
    pub earliest_start: Duration,
    /// The latest time this attempt can start.
    pub latest_start: Duration,
    /// The latest time this attempt can finish.
    ///
    /// It is `None` when no `start_to_close` bounds an attempt.
    pub latest_finish: Option<Duration>,
}

/// The result of a retry envelope analysis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryEnvelope {
    /// The attempts that the policy promises. A `max_attempts` of 0 counts as 1.
    pub configured_attempts: u32,
    /// The deadline on all attempts together, if any.
    pub schedule_to_close: Option<Duration>,
    /// The attempts that can start before the deadline, in order.
    pub attempts: Vec<AttemptWindow>,
    /// The first attempt whose base backoff reaches `max_interval`.
    ///
    /// Later attempts gain nothing from the backoff coefficient.
    pub saturates_at: Option<u32>,
    /// `true` when the list stops at [`MAX_LISTED_ATTEMPTS`].
    pub truncated: bool,
}

impl RetryEnvelope {
    /// The number of attempts that can start before the deadline.
    #[must_use]
    pub fn possible_attempts(&self) -> u32 {
        u32::try_from(self.attempts.len()).unwrap_or(u32::MAX)
    }

    /// The number of attempts that get a full `start_to_close` window.
    ///
    /// An attempt counts when its latest finish is at or before the deadline.
    /// With no deadline, every listed attempt counts.
    #[must_use]
    pub fn guaranteed_attempts(&self) -> u32 {
        let Some(deadline) = self.schedule_to_close else {
            return self.possible_attempts();
        };
        let count = self
            .attempts
            .iter()
            .take_while(|w| w.latest_finish.is_some_and(|f| f <= deadline))
            .count();
        u32::try_from(count).unwrap_or(u32::MAX)
    }

    /// The configured attempts that can never start.
    ///
    /// A value above zero means that the deadline cuts the retry policy short.
    #[must_use]
    pub fn unreachable_attempts(&self) -> u32 {
        if self.truncated {
            return 0;
        }
        self.configured_attempts
            .saturating_sub(self.possible_attempts())
    }

    /// The longest time from schedule to the final outcome.
    ///
    /// It is the latest finish of the last attempt, capped at the deadline.
    /// It is `None` when neither timeout bounds the last attempt.
    ///
    /// A truncated list omits attempts, so its last finish is not the end.
    /// Then only the deadline bounds the total.
    #[must_use]
    pub fn worst_case_total(&self) -> Option<Duration> {
        if self.truncated {
            return self.schedule_to_close;
        }
        let last = self.attempts.last().and_then(|w| w.latest_finish);
        match (last, self.schedule_to_close) {
            (Some(finish), Some(deadline)) => Some(finish.min(deadline)),
            (Some(finish), None) => Some(finish),
            (None, deadline) => deadline,
        }
    }
}

/// The shortest and the longest delay before retry `retry` of `policy`.
///
/// `retry` is 1-based, as in [`RetryPolicy::next_delay_with_seed`]. The bounds
/// hold for every seed of [`crate::policy::compute_retry_delay_with_seed`].
#[must_use]
pub fn retry_delay_bounds(policy: &RetryPolicy, retry: u32) -> (Duration, Duration) {
    let base_at = |n: u32| {
        compute_retry_delay(
            policy.initial_interval,
            policy.backoff_coefficient,
            policy.max_interval,
            n,
        )
    };
    let base = base_at(retry);
    let clamp_nanos =
        |d: Duration| Duration::from_nanos(u64::try_from(d.as_nanos()).unwrap_or(u64::MAX));
    match policy.jitter {
        JitterPolicy::None => (base, base),
        JitterPolicy::Full => (Duration::ZERO, clamp_nanos(base)),
        JitterPolicy::Equal => {
            let hi = clamp_nanos(base);
            if hi.as_nanos() <= 1 {
                (hi, hi)
            } else {
                (hi / 2, hi)
            }
        }
        JitterPolicy::Decorrelated => {
            let prev = if retry <= 1 {
                policy.initial_interval
            } else {
                base_at(retry - 1)
            };
            let upper = prev.saturating_mul(3).min(policy.max_interval);
            if upper <= policy.initial_interval {
                (upper, upper)
            } else {
                (clamp_nanos(policy.initial_interval), clamp_nanos(upper))
            }
        }
    }
}

/// Analyze the retry envelope of one activity.
///
/// `start_to_close` bounds one attempt. `schedule_to_close` bounds all of them
/// together. Pass `None` for a timeout that the activity does not set.
#[must_use]
pub fn analyze(
    policy: &RetryPolicy,
    start_to_close: Option<Duration>,
    schedule_to_close: Option<Duration>,
) -> RetryEnvelope {
    let configured_attempts = policy.max_attempts.max(1);
    let saturates_at = (1..configured_attempts)
        .take_while(|&retry| retry <= MAX_LISTED_ATTEMPTS)
        .find(|&retry| {
            compute_retry_delay(
                policy.initial_interval,
                policy.backoff_coefficient,
                policy.max_interval,
                retry,
            ) >= policy.max_interval
        })
        .map(|retry| retry + 1);

    let mut attempts = Vec::new();
    let mut earliest_start = Duration::ZERO;
    let mut latest_start = Duration::ZERO;
    let mut truncated = false;
    for attempt in 1..=configured_attempts {
        if attempt > MAX_LISTED_ATTEMPTS {
            truncated = true;
            break;
        }
        let (min_backoff, max_backoff) = if attempt == 1 {
            (Duration::ZERO, Duration::ZERO)
        } else {
            retry_delay_bounds(policy, attempt - 1)
        };
        earliest_start = earliest_start.saturating_add(min_backoff);
        latest_start = latest_start.saturating_add(max_backoff);
        // Attempt 1 starts at the schedule time, so a deadline never stops it.
        if attempt > 1 && schedule_to_close.is_some_and(|d| earliest_start >= d) {
            break;
        }
        let latest_finish = start_to_close.map(|bound| latest_start.saturating_add(bound));
        attempts.push(AttemptWindow {
            attempt,
            min_backoff,
            max_backoff,
            earliest_start,
            latest_start,
            latest_finish,
        });
        // The next attempt starts after this one ends, so its latest start
        // includes this attempt's full run. With no bound it is unbounded.
        latest_start = latest_finish.unwrap_or(Duration::MAX);
    }

    RetryEnvelope {
        configured_attempts,
        schedule_to_close,
        attempts,
        saturates_at,
        truncated,
    }
}

/// Analyze the retry envelope of the schedule-time values of one activity.
///
/// It returns `None` when the values set no retry policy.
#[must_use]
pub fn analyze_defaults(defaults: &ActivityDefaults) -> Option<RetryEnvelope> {
    defaults
        .retry_policy
        .as_ref()
        .map(|policy| analyze(policy, defaults.start_to_close, defaults.schedule_to_close))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::compute_retry_delay_with_seed;

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    fn exact(max_attempts: u32, initial: u64) -> RetryPolicy {
        RetryPolicy::exponential(max_attempts, secs(initial)).with_jitter(JitterPolicy::None)
    }

    #[test]
    fn no_deadline_lists_every_configured_attempt() {
        let env = analyze(&exact(4, 1), Some(secs(10)), None);
        assert_eq!(env.configured_attempts, 4);
        assert_eq!(env.possible_attempts(), 4);
        assert_eq!(env.guaranteed_attempts(), 4);
        assert_eq!(env.unreachable_attempts(), 0);
        // Backoffs 1, 2, 4 and four 10 s runs.
        assert_eq!(env.worst_case_total(), Some(secs(47)));
    }

    #[test]
    fn windows_follow_the_backoff_schedule() {
        let env = analyze(&exact(3, 1), Some(secs(10)), None);
        let starts: Vec<_> = env
            .attempts
            .iter()
            .map(|w| (w.earliest_start, w.latest_start, w.latest_finish))
            .collect();
        assert_eq!(
            starts,
            vec![
                (secs(0), secs(0), Some(secs(10))),
                (secs(1), secs(11), Some(secs(21))),
                (secs(3), secs(23), Some(secs(33))),
            ]
        );
    }

    #[test]
    fn deadline_cuts_attempts_the_policy_promises() {
        // Ten attempts with 60 s backoffs cannot fit in two minutes.
        let policy = RetryPolicy::fixed(10, secs(60)).with_jitter(JitterPolicy::None);
        let env = analyze(&policy, Some(secs(5)), Some(secs(120)));
        // Attempts start no earlier than 0, 60, 120 ...; 120 is not before the deadline.
        assert_eq!(env.possible_attempts(), 2);
        assert_eq!(env.unreachable_attempts(), 8);
        // Attempt 2 starts at 65 s at worst and ends by 70 s.
        assert_eq!(env.guaranteed_attempts(), 2);
        assert_eq!(env.worst_case_total(), Some(secs(70)));
    }

    #[test]
    fn guaranteed_counts_only_full_windows() {
        let policy = RetryPolicy::fixed(5, secs(10)).with_jitter(JitterPolicy::None);
        let env = analyze(&policy, Some(secs(30)), Some(secs(60)));
        // Earliest starts 0, 10, 20, 30, 40: all before 60 s.
        assert_eq!(env.possible_attempts(), 5);
        // Latest finishes 30, 70, ...: only attempt 1 fits in the worst case.
        assert_eq!(env.guaranteed_attempts(), 1);
        assert_eq!(env.worst_case_total(), Some(secs(60)));
    }

    #[test]
    fn full_jitter_lets_more_attempts_start() {
        let policy = RetryPolicy::fixed(4, secs(60)).with_jitter(JitterPolicy::Full);
        let env = analyze(&policy, Some(secs(1)), Some(secs(30)));
        // Full jitter can pick a zero delay, so every attempt can start.
        assert_eq!(env.possible_attempts(), 4);
        assert_eq!(env.guaranteed_attempts(), 1);
    }

    #[test]
    fn zero_max_attempts_counts_as_one() {
        let env = analyze(&exact(0, 1), Some(secs(2)), Some(secs(1)));
        assert_eq!(env.configured_attempts, 1);
        assert_eq!(env.possible_attempts(), 1);
        assert_eq!(env.guaranteed_attempts(), 0);
        assert_eq!(env.worst_case_total(), Some(secs(1)));
    }

    #[test]
    fn unbounded_attempt_makes_later_starts_unbounded() {
        let env = analyze(&exact(3, 1), None, None);
        assert_eq!(env.attempts[0].latest_finish, None);
        assert_eq!(env.attempts[1].latest_start, Duration::MAX);
        assert_eq!(env.worst_case_total(), None);
    }

    #[test]
    fn unbounded_attempt_with_deadline_reports_the_deadline() {
        let env = analyze(&exact(3, 1), None, Some(secs(100)));
        assert_eq!(env.possible_attempts(), 3);
        assert_eq!(env.guaranteed_attempts(), 0);
        assert_eq!(env.worst_case_total(), Some(secs(100)));
    }

    #[test]
    fn saturation_names_the_first_capped_attempt() {
        let mut policy = exact(10, 1);
        policy.max_interval = secs(8);
        let env = analyze(&policy, Some(secs(1)), None);
        // Retries 1..4 have base delays 1, 2, 4, 8; retry 4 starts attempt 5.
        assert_eq!(env.saturates_at, Some(5));
        assert_eq!(env.attempts[4].max_backoff, secs(8));
    }

    #[test]
    fn huge_max_attempts_is_truncated() {
        let policy = RetryPolicy::fixed(u32::MAX, Duration::ZERO).with_jitter(JitterPolicy::None);
        let env = analyze(&policy, Some(secs(1)), Some(secs(1)));
        assert!(env.truncated);
        assert_eq!(env.possible_attempts(), MAX_LISTED_ATTEMPTS);
        assert_eq!(env.unreachable_attempts(), 0);
    }

    #[test]
    fn truncation_does_not_understate_the_worst_case() {
        let policy = RetryPolicy::fixed(MAX_LISTED_ATTEMPTS + 1, Duration::ZERO)
            .with_jitter(JitterPolicy::None);
        // The listed prefix ends at 10,000 s, but one more attempt is configured.
        let env = analyze(&policy, Some(secs(1)), None);
        assert!(env.truncated);
        assert_eq!(env.worst_case_total(), None);
        // A deadline past the listed prefix is the only safe bound.
        let env = analyze(&policy, Some(secs(1)), Some(secs(20_000)));
        assert!(env.truncated);
        assert_eq!(env.worst_case_total(), Some(secs(20_000)));
    }

    #[test]
    fn defaults_without_a_policy_have_no_envelope() {
        assert_eq!(analyze_defaults(&ActivityDefaults::default()), None);
        let defaults = ActivityDefaults {
            retry_policy: Some(exact(3, 1)),
            start_to_close: Some(secs(10)),
            schedule_to_close: Some(secs(15)),
            ..ActivityDefaults::default()
        };
        let env = analyze_defaults(&defaults).expect("a policy is set");
        assert_eq!(env.possible_attempts(), 3);
        assert_eq!(env.guaranteed_attempts(), 1);
    }

    #[test]
    fn delay_bounds_contain_every_seeded_delay() {
        let jitters = [
            JitterPolicy::None,
            JitterPolicy::Full,
            JitterPolicy::Equal,
            JitterPolicy::Decorrelated,
        ];
        for jitter in jitters {
            let mut policy = RetryPolicy::exponential(12, Duration::from_millis(250));
            policy.max_interval = secs(20);
            let policy = policy.with_jitter(jitter);
            for retry in 1..12 {
                let (lo, hi) = retry_delay_bounds(&policy, retry);
                assert!(lo <= hi, "{jitter:?} retry {retry}");
                for seed in 0..200u64 {
                    let seed = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15);
                    let delay = compute_retry_delay_with_seed(&policy, retry, seed);
                    assert!(
                        lo <= delay && delay <= hi,
                        "{jitter:?} retry {retry} seed {seed}: {delay:?} not in [{lo:?}, {hi:?}]"
                    );
                }
            }
        }
    }
}
