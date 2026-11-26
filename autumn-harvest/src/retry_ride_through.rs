//! Outage ride-through: how long an outage can one retrying call survive?
//!
//! [`crate::retry_storm`] asks how much load an outage adds over many calls.
//! [`crate::retry_envelope`] asks which attempts a deadline lets start. This
//! module joins the two questions for one call. It asks whether the call
//! still succeeds when its dependency is down for a given time.
//!
//! ## Model
//!
//! - The outage starts at the schedule time and lasts `outage`.
//! - An attempt fails when it starts inside the outage. Any other attempt
//!   succeeds.
//! - A failed attempt takes `failure_cost` before the next backoff starts.
//!   Use zero for a refused connection. Use `start_to_close` for a hang.
//! - An attempt after the first one starts only before `schedule_to_close`,
//!   as in [`crate::retry_envelope`].
//!
//! Each start time is a window. The earliest start takes the shortest jitter
//! delay at each retry. The latest start takes the longest. So every bound
//! below holds for every jitter seed, and the report needs no sampling.
//!
//! ## Example
//!
//! ```rust
//! use std::time::Duration;
//! use autumn_harvest::policy::{JitterPolicy, RetryPolicy};
//! use autumn_harvest::retry_ride_through::{Recovery, profile};
//!
//! let policy = RetryPolicy::fixed(4, Duration::from_secs(10)).with_jitter(JitterPolicy::None);
//! let ride = profile(&policy, Duration::ZERO, Some(Duration::from_secs(60)));
//! // The fourth attempt starts at 30 s, so a 30 s outage is always survived.
//! assert_eq!(ride.guaranteed_ride_through(), Duration::from_secs(30));
//! // A 31 s outage outlasts every attempt.
//! assert_eq!(ride.recovery(Duration::from_secs(31)), Recovery::Exhausted);
//! ```
//!
//! The module is pure. It sends no SQL and reads no clock.

use std::time::Duration;

use crate::policy::RetryPolicy;
use crate::retry_envelope::{MAX_LISTED_ATTEMPTS, retry_delay_bounds};

/// The start window of one attempt during an outage.
///
/// Offsets count from the schedule time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartWindow {
    /// The 1-based attempt number. Attempt 1 is the first try.
    pub attempt: u32,
    /// The earliest time this attempt can start.
    pub earliest: Duration,
    /// The latest time this attempt can start.
    pub latest: Duration,
}

/// The outcome of one call through an outage of a given length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recovery {
    /// Every jitter seed gives a healthy attempt before the deadline.
    Guaranteed {
        /// The earliest time that a healthy attempt can start.
        earliest: Duration,
        /// The latest time that the first healthy attempt can start.
        latest: Duration,
    },
    /// Some jitter seeds give a healthy attempt. Other seeds use up the
    /// attempts or reach the deadline first.
    Possible {
        /// The earliest time that a healthy attempt can start.
        earliest: Duration,
    },
    /// No jitter seed gives a healthy attempt. The call fails.
    Exhausted,
    /// The outage outlasts the listed attempts, but the policy has more.
    ///
    /// The list stops at [`MAX_LISTED_ATTEMPTS`], so the outcome is unknown.
    BeyondHorizon,
}

/// The start windows of one call through an outage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RideThroughProfile {
    /// The deadline on all attempts together, if any.
    pub schedule_to_close: Option<Duration>,
    /// The time that each failed attempt takes.
    pub failure_cost: Duration,
    /// The attempts that can start before the deadline, in order.
    pub windows: Vec<StartWindow>,
    /// `true` when the list stops at [`MAX_LISTED_ATTEMPTS`].
    pub truncated: bool,
}

impl RideThroughProfile {
    /// The outcome of one call through an outage of length `outage`.
    #[must_use]
    pub fn recovery(&self, outage: Duration) -> Recovery {
        // The first attempt that can start after the outage gives the best case.
        let Some(first) = self.windows.iter().find(|w| w.latest >= outage) else {
            return if self.truncated {
                Recovery::BeyondHorizon
            } else {
                Recovery::Exhausted
            };
        };
        let earliest = first.earliest.max(outage);
        if first.attempt > 1 && self.schedule_to_close.is_some_and(|d| earliest >= d) {
            return Recovery::Exhausted;
        }
        // The first attempt that must start after the outage bounds the worst case.
        match self.windows.iter().find(|w| w.earliest >= outage) {
            Some(sure) if self.starts_for_sure(sure) => Recovery::Guaranteed {
                earliest,
                latest: sure.latest,
            },
            _ => Recovery::Possible { earliest },
        }
    }

    /// `true` when every jitter seed starts `window` before the deadline.
    fn starts_for_sure(&self, window: &StartWindow) -> bool {
        window.attempt == 1 || self.schedule_to_close.is_none_or(|d| window.latest < d)
    }

    /// The windows that every jitter seed starts before the deadline.
    fn sure_windows(&self) -> impl Iterator<Item = &StartWindow> {
        self.windows.iter().take_while(|w| self.starts_for_sure(w))
    }

    /// The longest outage that every jitter seed survives.
    ///
    /// It is zero when no retry is sure to start before the deadline.
    #[must_use]
    pub fn guaranteed_ride_through(&self) -> Duration {
        self.sure_windows()
            .last()
            .map_or(Duration::ZERO, |w| w.earliest)
    }

    /// The longest time that a healed dependency can wait for the next try.
    ///
    /// It covers only the outages that every seed survives. It is `None`
    /// when the profile has no guaranteed retry.
    #[must_use]
    pub fn worst_recovery_lag(&self) -> Option<Duration> {
        // An outage that ends just after the earliest start of one attempt can
        // wait until the latest start of the next attempt.
        let sure: Vec<_> = self.sure_windows().collect();
        sure.windows(2)
            .map(|pair| pair[1].latest.saturating_sub(pair[0].earliest))
            .max()
    }
}

/// Build the ride-through profile of one call.
///
/// `failure_cost` is the time that one failed attempt takes. Pass `None` for
/// `schedule_to_close` when the activity sets no deadline.
#[must_use]
pub fn profile(
    policy: &RetryPolicy,
    failure_cost: Duration,
    schedule_to_close: Option<Duration>,
) -> RideThroughProfile {
    let configured_attempts = policy.max_attempts.max(1);
    let mut windows = Vec::new();
    let mut earliest = Duration::ZERO;
    let mut latest = Duration::ZERO;
    let mut truncated = false;
    for attempt in 1..=configured_attempts {
        if attempt > MAX_LISTED_ATTEMPTS {
            truncated = true;
            break;
        }
        if attempt > 1 {
            let (min_backoff, max_backoff) = retry_delay_bounds(policy, attempt - 1);
            earliest = earliest
                .saturating_add(failure_cost)
                .saturating_add(min_backoff);
            latest = latest
                .saturating_add(failure_cost)
                .saturating_add(max_backoff);
            if schedule_to_close.is_some_and(|d| earliest >= d) {
                break;
            }
        }
        windows.push(StartWindow {
            attempt,
            earliest,
            latest,
        });
    }
    RideThroughProfile {
        schedule_to_close,
        failure_cost,
        windows,
        truncated,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::JitterPolicy;

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    fn fixed(attempts: u32, delay: u64, jitter: JitterPolicy) -> RetryPolicy {
        RetryPolicy::fixed(attempts, secs(delay)).with_jitter(jitter)
    }

    #[test]
    fn windows_add_backoff_and_failure_cost() {
        let ride = profile(&fixed(3, 10, JitterPolicy::None), secs(5), None);
        let starts: Vec<_> = ride
            .windows
            .iter()
            .map(|w| (w.earliest, w.latest))
            .collect();
        assert_eq!(
            starts,
            vec![
                (secs(0), secs(0)),
                (secs(15), secs(15)),
                (secs(30), secs(30))
            ]
        );
        assert!(!ride.truncated);
    }

    #[test]
    fn deadline_drops_attempts_that_cannot_start() {
        let ride = profile(
            &fixed(10, 10, JitterPolicy::None),
            Duration::ZERO,
            Some(secs(25)),
        );
        let attempts: Vec<_> = ride.windows.iter().map(|w| w.attempt).collect();
        assert_eq!(attempts, vec![1, 2, 3]);
    }

    #[test]
    fn exact_backoff_gives_a_sharp_ride_through() {
        let ride = profile(
            &fixed(4, 10, JitterPolicy::None),
            Duration::ZERO,
            Some(secs(60)),
        );
        assert_eq!(ride.guaranteed_ride_through(), secs(30));
        assert_eq!(
            ride.recovery(secs(30)),
            Recovery::Guaranteed {
                earliest: secs(30),
                latest: secs(30)
            }
        );
        assert_eq!(ride.recovery(secs(31)), Recovery::Exhausted);
    }

    #[test]
    fn a_zero_outage_recovers_on_the_first_attempt() {
        let ride = profile(&fixed(1, 10, JitterPolicy::None), Duration::ZERO, None);
        assert_eq!(
            ride.recovery(Duration::ZERO),
            Recovery::Guaranteed {
                earliest: Duration::ZERO,
                latest: Duration::ZERO
            }
        );
        assert_eq!(ride.recovery(secs(1)), Recovery::Exhausted);
        assert_eq!(ride.guaranteed_ride_through(), Duration::ZERO);
        assert_eq!(ride.worst_recovery_lag(), None);
    }

    #[test]
    fn a_healthy_attempt_inside_a_window_recovers_at_the_outage_end() {
        // Equal jitter puts retry 2 in [5 s, 10 s].
        let ride = profile(&fixed(3, 10, JitterPolicy::Equal), Duration::ZERO, None);
        assert_eq!(
            ride.recovery(secs(7)),
            Recovery::Guaranteed {
                earliest: secs(7),
                latest: secs(20)
            }
        );
    }

    #[test]
    fn the_deadline_turns_a_sure_recovery_into_a_possible_one() {
        // Full jitter puts retry 2 in [0 s, 60 s]. The deadline is 30 s.
        let ride = profile(
            &fixed(2, 60, JitterPolicy::Full),
            Duration::ZERO,
            Some(secs(30)),
        );
        assert_eq!(
            ride.recovery(secs(10)),
            Recovery::Possible { earliest: secs(10) }
        );
        assert_eq!(ride.guaranteed_ride_through(), Duration::ZERO);
        assert_eq!(ride.worst_recovery_lag(), None);
    }

    #[test]
    fn a_hang_spends_attempts_but_stretches_their_reach() {
        let policy = fixed(4, 10, JitterPolicy::None);
        let fast = profile(&policy, Duration::ZERO, Some(secs(60)));
        let hang = profile(&policy, secs(15), Some(secs(60)));
        assert_eq!(fast.guaranteed_ride_through(), secs(30));
        // Starts at 0, 25 and 50 s. Attempt 4 would start at 75 s.
        assert_eq!(hang.guaranteed_ride_through(), secs(50));
        assert_eq!(hang.windows.len(), 3);
    }

    #[test]
    fn worst_lag_is_the_widest_gap_between_sure_starts() {
        // Exponential backoff: retries after 1, 2 and 4 s.
        let policy = RetryPolicy::exponential(4, secs(1)).with_jitter(JitterPolicy::None);
        let ride = profile(&policy, Duration::ZERO, None);
        assert_eq!(ride.worst_recovery_lag(), Some(secs(4)));
    }

    #[test]
    fn ride_through_is_the_last_outage_with_a_sure_recovery() {
        let policy = fixed(6, 10, JitterPolicy::Equal);
        let ride = profile(&policy, secs(1), Some(secs(40)));
        let limit = ride.guaranteed_ride_through();
        assert!(matches!(ride.recovery(limit), Recovery::Guaranteed { .. }));
        let longer = limit + Duration::from_millis(1);
        assert!(!matches!(
            ride.recovery(longer),
            Recovery::Guaranteed { .. }
        ));
    }

    #[test]
    fn an_unbounded_policy_reports_the_horizon() {
        let policy = fixed(u32::MAX, 0, JitterPolicy::None);
        let ride = profile(&policy, secs(1), None);
        assert!(ride.truncated);
        let past = secs(u64::from(MAX_LISTED_ATTEMPTS) + 1);
        assert_eq!(ride.recovery(past), Recovery::BeyondHorizon);
    }
}
