//! Breaker tape: replay a traffic trace through a real circuit breaker.
//!
//! A [`CircuitBreakerPolicy`] has three knobs, and each one trades a cost
//! against another. A low threshold trips fast but wastes few calls. A long
//! cooldown shields the dependency but keeps a healed dependency dark. Sparse
//! traffic makes every recovery slower, because only a call can probe.
//! [`crate::retry_ride_through`] asks how one retrying call meets an outage.
//! This module asks how one breaker policy meets a whole traffic trace.
//!
//! ## Model
//!
//! - A tape is a list of call times and a list of outages.
//! - A call that starts inside an outage fails with a retryable error. Any
//!   other call succeeds.
//! - A call completes at the instant that it starts.
//!
//! The replay drives the production [`CircuitBreakerRegistry`], not a copy of
//! its rules. So the tape shows the real trip, probe and recovery behavior,
//! including the generation fence and the rolling window.
//!
//! ## Example
//!
//! ```rust
//! use std::time::Duration;
//! use autumn_harvest::breaker_tape::{Outage, replay};
//! use autumn_harvest::policy::CircuitBreakerPolicy;
//!
//! let s = Duration::from_secs;
//! let policy = CircuitBreakerPolicy::new(3, s(60), s(5));
//! let calls: Vec<Duration> = (0..60).map(s).collect();
//! let report = replay(policy, &calls, &[Outage::new(s(10), s(30))]);
//! let score = &report.outages[0];
//! // Calls at 10 s, 11 s and 12 s fail, so the breaker trips at 12 s.
//! assert_eq!(score.detection_lag, Some(s(2)));
//! // The first probe after the outage runs at 32 s and closes the breaker.
//! assert_eq!(score.recovery_lag, Some(s(2)));
//! ```
//!
//! The module is pure. It sends no SQL, and its results do not depend on the
//! clock. It reads the clock once, only to anchor the tape offsets.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::circuit_breaker::{
    AttemptOutcome, CircuitBreakerRegistry, CircuitTransition, DispatchDecision,
};
use crate::policy::CircuitBreakerPolicy;

/// The activity name under which the replay registers its one breaker.
const TAPE_ACTIVITY: &str = "breaker_tape";

/// One period in which the dependency fails every call.
///
/// The period starts at `start` and ends before `end`. Offsets count from the
/// start of the tape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Outage {
    /// The first instant of the outage.
    pub start: Duration,
    /// The first healthy instant after the outage.
    pub end: Duration,
}

impl Outage {
    /// Construct an outage from `start` up to, but not including, `end`.
    #[must_use]
    pub const fn new(start: Duration, end: Duration) -> Self {
        Self { start, end }
    }

    /// `true` when a call at `at` starts inside this outage.
    #[must_use]
    pub fn contains(&self, at: Duration) -> bool {
        self.start <= at && at < self.end
    }
}

/// What the breaker did with one call of the tape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// The breaker let the call through to the dependency.
    Dispatched {
        /// `true` when the dependency was healthy at the call time.
        healthy: bool,
        /// `true` when the breaker admitted the call as its half-open probe.
        probe: bool,
    },
    /// The breaker stopped the call before it reached the dependency.
    ShortCircuited {
        /// `true` when the dependency was healthy at the call time.
        healthy: bool,
    },
}

/// One call of the tape and the breaker response to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TapeStep {
    /// The call time, from the start of the tape.
    pub at: Duration,
    /// What the breaker did with the call.
    pub verdict: Verdict,
    /// The open or closed transition that the call caused, if any.
    pub transition: Option<CircuitTransition>,
}

/// The breaker performance during one outage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutageScore {
    /// The outage that this score describes.
    pub outage: Outage,
    /// The time from the outage start to the first trip inside it.
    ///
    /// It is zero when the breaker is already open at the outage start. It is
    /// `None` when the breaker stays closed for the whole outage.
    pub detection_lag: Option<Duration>,
    /// The time from the outage end until the breaker closes again.
    ///
    /// It is zero when the breaker is closed at the outage end. It is `None`
    /// when the breaker does not close before the tape ends.
    pub recovery_lag: Option<Duration>,
    /// The calls inside the outage that reached the dead dependency.
    pub wasted: usize,
    /// The calls inside the outage that the breaker stopped.
    pub shielded: usize,
}

/// The result of one replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TapeReport {
    /// The policy that the replay used.
    pub policy: CircuitBreakerPolicy,
    /// Each call in time order.
    pub steps: Vec<TapeStep>,
    /// One score for each outage, in the input order.
    pub outages: Vec<OutageScore>,
}

impl TapeReport {
    /// The calls that reached the dependency while it was down.
    #[must_use]
    pub fn wasted(&self) -> usize {
        self.count(|v| matches!(v, Verdict::Dispatched { healthy: false, .. }))
    }

    /// The calls that the breaker stopped while the dependency was down.
    #[must_use]
    pub fn shielded(&self) -> usize {
        self.count(|v| matches!(v, Verdict::ShortCircuited { healthy: false }))
    }

    /// The calls that the breaker stopped while the dependency was healthy.
    ///
    /// Each one is the price of a slow recovery.
    #[must_use]
    pub fn blocked_healthy(&self) -> usize {
        self.count(|v| matches!(v, Verdict::ShortCircuited { healthy: true }))
    }

    /// The number of times that the breaker opened.
    #[must_use]
    pub fn trips(&self) -> usize {
        self.steps
            .iter()
            .filter(|s| s.transition == Some(CircuitTransition::Tripped))
            .count()
    }

    fn count(&self, pred: impl Fn(Verdict) -> bool) -> usize {
        self.steps.iter().filter(|s| pred(s.verdict)).count()
    }

    /// `true` when the breaker is open just before `at`.
    ///
    /// The last transition before `at` decides. A tape with no earlier
    /// transition starts closed.
    fn open_before(&self, at: Duration) -> bool {
        self.steps
            .iter()
            .take_while(|s| s.at < at)
            .filter_map(|s| s.transition)
            .last()
            == Some(CircuitTransition::Tripped)
    }

    fn score(&self, outage: Outage) -> OutageScore {
        let inside = || self.steps.iter().filter(|s| outage.contains(s.at));
        let detection_lag = if self.open_before(outage.start) {
            Some(Duration::ZERO)
        } else {
            inside()
                .find(|s| s.transition == Some(CircuitTransition::Tripped))
                .map(|s| s.at - outage.start)
        };
        let recovery_lag = if self.open_before(outage.end) {
            self.steps
                .iter()
                .find(|s| s.at >= outage.end && s.transition == Some(CircuitTransition::Closed))
                .map(|s| s.at - outage.end)
        } else {
            Some(Duration::ZERO)
        };
        OutageScore {
            outage,
            detection_lag,
            recovery_lag,
            wasted: inside()
                .filter(|s| matches!(s.verdict, Verdict::Dispatched { .. }))
                .count(),
            shielded: inside()
                .filter(|s| matches!(s.verdict, Verdict::ShortCircuited { .. }))
                .count(),
        }
    }
}

/// Replay `calls` through one breaker with `policy`, against `outages`.
///
/// The calls may come in any order. The replay sorts them first. Outages may
/// overlap, and a call inside any of them fails.
#[must_use]
pub fn replay(policy: CircuitBreakerPolicy, calls: &[Duration], outages: &[Outage]) -> TapeReport {
    let registry = CircuitBreakerRegistry::new(HashMap::from([(TAPE_ACTIVITY.to_owned(), policy)]));
    let base = Instant::now();
    let mut times = calls.to_vec();
    times.sort_unstable();
    let steps = times
        .into_iter()
        .map(|at| {
            let now = base + at;
            let healthy = !outages.iter().any(|o| o.contains(at));
            match registry.on_dispatch(TAPE_ACTIVITY, now) {
                DispatchDecision::Allow { token } => {
                    let outcome = if healthy {
                        AttemptOutcome::Success
                    } else {
                        AttemptOutcome::RetryableFailure
                    };
                    TapeStep {
                        at,
                        verdict: Verdict::Dispatched {
                            healthy,
                            probe: token.is_probe(),
                        },
                        transition: registry.on_result(TAPE_ACTIVITY, outcome, token, now),
                    }
                }
                DispatchDecision::ShortCircuit { .. } => TapeStep {
                    at,
                    verdict: Verdict::ShortCircuited { healthy },
                    transition: None,
                },
            }
        })
        .collect();
    let mut report = TapeReport {
        policy,
        steps,
        outages: Vec::new(),
    };
    report.outages = outages.iter().map(|&o| report.score(o)).collect();
    report
}

/// Call times from `start` up to, but not including, `end`, one each `every`.
///
/// It returns no call when `every` is zero.
#[must_use]
pub fn steady_calls(start: Duration, end: Duration, every: Duration) -> Vec<Duration> {
    if every.is_zero() {
        return Vec::new();
    }
    std::iter::successors(Some(start), |&t| t.checked_add(every))
        .take_while(|&t| t < end)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    fn every_second(end: u64) -> Vec<Duration> {
        steady_calls(Duration::ZERO, s(end), s(1))
    }

    #[test]
    fn a_healthy_tape_never_trips() {
        let report = replay(
            CircuitBreakerPolicy::new(1, s(60), s(5)),
            &every_second(30),
            &[],
        );
        assert_eq!(report.steps.len(), 30);
        assert_eq!(report.trips(), 0);
        assert_eq!(report.wasted(), 0);
        assert_eq!(report.shielded(), 0);
        assert_eq!(report.blocked_healthy(), 0);
    }

    #[test]
    fn a_dense_outage_trips_probes_and_recovers() {
        let outage = Outage::new(s(10), s(30));
        let report = replay(
            CircuitBreakerPolicy::new(3, s(60), s(5)),
            &every_second(60),
            &[outage],
        );
        // Trip at 12 s, then failed probes at 17 s, 22 s and 27 s.
        assert_eq!(report.trips(), 4);
        // Three calls before the trip and three probes reach the dead dependency.
        assert_eq!(report.wasted(), 6);
        assert_eq!(report.shielded(), 14);
        // Calls at 30 s and 31 s wait for the probe at 32 s.
        assert_eq!(report.blocked_healthy(), 2);
        let score = report.outages[0];
        assert_eq!(score.detection_lag, Some(s(2)));
        assert_eq!(score.recovery_lag, Some(s(2)));
        assert_eq!((score.wasted, score.shielded), (6, 14));
    }

    #[test]
    fn probe_steps_are_marked() {
        let report = replay(
            CircuitBreakerPolicy::new(3, s(60), s(5)),
            &every_second(60),
            &[Outage::new(s(10), s(30))],
        );
        let probes: Vec<Duration> = report
            .steps
            .iter()
            .filter(|st| matches!(st.verdict, Verdict::Dispatched { probe: true, .. }))
            .map(|st| st.at)
            .collect();
        assert_eq!(probes, vec![s(17), s(22), s(27), s(32)]);
    }

    #[test]
    fn sparse_traffic_slows_recovery() {
        let calls = steady_calls(Duration::ZERO, s(60), s(10));
        let report = replay(
            CircuitBreakerPolicy::new(1, s(60), s(5)),
            &calls,
            &[Outage::new(Duration::ZERO, s(25))],
        );
        // The cooldown is 5 s, but no call probes until 30 s.
        assert_eq!(report.outages[0].recovery_lag, Some(s(5)));
        assert_eq!(report.outages[0].detection_lag, Some(Duration::ZERO));
    }

    #[test]
    fn an_outage_past_the_tape_never_recovers() {
        let report = replay(
            CircuitBreakerPolicy::new(1, s(60), s(5)),
            &every_second(20),
            &[Outage::new(s(5), s(100))],
        );
        assert_eq!(report.outages[0].recovery_lag, None);
    }

    #[test]
    fn a_short_outage_is_not_detected() {
        let report = replay(
            CircuitBreakerPolicy::new(5, s(60), s(5)),
            &every_second(20),
            &[Outage::new(s(5), s(7))],
        );
        let score = report.outages[0];
        assert_eq!(score.detection_lag, None);
        assert_eq!(score.recovery_lag, Some(Duration::ZERO));
        assert_eq!((score.wasted, score.shielded), (2, 0));
    }

    #[test]
    fn a_second_outage_inside_the_cooldown_is_detected_at_once() {
        let first = Outage::new(s(2), s(5));
        let second = Outage::new(s(6), s(10));
        let report = replay(
            CircuitBreakerPolicy::new(2, s(60), s(10)),
            &every_second(20),
            &[first, second],
        );
        // Calls at 2 s and 3 s trip the breaker. The probe at 13 s succeeds.
        assert_eq!(report.outages[0].detection_lag, Some(s(1)));
        assert_eq!(report.outages[0].recovery_lag, Some(s(8)));
        assert_eq!(report.outages[1].detection_lag, Some(Duration::ZERO));
        assert_eq!(report.outages[1].wasted, 0);
        assert_eq!(report.outages[1].recovery_lag, Some(s(3)));
    }

    #[test]
    fn a_narrow_window_never_trips_on_slow_failures() {
        let calls = steady_calls(Duration::ZERO, s(40), s(2));
        let outage = [Outage::new(Duration::ZERO, s(100))];
        let narrow = replay(CircuitBreakerPolicy::new(3, s(3), s(5)), &calls, &outage);
        assert_eq!(narrow.trips(), 0);
        assert_eq!(narrow.wasted(), calls.len());
        let wide = replay(CircuitBreakerPolicy::new(3, s(4), s(5)), &calls, &outage);
        assert_eq!(wide.outages[0].detection_lag, Some(s(4)));
    }

    #[test]
    fn unsorted_calls_replay_in_time_order() {
        let mut calls = every_second(60);
        calls.reverse();
        let report = replay(
            CircuitBreakerPolicy::new(3, s(60), s(5)),
            &calls,
            &[Outage::new(s(10), s(30))],
        );
        assert!(report.steps.windows(2).all(|w| w[0].at <= w[1].at));
        assert_eq!(report.outages[0].detection_lag, Some(s(2)));
    }

    #[test]
    fn steady_calls_handles_edges() {
        assert!(steady_calls(Duration::ZERO, s(10), Duration::ZERO).is_empty());
        assert!(steady_calls(s(5), s(5), s(1)).is_empty());
        assert_eq!(steady_calls(s(1), s(4), s(1)), vec![s(1), s(2), s(3)]);
    }
}
