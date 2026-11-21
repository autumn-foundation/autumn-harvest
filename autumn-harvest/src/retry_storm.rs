//! Retry storm forecast: how much load does an outage add?
//!
//! A [`RetryPolicy`] bounds the retries of one call. It does not bound the
//! retries of many calls together. During an outage, every failing call
//! retries, so the dependency sees more attempts than calls. When the outage
//! ends, the deferred work lands on a dependency that has only just come back.
//!
//! This module replays a steady stream of calls through one outage. It
//! reports the attempts in each time bucket, split into first attempts and
//! retries. The peak bucket against the arrival rate is the amplification
//! that the dependency must survive.
//!
//! The forecast uses the real parts of the engine:
//!
//! - Each retry delay comes from [`RetryPolicy::next_delay_with_seed`]. The
//!   seed is the call index, so jitter spreads calls the way it spreads tasks.
//! - A [`RetryBudgetPolicy`] runs through a real [`RetryBudgetRegistry`]. A
//!   retry that the budget defers waits and keeps its attempt, as on a worker.
//!
//! ## Model
//!
//! - Calls arrive at a constant rate over `traffic`, from time zero.
//! - An attempt fails when it starts inside `[outage_start, outage_end)`. Any
//!   other attempt succeeds.
//! - An attempt takes no time. A failure schedules the next attempt at once.
//! - One budget covers all calls, as for one activity type on one worker.
//!
//! The model ignores `schedule_to_close`. Use [`crate::retry_envelope`] for
//! the attempts that a deadline cuts.
//!
//! ## Example
//!
//! ```rust
//! use std::time::Duration;
//! use autumn_harvest::policy::{JitterPolicy, RetryPolicy};
//! use autumn_harvest::retry_storm::{StormScenario, simulate};
//!
//! let policy = RetryPolicy::fixed(5, Duration::from_secs(1)).with_jitter(JitterPolicy::None);
//! let scenario = StormScenario {
//!     arrivals_per_sec: 100.0,
//!     traffic: Duration::from_secs(20),
//!     outage_start: Duration::ZERO,
//!     outage_end: Duration::from_secs(10),
//!     bucket: Duration::from_secs(1),
//!     horizon: Duration::from_secs(600),
//! };
//! let report = simulate(&policy, None, &scenario).unwrap();
//! // Five attempts for each call: the outage sends five times the load.
//! assert_eq!(report.peak_ratio(), 5.0);
//! ```
//!
//! The module sends no SQL. It reads the clock once, as the base instant of
//! the budget. No result depends on that instant.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::time::{Duration, Instant};

use crate::policy::{RetryBudgetPolicy, RetryPolicy};
use crate::retry_budget::{
    Admission, MAX_RETRY_BUDGET_DEFER, RetryBudgetConfig, RetryBudgetRegistry,
};

/// The largest number of events that one simulation processes.
///
/// An event is an arrival, an attempt or a budget deferral. The cap keeps a
/// huge rate or a long horizon from running without end.
pub const MAX_SIMULATED_EVENTS: u64 = 5_000_000;

/// The activity name of the simulated budget. Only one type is simulated.
const STORM_ACTIVITY: &str = "retry_storm";

/// One outage scenario.
#[derive(Debug, Clone, PartialEq)]
pub struct StormScenario {
    /// First attempts each second.
    pub arrivals_per_sec: f64,
    /// Calls arrive over `[0, traffic)`.
    pub traffic: Duration,
    /// The first instant at which the dependency fails.
    pub outage_start: Duration,
    /// The first instant at which the dependency works again.
    pub outage_end: Duration,
    /// The width of one report bucket.
    pub bucket: Duration,
    /// The simulation stops here. Later work counts as pending.
    pub horizon: Duration,
}

/// The attempts that start in one bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StormBucket {
    /// The start of the bucket.
    pub start: Duration,
    /// First attempts that start in the bucket.
    pub first_attempts: u64,
    /// Retries that start in the bucket.
    pub retries: u64,
    /// Attempts in the bucket that fail.
    pub failures: u64,
}

impl StormBucket {
    /// All attempts that start in the bucket.
    #[must_use]
    pub const fn attempts(&self) -> u64 {
        self.first_attempts + self.retries
    }
}

/// The result of one retry storm simulation.
#[derive(Debug, Clone, PartialEq)]
pub struct StormReport {
    /// The buckets from time zero to the last attempt, in order.
    pub buckets: Vec<StormBucket>,
    /// The first attempts that one bucket gets without an outage.
    pub baseline_per_bucket: f64,
    /// The calls that arrive.
    pub calls: u64,
    /// The retries that start.
    pub retries: u64,
    /// The calls that end with a successful attempt.
    pub succeeded: u64,
    /// The calls whose last permitted attempt fails.
    pub exhausted: u64,
    /// The times that the budget defers a retry. One retry can defer often.
    pub deferrals: u64,
    /// The calls with work still due at the horizon.
    pub pending_at_horizon: u64,
    /// The start of the last attempt before the horizon.
    pub last_attempt: Option<Duration>,
}

/// Why a scenario cannot be simulated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StormError {
    /// The arrival rate is not a finite, positive number.
    #[error("the arrival rate must be finite and above zero")]
    InvalidRate,
    /// The bucket width is zero.
    #[error("the bucket width must be above zero")]
    ZeroBucket,
    /// The outage ends before it starts.
    #[error("the outage must not end before it starts")]
    OutageEndsBeforeStart,
    /// The scenario needs more than [`MAX_SIMULATED_EVENTS`] events.
    #[error("the scenario needs more than {MAX_SIMULATED_EVENTS} events")]
    TooManyEvents,
}

impl StormReport {
    /// All attempts that start before the horizon.
    #[must_use]
    pub fn attempts(&self) -> u64 {
        self.buckets.iter().map(StormBucket::attempts).sum()
    }

    /// Attempts for each call. A value of 1 means that no call retries.
    ///
    /// It is 0 when no call arrives.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn amplification(&self) -> f64 {
        let first: u64 = self.buckets.iter().map(|b| b.first_attempts).sum();
        if first == 0 {
            return 0.0;
        }
        self.attempts() as f64 / first as f64
    }

    /// The bucket with the most attempts. A tie goes to the earliest bucket.
    #[must_use]
    pub fn peak(&self) -> Option<&StormBucket> {
        self.buckets.iter().rev().max_by_key(|b| b.attempts())
    }

    /// The peak bucket against the baseline. A value of 3 means three times
    /// the load that the dependency sees without an outage.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn peak_ratio(&self) -> f64 {
        if self.baseline_per_bucket <= 0.0 {
            return 0.0;
        }
        self.peak()
            .map_or(0.0, |b| b.attempts() as f64 / self.baseline_per_bucket)
    }

    /// When the storm is over: the last attempt, if no work is pending.
    #[must_use]
    pub const fn drained_at(&self) -> Option<Duration> {
        if self.pending_at_horizon > 0 {
            return None;
        }
        self.last_attempt
    }
}

/// Simulate `scenario` under `policy`, and under `budget` when one is given.
///
/// # Errors
///
/// Returns a [`StormError`] when the scenario is invalid or too large.
pub fn simulate(
    policy: &RetryPolicy,
    budget: Option<&RetryBudgetPolicy>,
    scenario: &StormScenario,
) -> Result<StormReport, StormError> {
    validate(scenario)?;
    let rate = scenario.arrivals_per_sec;
    let width = nanos(scenario.bucket);
    let horizon = nanos(scenario.horizon);
    let calls = arrivals_before(rate, nanos(scenario.traffic))?;
    if horizon / width >= MAX_SIMULATED_EVENTS {
        return Err(StormError::TooManyEvents);
    }
    let anchor = Instant::now();
    // The budget reads time as an `Instant`. A horizon past its range cannot
    // be simulated with a budget.
    if budget.is_some() && anchor.checked_add(scenario.horizon).is_none() {
        return Err(StormError::TooManyEvents);
    }
    let registry = budget
        .map(|b| RetryBudgetRegistry::new(RetryBudgetConfig::disabled().with_default(Some(*b))));
    let outage = nanos(scenario.outage_start)..nanos(scenario.outage_end);

    let mut report = StormReport {
        buckets: Vec::new(),
        baseline_per_bucket: rate * scenario.bucket.as_secs_f64(),
        calls,
        retries: 0,
        succeeded: 0,
        exhausted: 0,
        deferrals: 0,
        pending_at_horizon: 0,
        last_attempt: None,
    };
    // Each call has at most one due event: its next attempt.
    let mut due: BinaryHeap<Reverse<(u64, u64, u32)>> = BinaryHeap::new();
    let mut next_call = 0;
    let mut events = 0_u64;
    loop {
        let arrival = (next_call < calls).then(|| arrival_at(rate, next_call));
        let event = match (arrival, due.peek()) {
            (None, None) => break,
            (Some(at), Some(Reverse(head))) if head.0 <= at => due.pop().map(|Reverse(e)| e),
            (Some(at), _) => {
                next_call += 1;
                Some((at, next_call - 1, 1))
            }
            (None, Some(_)) => due.pop().map(|Reverse(e)| e),
        };
        let Some((at, call, attempt)) = event else {
            break;
        };
        if at >= horizon {
            // This event, the events still due and the calls yet to arrive
            // all have work past the horizon.
            report.pending_at_horizon = 1 + due.len() as u64 + (calls - next_call);
            break;
        }
        events += 1;
        if events > MAX_SIMULATED_EVENTS {
            return Err(StormError::TooManyEvents);
        }
        if let Some(registry) = &registry {
            let now = anchor + Duration::from_nanos(at);
            match registry.admit(STORM_ACTIVITY, attempt > 1, now) {
                Admission::Admitted { ticket, .. } => {
                    registry.commit(STORM_ACTIVITY, ticket, now);
                }
                Admission::Deferred {
                    retry_after,
                    reservation,
                    ..
                } => {
                    report.deferrals += 1;
                    let delay = match reservation {
                        Some(_) => retry_after,
                        None => overflow_delay(call, report.deferrals),
                    };
                    due.push(Reverse((at.saturating_add(nanos(delay)), call, attempt)));
                    continue;
                }
                Admission::Untracked => {}
            }
        }
        let bucket = record(&mut report.buckets, at, width);
        report.last_attempt = Some(Duration::from_nanos(at));
        if attempt == 1 {
            bucket.first_attempts += 1;
        } else {
            bucket.retries += 1;
            report.retries += 1;
        }
        if !outage.contains(&at) {
            report.succeeded += 1;
            continue;
        }
        bucket.failures += 1;
        match policy.next_delay_with_seed(attempt, call) {
            Some(delay) => due.push(Reverse((
                at.saturating_add(nanos(delay)),
                call,
                attempt + 1,
            ))),
            None => report.exhausted += 1,
        }
    }
    Ok(report)
}

/// Refuse a scenario that the simulator cannot model.
fn validate(scenario: &StormScenario) -> Result<(), StormError> {
    let rate = scenario.arrivals_per_sec;
    if !rate.is_finite() || rate <= 0.0 {
        return Err(StormError::InvalidRate);
    }
    if scenario.bucket.is_zero() {
        return Err(StormError::ZeroBucket);
    }
    if scenario.outage_end < scenario.outage_start {
        return Err(StormError::OutageEndsBeforeStart);
    }
    Ok(())
}

/// A seeded delay in the upper half of [`MAX_RETRY_BUDGET_DEFER`].
///
/// The registry gives an overflow deferral a random delay in that range. The
/// simulator draws from the same range with a seed, so each run is the same.
fn overflow_delay(call: u64, deferral: u64) -> Duration {
    let half = MAX_RETRY_BUDGET_DEFER / 2;
    let seed = call.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ deferral;
    let spread = mix64(seed) % (nanos(half) + 1);
    half + Duration::from_nanos(spread)
}

/// The `SplitMix64` finalizer: a fast, well-spread hash of one word.
const fn mix64(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// The bucket that holds `at`. Missing buckets up to it are added empty.
fn record(buckets: &mut Vec<StormBucket>, at: u64, width: u64) -> &mut StormBucket {
    let index = usize::try_from(at / width).unwrap_or(usize::MAX);
    while buckets.len() <= index {
        let start = width.saturating_mul(buckets.len() as u64);
        buckets.push(StormBucket {
            start: Duration::from_nanos(start),
            ..StormBucket::default()
        });
    }
    &mut buckets[index]
}

/// The number of calls that arrive before `traffic` nanoseconds.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    reason = "the estimate is checked against the cap before the cast"
)]
fn arrivals_before(rate: f64, traffic: u64) -> Result<u64, StormError> {
    let estimate = (traffic as f64 / 1e9 * rate).ceil();
    if estimate >= MAX_SIMULATED_EVENTS as f64 {
        return Err(StormError::TooManyEvents);
    }
    let mut calls = estimate as u64;
    while calls > 0 && arrival_at(rate, calls - 1) >= traffic {
        calls -= 1;
    }
    Ok(calls)
}

/// The arrival time of call `index`, in nanoseconds.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    reason = "the index is below the event cap and the rate is positive"
)]
fn arrival_at(rate: f64, index: u64) -> u64 {
    (index as f64 * 1e9 / rate).round() as u64
}

fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::JitterPolicy;

    const fn secs(s: u64) -> Duration {
        Duration::from_secs(s)
    }

    fn scenario(outage: (u64, u64)) -> StormScenario {
        StormScenario {
            arrivals_per_sec: 100.0,
            traffic: secs(20),
            outage_start: secs(outage.0),
            outage_end: secs(outage.1),
            bucket: secs(1),
            horizon: secs(600),
        }
    }

    fn fixed(attempts: u32) -> RetryPolicy {
        RetryPolicy::fixed(attempts, secs(1)).with_jitter(JitterPolicy::None)
    }

    #[test]
    fn no_outage_means_no_retries() {
        let report = simulate(&fixed(5), None, &scenario((0, 0))).unwrap();
        assert_eq!(report.calls, 2000);
        assert_eq!(report.retries, 0);
        assert_eq!(report.succeeded, 2000);
        assert!((report.amplification() - 1.0).abs() < 1e-12);
        assert!((report.peak_ratio() - 1.0).abs() < 1e-12);
    }

    #[test]
    fn a_long_outage_multiplies_load_by_the_attempt_count() {
        let report = simulate(&fixed(5), None, &scenario((0, 10))).unwrap();
        // Inside the outage, each second carries 100 first attempts and the
        // retries of the four previous seconds of arrivals.
        let interior = &report.buckets[6];
        assert_eq!(interior.first_attempts, 100);
        assert_eq!(interior.retries, 400);
        assert_eq!(interior.failures, 500);
        assert!((report.peak_ratio() - 5.0).abs() < 1e-12);
        // A call that arrives in [0, 6) fails all five attempts.
        assert_eq!(report.exhausted, 600);
        assert_eq!(report.succeeded + report.exhausted, report.calls);
        assert_eq!(report.pending_at_horizon, 0);
    }

    #[test]
    fn exponential_backoff_lands_the_last_retry_after_recovery() {
        let policy = RetryPolicy::exponential(8, secs(1)).with_jitter(JitterPolicy::None);
        let mut s = scenario((0, 30));
        s.traffic = secs(1);
        let report = simulate(&policy, None, &s).unwrap();
        // Retries fall at 1, 3, 7, 15 and 31 s. Only the 31 s retry is after
        // recovery, so every call succeeds there.
        assert_eq!(report.succeeded, 100);
        assert_eq!(report.buckets[31].retries, 100);
        assert_eq!(report.buckets[31].failures, 0);
        assert_eq!(report.drained_at(), Some(Duration::from_millis(31_990)));
    }

    #[test]
    fn full_jitter_flattens_the_herd() {
        let bare = RetryPolicy::fixed(10, secs(5)).with_jitter(JitterPolicy::None);
        let jittered = bare.clone().with_jitter(JitterPolicy::Full);
        let mut s = scenario((0, 120));
        s.arrivals_per_sec = 1000.0;
        s.traffic = Duration::from_millis(100);
        let bare = simulate(&bare, None, &s).unwrap();
        let jittered = simulate(&jittered, None, &s).unwrap();
        // Without jitter, the burst retries as one block every 5 s. Full
        // jitter spreads each call on its own schedule.
        let peak_retries = |r: &StormReport| r.buckets.iter().map(|b| b.retries).max();
        assert_eq!(peak_retries(&bare), Some(100));
        assert!(
            peak_retries(&jittered) < Some(60),
            "{:?}",
            peak_retries(&jittered)
        );
    }

    #[test]
    fn a_retry_budget_caps_retry_load_and_defers_the_rest() {
        let budget = RetryBudgetPolicy::new(0.1, 10.0, 1.0);
        let bare = simulate(&fixed(5), None, &scenario((0, 10))).unwrap();
        let capped = simulate(&fixed(5), Some(&budget), &scenario((0, 10))).unwrap();
        assert!(capped.retries < bare.retries);
        assert!(capped.deferrals > 0);
        // Deposits of 10 per second plus 1 per second of refill fund the
        // retries. The full bucket adds 10 more.
        let outage_retries: u64 = capped.buckets[..10].iter().map(|b| b.retries).sum();
        assert!(outage_retries <= 10 * 11 + 10, "{outage_retries}");
        // A deferral uses no attempt, so fewer calls exhaust.
        assert!(capped.exhausted < bare.exhausted);
    }

    #[test]
    fn work_past_the_horizon_is_reported_as_pending() {
        let mut s = scenario((0, 1000));
        s.horizon = secs(30);
        let report = simulate(&RetryPolicy::fixed(0, secs(1)), None, &s).unwrap();
        assert_eq!(report.exhausted, report.calls);
        let report = simulate(&RetryPolicy::fixed(100, secs(1)), None, &s).unwrap();
        assert_eq!(report.pending_at_horizon, report.calls);
        assert_eq!(report.drained_at(), None);
    }

    #[test]
    fn the_simulation_is_deterministic() {
        let policy = RetryPolicy::exponential(6, secs(1));
        let budget = RetryBudgetPolicy::default();
        let a = simulate(&policy, Some(&budget), &scenario((2, 12))).unwrap();
        let b = simulate(&policy, Some(&budget), &scenario((2, 12))).unwrap();
        assert_eq!(a, b);
        assert!(a.deferrals > 0);
    }

    #[test]
    fn invalid_scenarios_are_refused() {
        let mut s = scenario((5, 1));
        assert_eq!(
            simulate(&fixed(3), None, &s),
            Err(StormError::OutageEndsBeforeStart)
        );
        s = scenario((0, 1));
        s.arrivals_per_sec = f64::NAN;
        assert_eq!(simulate(&fixed(3), None, &s), Err(StormError::InvalidRate));
        s = scenario((0, 1));
        s.bucket = Duration::ZERO;
        assert_eq!(simulate(&fixed(3), None, &s), Err(StormError::ZeroBucket));
        s = scenario((0, 1));
        s.arrivals_per_sec = 1e9;
        assert_eq!(
            simulate(&fixed(3), None, &s),
            Err(StormError::TooManyEvents)
        );
    }
}
