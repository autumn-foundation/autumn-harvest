//! Breaker forecast: how much does a circuit breaker shield a dependency?
//!
//! [`crate::retry_storm`] replays an outage with retries alone. Every failing
//! call keeps hitting the dead dependency until its attempts run out. A
//! [`CircuitBreakerPolicy`] cuts that load, but its knobs trade against each
//! other. A low threshold trips early. A long cooldown keeps the breaker open
//! after the dependency recovers, so good work waits or fails for nothing.
//!
//! This module replays the same steady call stream through one outage, with
//! a real [`CircuitBreakerRegistry`] in front of the dependency. It reports:
//!
//! - the attempts that reach the dependency, and the short circuits that the
//!   breaker absorbs, in each time bucket;
//! - each trip and each recovery of the breaker, in order;
//! - the detection lag, from the outage start to the first trip;
//! - the recovery lag, from the outage end to the breaker close.
//!
//! The open mode of the policy decides what a short circuit does. In
//! `FailFast` mode, the call ends with a non-retryable failure. In `Defer`
//! mode, the call waits and keeps its attempt, as on a worker. The wait uses
//! the clamps of the worker, so the forecast shows where the deferred
//! backlog lands after a recovery.
//!
//! ## Model
//!
//! The model is the model of [`crate::retry_storm`]:
//!
//! - Calls arrive at a constant rate over `traffic`, from time zero.
//! - An attempt fails when it starts inside `[outage_start, outage_end)`.
//! - An attempt takes no time, so a half-open probe resolves at once.
//! - Each retry delay comes from [`RetryPolicy::next_delay_with_seed`].
//!
//! ## Example
//!
//! ```rust
//! use std::time::Duration;
//! use autumn_harvest::breaker_forecast::simulate;
//! use autumn_harvest::policy::{CircuitBreakerPolicy, CircuitOpenMode, RetryPolicy};
//! use autumn_harvest::retry_storm::StormScenario;
//!
//! let breaker = CircuitBreakerPolicy::new(5, Duration::from_secs(10), Duration::from_secs(5))
//!     .with_open_mode(CircuitOpenMode::FailFast);
//! let scenario = StormScenario {
//!     arrivals_per_sec: 100.0,
//!     traffic: Duration::from_secs(20),
//!     outage_start: Duration::ZERO,
//!     outage_end: Duration::from_secs(10),
//!     bucket: Duration::from_secs(1),
//!     horizon: Duration::from_secs(600),
//! };
//! let report = simulate(&RetryPolicy::fixed(0, Duration::from_secs(1)), &breaker, &scenario)
//!     .unwrap();
//! // Without a breaker, 1000 calls fail against the dead dependency. The
//! // breaker lets 5 through to trip it, and one failed probe at 5.04 s.
//! assert_eq!(report.dependency_failures(), 6);
//! assert_eq!(report.recovery_lag(), Some(Duration::from_millis(40)));
//! ```
//!
//! The module sends no SQL. It reads the clock once, as the base instant of
//! the breaker. No result depends on that instant.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::time::{Duration, Instant};

use crate::circuit_breaker::{
    AttemptOutcome, CircuitBreakerRegistry, CircuitTransition, DispatchDecision,
};
use crate::policy::{CircuitBreakerPolicy, CircuitOpenMode, RetryPolicy};
use crate::retry_storm::{
    MAX_SIMULATED_EVENTS, StormError, StormScenario, arrival_at, arrivals_before, nanos, validate,
};

/// The activity name of the simulated breaker. Only one type is simulated.
const FORECAST_ACTIVITY: &str = "breaker_forecast";

/// The shortest deferral of the worker. It mirrors `CIRCUIT_DEFER_MIN`.
const DEFER_MIN: Duration = Duration::from_millis(100);
/// The longest deferral base of the worker. It mirrors `CIRCUIT_DEFER_MAX`.
const DEFER_MAX: Duration = Duration::from_secs(30);
/// The deferral base with no probe time. It mirrors `CIRCUIT_DEFER_UNSCHEDULED`.
const DEFER_UNSCHEDULED: Duration = Duration::from_secs(5);

/// The traffic of one bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BreakerBucket {
    /// The start of the bucket.
    pub start: Duration,
    /// First attempts that reach the dependency in the bucket.
    pub first_attempts: u64,
    /// Retries that reach the dependency in the bucket.
    pub retries: u64,
    /// Attempts in the bucket that reach the dependency and fail.
    pub failures: u64,
    /// Dispatches in the bucket that the open breaker stops.
    pub short_circuits: u64,
}

impl BreakerBucket {
    /// All attempts that reach the dependency in the bucket.
    #[must_use]
    pub const fn attempts(&self) -> u64 {
        self.first_attempts + self.retries
    }

    /// Count one attempt that reaches the dependency, and classify it.
    const fn count_attempt(&mut self, attempt: u32, failed: bool) -> AttemptOutcome {
        if attempt == 1 {
            self.first_attempts += 1;
        } else {
            self.retries += 1;
        }
        if failed {
            self.failures += 1;
            AttemptOutcome::RetryableFailure
        } else {
            AttemptOutcome::Success
        }
    }
}

/// One change of the breaker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BreakerChange {
    /// When the change happens.
    pub at: Duration,
    /// The change: a trip or a recovery.
    pub transition: CircuitTransition,
}

/// The result of one breaker forecast.
#[derive(Debug, Clone, PartialEq)]
pub struct BreakerReport {
    /// The buckets from time zero to the last dispatch, in order.
    pub buckets: Vec<BreakerBucket>,
    /// The first attempts that one bucket gets without an outage.
    pub baseline_per_bucket: f64,
    /// The trips and recoveries of the breaker, in order.
    pub changes: Vec<BreakerChange>,
    /// The start of the outage in the scenario.
    pub outage_start: Duration,
    /// The end of the outage in the scenario.
    pub outage_end: Duration,
    /// The calls that arrive.
    pub calls: u64,
    /// The calls that end with a successful attempt.
    pub succeeded: u64,
    /// The calls whose last permitted attempt fails at the dependency.
    pub exhausted: u64,
    /// The calls that a `FailFast` short circuit ends.
    pub fast_failed: u64,
    /// The times that a `Defer` short circuit delays a call.
    pub deferrals: u64,
    /// The calls with work still due at the horizon.
    pub pending_at_horizon: u64,
}

impl BreakerReport {
    /// All attempts that reach the dependency.
    #[must_use]
    pub fn dependency_attempts(&self) -> u64 {
        self.buckets.iter().map(BreakerBucket::attempts).sum()
    }

    /// All attempts that reach the dependency and fail.
    #[must_use]
    pub fn dependency_failures(&self) -> u64 {
        self.buckets.iter().map(|b| b.failures).sum()
    }

    /// All dispatches that the open breaker stops.
    #[must_use]
    pub fn short_circuits(&self) -> u64 {
        self.buckets.iter().map(|b| b.short_circuits).sum()
    }

    /// The bucket with the most dependency attempts. A tie goes to the
    /// earliest bucket.
    #[must_use]
    pub fn peak(&self) -> Option<&BreakerBucket> {
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

    /// The time from the outage start to the first trip.
    ///
    /// It is `None` when the breaker never trips.
    #[must_use]
    pub fn detection_lag(&self) -> Option<Duration> {
        self.changes
            .iter()
            .find(|c| c.transition == CircuitTransition::Tripped)
            .map(|c| c.at.saturating_sub(self.outage_start))
    }

    /// The time from the outage end to the last recovery of the breaker.
    ///
    /// It is `None` when the breaker never trips, or when it is still open
    /// at the end of the forecast.
    #[must_use]
    pub fn recovery_lag(&self) -> Option<Duration> {
        let last = self.changes.last()?;
        (last.transition == CircuitTransition::Closed)
            .then(|| last.at.saturating_sub(self.outage_end))
    }
}

/// Forecast `scenario` under `policy`, with `breaker` in front of the
/// dependency.
///
/// # Errors
///
/// Returns a [`StormError`] when the scenario is invalid or too large.
pub fn simulate(
    policy: &RetryPolicy,
    breaker: &CircuitBreakerPolicy,
    scenario: &StormScenario,
) -> Result<BreakerReport, StormError> {
    validate(scenario)?;
    let rate = scenario.arrivals_per_sec;
    let width = nanos(scenario.bucket);
    let horizon = nanos(scenario.horizon);
    let calls = arrivals_before(rate, nanos(scenario.traffic))?;
    if horizon / width >= MAX_SIMULATED_EVENTS {
        return Err(StormError::TooManyEvents);
    }
    // The breaker reads time as an `Instant`. A horizon past its range
    // cannot be simulated.
    let anchor = Instant::now();
    if anchor.checked_add(scenario.horizon).is_none() {
        return Err(StormError::TooManyEvents);
    }
    let registry =
        CircuitBreakerRegistry::new(HashMap::from([(FORECAST_ACTIVITY.to_owned(), *breaker)]));
    let outage = nanos(scenario.outage_start)..nanos(scenario.outage_end);

    let mut report = BreakerReport {
        buckets: Vec::new(),
        baseline_per_bucket: rate * scenario.bucket.as_secs_f64(),
        changes: Vec::new(),
        outage_start: scenario.outage_start,
        outage_end: scenario.outage_end,
        calls,
        succeeded: 0,
        exhausted: 0,
        fast_failed: 0,
        deferrals: 0,
        pending_at_horizon: 0,
    };
    // Each call has at most one due event: its next dispatch.
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
        let now = anchor + Duration::from_nanos(at);
        let bucket = record(&mut report.buckets, at, width);
        let token = match registry.on_dispatch(FORECAST_ACTIVITY, now) {
            DispatchDecision::Allow { token } => token,
            DispatchDecision::ShortCircuit { retry_after, .. } => {
                bucket.short_circuits += 1;
                match breaker.open_mode {
                    CircuitOpenMode::FailFast => report.fast_failed += 1,
                    CircuitOpenMode::Defer => {
                        report.deferrals += 1;
                        let delay = defer_delay(retry_after, breaker.cooldown, call, attempt);
                        due.push(Reverse((at.saturating_add(nanos(delay)), call, attempt)));
                    }
                }
                continue;
            }
        };
        let failed = outage.contains(&at);
        let outcome = bucket.count_attempt(attempt, failed);
        if let Some(transition) = registry.on_result(FORECAST_ACTIVITY, outcome, token, now) {
            report.changes.push(BreakerChange {
                at: Duration::from_nanos(at),
                transition,
            });
        }
        if !failed {
            report.succeeded += 1;
            continue;
        }
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

/// The delay of a call that an open breaker defers.
///
/// It follows `circuit_defer_delay` in the worker. The jitter seed is the
/// call index, so each run is the same.
fn defer_delay(
    retry_after: Option<Duration>,
    cooldown: Duration,
    call: u64,
    attempt: u32,
) -> Duration {
    let base = retry_after
        .unwrap_or_else(|| cooldown.max(DEFER_UNSCHEDULED))
        .clamp(DEFER_MIN, DEFER_MAX);
    base + crate::policy::full_jitter(base / 4, call, attempt)
}

/// The bucket that holds `at`. Missing buckets up to it are added empty.
fn record(buckets: &mut Vec<BreakerBucket>, at: u64, width: u64) -> &mut BreakerBucket {
    let index = usize::try_from(at / width).unwrap_or(usize::MAX);
    while buckets.len() <= index {
        let start = width.saturating_mul(buckets.len() as u64);
        buckets.push(BreakerBucket {
            start: Duration::from_nanos(start),
            ..BreakerBucket::default()
        });
    }
    &mut buckets[index]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::JitterPolicy;

    const fn secs(s: u64) -> Duration {
        Duration::from_secs(s)
    }

    const fn millis(ms: u64) -> Duration {
        Duration::from_millis(ms)
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

    fn no_retries() -> RetryPolicy {
        RetryPolicy::fixed(0, secs(1))
    }

    fn breaker(mode: CircuitOpenMode) -> CircuitBreakerPolicy {
        CircuitBreakerPolicy::new(5, secs(10), secs(5)).with_open_mode(mode)
    }

    #[test]
    fn no_outage_never_trips_the_breaker() {
        let report = simulate(
            &no_retries(),
            &breaker(CircuitOpenMode::FailFast),
            &scenario((0, 0)),
        )
        .unwrap();
        assert_eq!(report.calls, 2000);
        assert_eq!(report.succeeded, 2000);
        assert_eq!(report.short_circuits(), 0);
        assert_eq!(report.changes, Vec::new());
        assert_eq!(report.detection_lag(), None);
        assert_eq!(report.recovery_lag(), None);
    }

    #[test]
    fn fail_fast_trips_probes_and_recovers() {
        let report = simulate(
            &no_retries(),
            &breaker(CircuitOpenMode::FailFast),
            &scenario((0, 10)),
        )
        .unwrap();
        // Five failures trip at 40 ms. The probe at 5.04 s fails and re-trips.
        // The probe at 10.04 s finds the dependency back and closes.
        let changes: Vec<_> = report
            .changes
            .iter()
            .map(|c| (c.at, c.transition))
            .collect();
        assert_eq!(
            changes,
            vec![
                (millis(40), CircuitTransition::Tripped),
                (millis(5_040), CircuitTransition::Tripped),
                (millis(10_040), CircuitTransition::Closed),
            ]
        );
        assert_eq!(report.detection_lag(), Some(millis(40)));
        assert_eq!(report.recovery_lag(), Some(millis(40)));
        assert_eq!(report.dependency_failures(), 6);
        assert_eq!(report.exhausted, 6);
        // Calls 5 to 1003 short-circuit, except the probe at call 504.
        assert_eq!(report.fast_failed, 998);
        assert_eq!(report.short_circuits(), 998);
        assert_eq!(report.succeeded, 996);
        assert_eq!(
            report.succeeded + report.exhausted + report.fast_failed,
            report.calls
        );
    }

    #[test]
    fn defer_keeps_the_work_and_lands_it_after_recovery() {
        let report = simulate(
            &no_retries(),
            &breaker(CircuitOpenMode::Defer),
            &scenario((0, 10)),
        )
        .unwrap();
        assert_eq!(report.fast_failed, 0);
        assert_eq!(report.exhausted, 6);
        assert_eq!(report.succeeded, report.calls - 6);
        assert_eq!(report.pending_at_horizon, 0);
        assert!(report.deferrals >= 998, "{}", report.deferrals);
        // The deferred backlog lands on the dependency just after it closes.
        assert_eq!(report.recovery_lag(), Some(millis(40)));
        let peak = report.peak().unwrap();
        assert!(peak.start >= secs(10), "{peak:?}");
        assert!(report.peak_ratio() > 2.0, "{}", report.peak_ratio());
    }

    #[test]
    fn a_breaker_shields_the_dependency_from_a_retry_storm() {
        let policy = RetryPolicy::fixed(5, secs(1)).with_jitter(JitterPolicy::None);
        let storm = crate::retry_storm::simulate(&policy, None, &scenario((0, 10))).unwrap();
        let storm_failures: u64 = storm.buckets.iter().map(|b| b.failures).sum();
        let report = simulate(
            &policy,
            &breaker(CircuitOpenMode::FailFast),
            &scenario((0, 10)),
        )
        .unwrap();
        assert!(
            report.dependency_failures() * 100 < storm_failures,
            "{} against {storm_failures}",
            report.dependency_failures()
        );
        assert!(report.peak_ratio() <= storm.peak_ratio());
    }

    #[test]
    fn a_long_cooldown_delays_recovery() {
        let slow = CircuitBreakerPolicy::new(5, secs(10), secs(30))
            .with_open_mode(CircuitOpenMode::FailFast);
        let mut s = scenario((0, 10));
        s.traffic = secs(60);
        let report = simulate(&no_retries(), &slow, &s).unwrap();
        // The only probe comes at 30.04 s, 20 s after the dependency is back.
        assert_eq!(report.recovery_lag(), Some(millis(20_040)));
        assert!(report.fast_failed > 2000, "{}", report.fast_failed);
    }

    #[test]
    fn the_forecast_is_deterministic() {
        let policy = RetryPolicy::exponential(6, secs(1));
        let b = breaker(CircuitOpenMode::Defer);
        let first = simulate(&policy, &b, &scenario((2, 12))).unwrap();
        let second = simulate(&policy, &b, &scenario((2, 12))).unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn invalid_scenarios_are_refused() {
        let b = breaker(CircuitOpenMode::Defer);
        assert_eq!(
            simulate(&no_retries(), &b, &scenario((5, 1))),
            Err(StormError::OutageEndsBeforeStart)
        );
        let mut s = scenario((0, 1));
        s.bucket = Duration::ZERO;
        assert_eq!(simulate(&no_retries(), &b, &s), Err(StormError::ZeroBucket));
    }

    #[cfg(feature = "db")]
    #[test]
    fn the_defer_clamps_match_the_worker() {
        assert_eq!(DEFER_MIN, crate::worker::CIRCUIT_DEFER_MIN);
        assert_eq!(DEFER_MAX, crate::worker::CIRCUIT_DEFER_MAX);
        assert_eq!(DEFER_UNSCHEDULED, crate::worker::CIRCUIT_DEFER_UNSCHEDULED);
    }
}
