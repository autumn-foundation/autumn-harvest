//! Circuit breaker outage forecast: what does a breaker policy do in an outage?
//!
//! [`crate::circuit_breaker`] runs a breaker live, one dispatch at a time.
//! [`crate::retry_ride_through`] asks how long one retrying call survives an
//! outage. This module joins the two questions for the breaker. It replays
//! the breaker state machine against a steady stream of calls and a
//! dependency that is down for a given time.
//!
//! The report answers the questions that tune a policy:
//!
//! - When does the breaker trip, and how many calls hit the dead dependency?
//! - How many probes fail before the dependency heals?
//! - How long does a healed dependency stay shielded, and how many healthy
//!   calls does the breaker turn away in that time?
//!
//! ## Model
//!
//! - The outage starts at time zero and lasts `outage`.
//! - A call arrives every [`Traffic::interval`], the first one at time zero.
//! - A dispatched call reports [`Traffic::latency`] after its dispatch.
//! - A call fails with a retryable failure when it is dispatched inside the
//!   outage. Any other call succeeds.
//! - A result and a dispatch at the same instant: the result comes first.
//!
//! The transitions are those of [`crate::circuit_breaker`]. A result from an
//! older breaker generation changes nothing. One probe runs at a time. A
//! failed probe opens the breaker again at the time of its result. The model
//! counts each arrival as one dispatch decision. In
//! [`CircuitOpenMode::Defer`](crate::policy::CircuitOpenMode::Defer) mode a
//! short-circuited task goes back to the queue, so one task can arrive more
//! than once. The counts are then decisions, not distinct tasks.
//!
//! ## Example
//!
//! ```rust
//! use std::time::Duration;
//! use autumn_harvest::circuit_forecast::{Traffic, forecast};
//! use autumn_harvest::policy::CircuitBreakerPolicy;
//!
//! let secs = Duration::from_secs;
//! let policy = CircuitBreakerPolicy::new(5, secs(10), secs(30));
//! let traffic = Traffic::new(secs(1), Duration::ZERO);
//! let report = forecast(policy, traffic, secs(60)).expect("non-zero interval");
//! // Five failures trip the breaker at 4 s.
//! assert_eq!(report.tripped_at, Some(secs(4)));
//! // The probe at 34 s fails. The probe at 64 s closes the breaker.
//! assert_eq!(report.failed_probes, 1);
//! assert_eq!(report.closed_at, Some(secs(64)));
//! // The dependency heals at 60 s, so it stays shielded for 4 s.
//! assert_eq!(report.recovery_lag(), Some(secs(4)));
//! assert_eq!(report.shielded_after_recovery, 4);
//! ```
//!
//! The module is pure. It sends no SQL and reads no clock.

use std::collections::VecDeque;
use std::time::Duration;

use crate::circuit_breaker::CircuitPhase;
use crate::policy::CircuitBreakerPolicy;

/// The largest number of arrivals that one forecast replays.
///
/// A policy that never closes in this many arrivals gives a truncated report.
pub const MAX_ARRIVALS: u64 = 1_000_000;

/// A steady stream of calls to one activity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Traffic {
    /// The time between two arrivals. It must not be zero.
    pub interval: Duration,
    /// The time from a dispatch to its result.
    pub latency: Duration,
}

impl Traffic {
    /// Construct a stream of one call every `interval`.
    #[must_use]
    pub const fn new(interval: Duration, latency: Duration) -> Self {
        Self { interval, latency }
    }
}

/// What one breaker policy does through one outage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BreakerForecast {
    /// The length of the outage.
    pub outage: Duration,
    /// The first time that the breaker opens, if it opens.
    pub tripped_at: Option<Duration>,
    /// The time that the breaker closes after it opened, if it closes.
    pub closed_at: Option<Duration>,
    /// The arrivals that the forecast replays.
    pub arrivals: u64,
    /// The dispatched calls that hit the dead dependency, probes included.
    pub doomed: u64,
    /// The arrivals that the breaker turns away inside the outage.
    pub shielded_during_outage: u64,
    /// The arrivals that the breaker turns away after the dependency heals.
    pub shielded_after_recovery: u64,
    /// The half-open probes that the breaker admits.
    pub probes: u64,
    /// The probes that fail and open the breaker again.
    pub failed_probes: u64,
    /// `true` when the replay stops at [`MAX_ARRIVALS`].
    pub truncated: bool,
}

impl BreakerForecast {
    /// A report with no arrival yet.
    const fn empty(outage: Duration) -> Self {
        Self {
            outage,
            tripped_at: None,
            closed_at: None,
            arrivals: 0,
            doomed: 0,
            shielded_during_outage: 0,
            shielded_after_recovery: 0,
            probes: 0,
            failed_probes: 0,
            truncated: false,
        }
    }

    /// The time from the end of the outage to the close of the breaker.
    ///
    /// It is `None` when the breaker never opens or never closes.
    #[must_use]
    pub fn recovery_lag(&self) -> Option<Duration> {
        self.closed_at
            .map(|closed| closed.saturating_sub(self.outage))
    }

    /// The fraction of the outage arrivals that the breaker turns away.
    ///
    /// It is zero when no arrival falls inside the outage.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn outage_shield_ratio(&self) -> f64 {
        let inside = self.doomed + self.shielded_during_outage;
        if inside == 0 {
            return 0.0;
        }
        self.shielded_during_outage as f64 / inside as f64
    }
}

/// One dispatched call that has not reported yet.
#[derive(Debug, Clone, Copy)]
struct InFlight {
    reports_at: Duration,
    generation: u64,
    is_probe: bool,
    fails: bool,
}

/// The breaker state that the replay tracks.
///
/// It mirrors the fields of the live breaker that one stream of calls can
/// reach. An operator pin and a cancelled probe are out of scope.
#[derive(Debug)]
struct Replay {
    policy: CircuitBreakerPolicy,
    phase: CircuitPhase,
    failures: VecDeque<Duration>,
    opened_at: Duration,
    probe_in_flight: bool,
    generation: u64,
}

impl Replay {
    fn trip(&mut self, now: Duration) {
        self.phase = CircuitPhase::Open;
        self.opened_at = now;
        self.failures.clear();
        self.probe_in_flight = false;
        self.generation = self.generation.wrapping_add(1);
    }

    fn close(&mut self) {
        self.phase = CircuitPhase::Closed;
        self.failures.clear();
        self.probe_in_flight = false;
        self.generation = self.generation.wrapping_add(1);
    }

    /// Decide one arrival. It returns `Some(is_probe)` for a dispatch.
    fn dispatch(&mut self, now: Duration) -> Option<bool> {
        match self.phase {
            CircuitPhase::Closed => Some(false),
            CircuitPhase::Open if now.saturating_sub(self.opened_at) >= self.policy.cooldown => {
                self.phase = CircuitPhase::HalfOpen;
                self.probe_in_flight = true;
                Some(true)
            }
            CircuitPhase::Open => None,
            CircuitPhase::HalfOpen if self.probe_in_flight => None,
            CircuitPhase::HalfOpen => {
                self.probe_in_flight = true;
                Some(true)
            }
        }
    }

    /// Apply one result. It returns `true` when a probe fails.
    fn report(&mut self, call: InFlight) -> bool {
        if call.generation != self.generation {
            return false;
        }
        let now = call.reports_at;
        match self.phase {
            CircuitPhase::Closed if call.fails => {
                self.failures.push_back(now);
                while self
                    .failures
                    .front()
                    .is_some_and(|&front| now.saturating_sub(front) > self.policy.window)
                {
                    self.failures.pop_front();
                }
                if self.failures.len() >= self.policy.failure_threshold as usize {
                    self.trip(now);
                }
                false
            }
            CircuitPhase::Closed => {
                self.failures.clear();
                false
            }
            CircuitPhase::HalfOpen if call.is_probe && call.fails => {
                self.trip(now);
                true
            }
            CircuitPhase::HalfOpen if call.is_probe => {
                self.close();
                false
            }
            CircuitPhase::HalfOpen | CircuitPhase::Open => false,
        }
    }

    /// `true` when no later arrival or result can change the breaker.
    fn settled(
        &self,
        next_arrival: Duration,
        outage: Duration,
        pending: &VecDeque<InFlight>,
    ) -> bool {
        self.phase == CircuitPhase::Closed
            && next_arrival >= outage
            && !pending
                .iter()
                .any(|call| call.fails && call.generation == self.generation)
    }
}

/// Forecast what `policy` does when its dependency is down for `outage`.
///
/// It returns `None` when `traffic.interval` is zero. The replay stops when
/// the breaker is closed and no call can fail again, or at [`MAX_ARRIVALS`].
#[must_use]
pub fn forecast(
    policy: CircuitBreakerPolicy,
    traffic: Traffic,
    outage: Duration,
) -> Option<BreakerForecast> {
    if traffic.interval.is_zero() {
        return None;
    }
    let mut replay = Replay {
        policy,
        phase: CircuitPhase::Closed,
        failures: VecDeque::new(),
        opened_at: Duration::ZERO,
        probe_in_flight: false,
        generation: 0,
    };
    let mut report = BreakerForecast::empty(outage);
    // The latency is constant, so results come back in dispatch order.
    let mut pending: VecDeque<InFlight> = VecDeque::new();
    let mut next_arrival = Duration::ZERO;
    loop {
        if let Some(&call) = pending.front()
            && call.reports_at <= next_arrival
        {
            pending.pop_front();
            let was_open = replay.phase != CircuitPhase::Closed;
            report.failed_probes += u64::from(replay.report(call));
            if replay.phase == CircuitPhase::Open && report.tripped_at.is_none() {
                report.tripped_at = Some(call.reports_at);
            }
            if was_open && replay.phase == CircuitPhase::Closed {
                report.closed_at = Some(call.reports_at);
                break;
            }
            continue;
        }
        if replay.settled(next_arrival, outage, &pending) {
            break;
        }
        if report.arrivals >= MAX_ARRIVALS {
            report.truncated = true;
            break;
        }
        report.arrivals += 1;
        let fails = next_arrival < outage;
        match replay.dispatch(next_arrival) {
            Some(is_probe) => {
                report.probes += u64::from(is_probe);
                report.doomed += u64::from(fails);
                pending.push_back(InFlight {
                    reports_at: next_arrival.saturating_add(traffic.latency),
                    generation: replay.generation,
                    is_probe,
                    fails,
                });
            }
            None if fails => report.shielded_during_outage += 1,
            None => report.shielded_after_recovery += 1,
        }
        next_arrival = next_arrival.saturating_add(traffic.interval);
    }
    Some(report)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Instant;

    use super::*;
    use crate::circuit_breaker::{
        AttemptOutcome, CircuitBreakerRegistry, CircuitTransition, DispatchDecision, DispatchToken,
    };

    const NAME: &str = "charge_card";

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    fn run(policy: CircuitBreakerPolicy, traffic: Traffic, outage: Duration) -> BreakerForecast {
        forecast(policy, traffic, outage).expect("non-zero interval")
    }

    /// Drive the live registry with the same arrivals and results.
    ///
    /// It stops after the arrivals that the forecast replays, so the two
    /// runs see the same calls.
    fn live(
        policy: CircuitBreakerPolicy,
        traffic: Traffic,
        outage: Duration,
        arrivals: u64,
    ) -> BreakerForecast {
        let registry = CircuitBreakerRegistry::new(HashMap::from([(NAME.to_owned(), policy)]));
        let base = Instant::now();
        let mut report = BreakerForecast {
            arrivals,
            ..BreakerForecast::empty(outage)
        };
        let mut pending: VecDeque<(Duration, DispatchToken, bool)> = VecDeque::new();
        let mut next_arrival = Duration::ZERO;
        let mut seen = 0;
        loop {
            if let Some(&(at, token, fails)) = pending.front()
                && (at <= next_arrival || seen == arrivals)
            {
                pending.pop_front();
                let outcome = if fails {
                    AttemptOutcome::RetryableFailure
                } else {
                    AttemptOutcome::Success
                };
                match registry.on_result(NAME, outcome, token, base + at) {
                    Some(CircuitTransition::Tripped) => {
                        report.tripped_at.get_or_insert(at);
                        report.failed_probes += u64::from(token.is_probe());
                    }
                    Some(CircuitTransition::Closed) => {
                        report.closed_at = Some(at);
                        break;
                    }
                    None => {}
                }
                continue;
            }
            if seen == arrivals {
                break;
            }
            seen += 1;
            let fails = next_arrival < outage;
            match registry.on_dispatch(NAME, base + next_arrival) {
                DispatchDecision::Allow { token } => {
                    report.probes += u64::from(token.is_probe());
                    report.doomed += u64::from(fails);
                    pending.push_back((next_arrival + traffic.latency, token, fails));
                }
                DispatchDecision::ShortCircuit { .. } if fails => {
                    report.shielded_during_outage += 1;
                }
                DispatchDecision::ShortCircuit { .. } => report.shielded_after_recovery += 1,
            }
            next_arrival += traffic.interval;
        }
        report
    }

    #[test]
    fn a_zero_interval_has_no_forecast() {
        let policy = CircuitBreakerPolicy::new(3, secs(10), secs(5));
        assert!(forecast(policy, Traffic::new(Duration::ZERO, secs(1)), secs(10)).is_none());
    }

    #[test]
    fn an_outage_below_the_threshold_never_trips() {
        let policy = CircuitBreakerPolicy::new(5, secs(10), secs(30));
        let report = run(policy, Traffic::new(secs(1), Duration::ZERO), secs(3));
        assert_eq!(report.tripped_at, None);
        assert_eq!(report.closed_at, None);
        assert_eq!(report.recovery_lag(), None);
        assert_eq!(report.doomed, 3);
        assert_eq!(report.shielded_during_outage, 0);
        assert!(report.outage_shield_ratio().abs() < f64::EPSILON);
        assert!(!report.truncated);
    }

    #[test]
    fn a_long_outage_trips_probes_and_closes() {
        let policy = CircuitBreakerPolicy::new(5, secs(10), secs(30));
        let report = run(policy, Traffic::new(secs(1), Duration::ZERO), secs(60));
        assert_eq!(report.tripped_at, Some(secs(4)));
        assert_eq!(report.closed_at, Some(secs(64)));
        assert_eq!(report.probes, 2);
        assert_eq!(report.failed_probes, 1);
        assert_eq!(report.doomed, 6);
        assert_eq!(report.shielded_during_outage, 54);
        assert_eq!(report.shielded_after_recovery, 4);
        assert_eq!(report.arrivals, 65);
        assert!((report.outage_shield_ratio() - 0.9).abs() < 1e-9);
    }

    #[test]
    fn failures_spread_wider_than_the_window_never_trip() {
        // One failure every 6 s. A 10 s window holds at most two of them.
        let policy = CircuitBreakerPolicy::new(3, secs(10), secs(30));
        let report = run(policy, Traffic::new(secs(6), Duration::ZERO), secs(600));
        assert_eq!(report.tripped_at, None);
        assert_eq!(report.doomed, 100);
    }

    #[test]
    fn latency_lets_more_calls_reach_the_dead_dependency() {
        // Results come 5 s late, so four more calls start before the trip.
        let policy = CircuitBreakerPolicy::new(5, secs(10), secs(30));
        let fast = run(policy, Traffic::new(secs(1), Duration::ZERO), secs(60));
        let slow = run(policy, Traffic::new(secs(1), secs(5)), secs(60));
        assert_eq!(slow.tripped_at, Some(secs(9)));
        assert_eq!(slow.doomed, fast.doomed + 4);
    }

    #[test]
    fn a_shorter_cooldown_trades_probes_for_a_shorter_lag() {
        let traffic = Traffic::new(secs(1), Duration::ZERO);
        let long = run(
            CircuitBreakerPolicy::new(5, secs(10), secs(30)),
            traffic,
            secs(60),
        );
        let short = run(
            CircuitBreakerPolicy::new(5, secs(10), secs(7)),
            traffic,
            secs(60),
        );
        // Probes at 11, 18, ..., 53 s fail. The probe at 60 s meets the heal.
        assert_eq!(short.failed_probes, 7);
        assert_eq!(short.recovery_lag(), Some(Duration::ZERO));
        assert_eq!(long.failed_probes, 1);
        assert_eq!(long.recovery_lag(), Some(secs(4)));
    }

    #[test]
    fn the_replay_stops_at_the_arrival_cap() {
        let policy = CircuitBreakerPolicy::new(1, secs(1), secs(1));
        let report = run(
            policy,
            Traffic::new(Duration::from_nanos(1), Duration::ZERO),
            secs(10),
        );
        assert!(report.truncated);
        assert_eq!(report.arrivals, MAX_ARRIVALS);
        assert_eq!(report.closed_at, None);
    }

    #[test]
    fn the_forecast_matches_the_live_registry() {
        let mut checked = 0;
        for threshold in [1, 3, 5] {
            for window in [secs(2), secs(10)] {
                for cooldown in [secs(0), secs(3), secs(30)] {
                    for latency in [Duration::ZERO, secs(1), secs(4)] {
                        for outage in [secs(0), secs(2), secs(17), secs(90)] {
                            let policy = CircuitBreakerPolicy::new(threshold, window, cooldown);
                            let traffic = Traffic::new(secs(1), latency);
                            let expected = run(policy, traffic, outage);
                            let actual = live(policy, traffic, outage, expected.arrivals);
                            assert_eq!(
                                actual, expected,
                                "threshold {threshold}, window {window:?}, cooldown {cooldown:?}, \
                                 latency {latency:?}, outage {outage:?}"
                            );
                            checked += 1;
                        }
                    }
                }
            }
        }
        assert_eq!(checked, 216);
    }
}
