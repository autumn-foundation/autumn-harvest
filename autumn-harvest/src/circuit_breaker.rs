//! Per-activity circuit breaker that fast-fails dispatch during downstream
//! outages (issue #369).
//!
//! When a downstream service an activity depends on goes hard-down, harvest's
//! default behaviour is to retry every failing attempt across its full
//! [`RetryPolicy`](crate::policy::RetryPolicy) curve. Across thousands of
//! in-flight workflows that all call the same activity, this floods the task
//! queue with retries against a dead target and piles up identical DLQ entries.
//!
//! A [`CircuitBreakerPolicy`](crate::policy::CircuitBreakerPolicy) attached to
//! an activity lets the worker track that activity's recent failures and
//! **trip open** once they cross a threshold within a rolling window. While the
//! breaker is open, new dispatches short-circuit with a non-retryable
//! `"CircuitOpen"` failure instead of running the doomed work; workflows that
//! handle the failure (Saga compensation, branching) see it within seconds.
//!
//! ## State model
//!
//! ```text
//!            failures >= threshold in window
//!   Closed ─────────────────────────────────► Open
//!     ▲                                          │
//!     │ probe succeeds                           │ cooldown elapsed
//!     │                                          ▼
//!     └──────────────── HalfOpen ◄───────────────┘
//!                          │ probe fails
//!                          └──────────► Open
//! ```
//!
//! ## Scope and durability
//!
//! State is tracked **in-process and per-shard** (`Mutex<HashMap>`). It never
//! touches the workflow event log: a short-circuited attempt records an
//! ordinary `ActivityFailed` event with a typed `"CircuitOpen"` payload, so the
//! append-only contract and deterministic replay are both unaffected. Each
//! shard / worker process tracks its own breaker; an outage that hits every
//! shard trips each independently, matching the per-shard ACID model.

// Each public method intentionally holds the state lock for its whole body: it
// reads and mutates the same `BreakerState` and returns a value derived from
// it, so there is no meaningful window in which the guard could be released
// earlier. The drop-tightening lint is a false positive here.
#![allow(clippy::significant_drop_tightening)]

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::policy::CircuitBreakerPolicy;

/// The three observable phases of a breaker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitPhase {
    /// Normal operation: dispatches proceed unchanged.
    Closed,
    /// Tripped: dispatches fast-fail until the cooldown elapses.
    Open,
    /// Cooldown elapsed: a single probe dispatch is admitted.
    HalfOpen,
}

impl CircuitPhase {
    /// Stable, low-cardinality string used in API responses and metrics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::Open => "open",
            Self::HalfOpen => "half_open",
        }
    }
}

/// Outcome of consulting the breaker before dispatching an activity attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchDecision {
    /// Proceed with the dispatch (breaker closed, or an admitted half-open probe).
    Allow,
    /// Short-circuit the attempt: the breaker is open.
    ShortCircuit {
        /// Wall-clock instant at which the breaker last tripped, if known.
        opened_at: Option<DateTime<Utc>>,
        /// How long until a half-open probe will be admitted.
        retry_after: Duration,
    },
}

/// A state transition worth reporting to the metrics surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitTransition {
    /// The breaker moved into the open state (tripped or re-tripped).
    Tripped,
    /// The breaker recovered to the closed state.
    Closed,
}

/// Snapshot of a single activity's breaker for the management API.
#[derive(Debug, Clone, Serialize)]
pub struct CircuitSnapshot {
    /// The activity name.
    pub activity_name: String,
    /// Current phase: `"closed"`, `"open"`, or `"half_open"`.
    pub state: &'static str,
    /// Whether an operator has pinned the breaker open via the management API.
    pub forced_open: bool,
    /// Wall-clock timestamp of the most recent trip, if the breaker has tripped.
    pub last_trip: Option<DateTime<Utc>>,
    /// Failures currently counted inside the rolling window (closed-phase signal).
    pub rolling_failure_count: u32,
    /// Seconds until a half-open probe is admitted (only set while open and not
    /// forced).
    pub time_until_probe_secs: Option<f64>,
    /// Configured failure threshold.
    pub failure_threshold: u32,
    /// Configured rolling window, in seconds.
    pub window_secs: f64,
    /// Configured cooldown, in seconds.
    pub cooldown_secs: f64,
}

#[derive(Debug)]
struct BreakerState {
    phase: CircuitPhase,
    /// Failure timestamps inside the rolling window (closed-phase counter).
    failures: VecDeque<Instant>,
    /// Monotonic instant of the last trip (drives cooldown math).
    opened_at: Option<Instant>,
    /// Wall-clock instant of the last trip (for the observable snapshot).
    opened_at_wall: Option<DateTime<Utc>>,
    /// `true` while a half-open probe is in flight so concurrent dispatches
    /// short-circuit and only one probe runs.
    probe_in_flight: bool,
    /// Operator pin: while set, dispatches always short-circuit and results are
    /// ignored until an operator force-closes.
    forced_open: bool,
}

impl Default for BreakerState {
    fn default() -> Self {
        Self {
            phase: CircuitPhase::Closed,
            failures: VecDeque::new(),
            opened_at: None,
            opened_at_wall: None,
            probe_in_flight: false,
            forced_open: false,
        }
    }
}

impl BreakerState {
    fn prune(&mut self, now: Instant, window: Duration) {
        while let Some(&front) = self.failures.front() {
            if now.saturating_duration_since(front) > window {
                self.failures.pop_front();
            } else {
                break;
            }
        }
    }

    fn trip(&mut self, now: Instant) {
        self.phase = CircuitPhase::Open;
        self.opened_at = Some(now);
        self.opened_at_wall = Some(Utc::now());
        self.failures.clear();
        self.probe_in_flight = false;
    }

    fn close(&mut self) {
        self.phase = CircuitPhase::Closed;
        self.failures.clear();
        self.opened_at = None;
        self.opened_at_wall = None;
        self.probe_in_flight = false;
    }
}

/// In-process registry of per-activity circuit breakers.
///
/// Constructed once from the registered activities' policies and shared (behind
/// an `Arc`) between the worker dispatch path and the management API so that
/// both observe the same state. Activities without a declared policy are never
/// tracked: [`on_dispatch`](Self::on_dispatch) returns [`DispatchDecision::Allow`]
/// and [`on_result`](Self::on_result) is a no-op for them.
#[derive(Debug)]
pub struct CircuitBreakerRegistry {
    policies: HashMap<String, CircuitBreakerPolicy>,
    states: Mutex<HashMap<String, BreakerState>>,
}

impl CircuitBreakerRegistry {
    /// Build a registry from `(activity_name, policy)` pairs.
    #[must_use]
    pub fn new(policies: HashMap<String, CircuitBreakerPolicy>) -> Self {
        Self {
            policies,
            states: Mutex::new(HashMap::new()),
        }
    }

    /// An empty registry that tracks nothing (every dispatch is allowed).
    #[must_use]
    pub fn empty() -> Self {
        Self::new(HashMap::new())
    }

    /// `true` if no activity has a circuit-breaker policy declared.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.policies.is_empty()
    }

    /// Whether `activity_name` has a declared policy.
    #[must_use]
    pub fn has_policy(&self, activity_name: &str) -> bool {
        self.policies.contains_key(activity_name)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, BreakerState>> {
        // Poisoning only happens if a thread panicked while holding the lock.
        // The state is just counters/timestamps, so recovering the inner value
        // is always safe and far preferable to cascading a panic across the
        // worker hot path.
        self.states.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Decide whether to allow a dispatch of `activity_name` at `now`.
    ///
    /// Activities without a policy always return [`DispatchDecision::Allow`].
    /// When the breaker is open and the cooldown has elapsed this call performs
    /// the open → half-open transition and admits the returned dispatch as the
    /// single probe.
    #[must_use]
    pub fn on_dispatch(&self, activity_name: &str, now: Instant) -> DispatchDecision {
        let Some(&policy) = self.policies.get(activity_name) else {
            return DispatchDecision::Allow;
        };
        let mut states = self.lock();
        let st = states.entry(activity_name.to_string()).or_default();

        if st.forced_open {
            return DispatchDecision::ShortCircuit {
                opened_at: st.opened_at_wall,
                retry_after: policy.cooldown,
            };
        }

        match st.phase {
            CircuitPhase::Closed => DispatchDecision::Allow,
            CircuitPhase::Open => {
                let opened = st.opened_at.unwrap_or(now);
                let elapsed = now.saturating_duration_since(opened);
                if elapsed >= policy.cooldown {
                    // Cooldown elapsed: admit exactly one probe.
                    st.phase = CircuitPhase::HalfOpen;
                    st.probe_in_flight = true;
                    DispatchDecision::Allow
                } else {
                    DispatchDecision::ShortCircuit {
                        opened_at: st.opened_at_wall,
                        retry_after: policy.cooldown.saturating_sub(elapsed),
                    }
                }
            }
            CircuitPhase::HalfOpen => {
                if st.probe_in_flight {
                    // A probe is already running; keep short-circuiting.
                    DispatchDecision::ShortCircuit {
                        opened_at: st.opened_at_wall,
                        retry_after: Duration::ZERO,
                    }
                } else {
                    st.probe_in_flight = true;
                    DispatchDecision::Allow
                }
            }
        }
    }

    /// Record the outcome of a dispatched attempt.
    ///
    /// Returns `Some(transition)` when the breaker changed open/closed state so
    /// the caller can emit the corresponding metric. Activities without a
    /// policy are ignored and always return `None`.
    pub fn on_result(
        &self,
        activity_name: &str,
        success: bool,
        now: Instant,
    ) -> Option<CircuitTransition> {
        let &policy = self.policies.get(activity_name)?;
        let mut states = self.lock();
        let st = states.entry(activity_name.to_string()).or_default();

        if st.forced_open {
            // Operator-pinned: ignore organic results until force-closed.
            return None;
        }

        match st.phase {
            CircuitPhase::Closed => {
                if success {
                    st.failures.clear();
                    None
                } else {
                    st.failures.push_back(now);
                    st.prune(now, policy.window);
                    if st.failures.len() >= policy.failure_threshold as usize {
                        st.trip(now);
                        Some(CircuitTransition::Tripped)
                    } else {
                        None
                    }
                }
            }
            CircuitPhase::HalfOpen => {
                st.probe_in_flight = false;
                if success {
                    st.close();
                    Some(CircuitTransition::Closed)
                } else {
                    st.trip(now);
                    Some(CircuitTransition::Tripped)
                }
            }
            // A result arriving while fully open (no probe admitted) is a
            // stale straggler; leave the breaker untouched.
            CircuitPhase::Open => None,
        }
    }

    /// Operator action: pin the breaker open for manual incident response.
    pub fn force_open(&self, activity_name: &str, now: Instant) {
        let mut states = self.lock();
        let st = states.entry(activity_name.to_string()).or_default();
        st.forced_open = true;
        st.phase = CircuitPhase::Open;
        st.opened_at = Some(now);
        st.opened_at_wall = Some(Utc::now());
        st.probe_in_flight = false;
    }

    /// Operator action: clear any pin and reset the breaker to closed so normal
    /// tracking resumes ("I know the downstream is back, close it now").
    pub fn force_close(&self, activity_name: &str) {
        let mut states = self.lock();
        let st = states.entry(activity_name.to_string()).or_default();
        st.forced_open = false;
        st.close();
    }

    /// Observable snapshot for a single activity, or `None` if it has no policy.
    #[must_use]
    pub fn snapshot(&self, activity_name: &str, now: Instant) -> Option<CircuitSnapshot> {
        let &policy = self.policies.get(activity_name)?;
        let mut states = self.lock();
        let st = states.entry(activity_name.to_string()).or_default();
        st.prune(now, policy.window);
        Some(Self::snapshot_inner(activity_name, &policy, st, now))
    }

    /// Observable snapshots for every activity with a declared policy, sorted by
    /// name.
    #[must_use]
    pub fn list(&self, now: Instant) -> Vec<CircuitSnapshot> {
        let mut states = self.lock();
        let mut out: Vec<CircuitSnapshot> = self
            .policies
            .iter()
            .map(|(name, policy)| {
                let st = states.entry(name.clone()).or_default();
                st.prune(now, policy.window);
                Self::snapshot_inner(name, policy, st, now)
            })
            .collect();
        out.sort_by(|a, b| a.activity_name.cmp(&b.activity_name));
        out
    }

    fn snapshot_inner(
        activity_name: &str,
        policy: &CircuitBreakerPolicy,
        st: &BreakerState,
        now: Instant,
    ) -> CircuitSnapshot {
        // Only a non-forced open breaker counts down toward a probe; a forced
        // pin and the closed/half-open phases report no ETA.
        let time_until_probe_secs = if st.phase == CircuitPhase::Open && !st.forced_open {
            st.opened_at.map(|opened| {
                let elapsed = now.saturating_duration_since(opened);
                policy.cooldown.saturating_sub(elapsed).as_secs_f64()
            })
        } else {
            None
        };
        CircuitSnapshot {
            activity_name: activity_name.to_string(),
            state: st.phase.as_str(),
            forced_open: st.forced_open,
            last_trip: st.opened_at_wall,
            rolling_failure_count: u32::try_from(st.failures.len()).unwrap_or(u32::MAX),
            time_until_probe_secs,
            failure_threshold: policy.failure_threshold,
            window_secs: policy.window.as_secs_f64(),
            cooldown_secs: policy.cooldown.as_secs_f64(),
        }
    }
}

impl Default for CircuitBreakerRegistry {
    fn default() -> Self {
        Self::empty()
    }
}

// ---------------------------------------------------------------------------
// Tests (red phase: written before the implementation above existed)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> CircuitBreakerPolicy {
        // Trip after 3 failures in 30s; re-probe after 60s.
        CircuitBreakerPolicy::new(3, Duration::from_secs(30), Duration::from_secs(60))
    }

    fn registry() -> CircuitBreakerRegistry {
        let mut p = HashMap::new();
        p.insert("send_email".to_string(), policy());
        CircuitBreakerRegistry::new(p)
    }

    #[test]
    fn untracked_activity_always_allows_and_ignores_results() {
        let reg = CircuitBreakerRegistry::empty();
        let now = Instant::now();
        assert_eq!(reg.on_dispatch("anything", now), DispatchDecision::Allow);
        assert_eq!(reg.on_result("anything", false, now), None);
        assert!(reg.snapshot("anything", now).is_none());
        assert!(reg.is_empty());
    }

    #[test]
    fn closed_breaker_allows_dispatch() {
        let reg = registry();
        let now = Instant::now();
        assert_eq!(reg.on_dispatch("send_email", now), DispatchDecision::Allow);
        let snap = reg.snapshot("send_email", now).unwrap();
        assert_eq!(snap.state, "closed");
        assert_eq!(snap.rolling_failure_count, 0);
    }

    #[test]
    fn success_does_not_trip_and_resets_failures() {
        let reg = registry();
        let now = Instant::now();
        reg.on_result("send_email", false, now);
        reg.on_result("send_email", false, now);
        assert_eq!(reg.snapshot("send_email", now).unwrap().rolling_failure_count, 2);
        // A success clears the rolling window.
        assert_eq!(reg.on_result("send_email", true, now), None);
        assert_eq!(reg.snapshot("send_email", now).unwrap().rolling_failure_count, 0);
    }

    #[test]
    fn trips_open_at_threshold() {
        let reg = registry();
        let now = Instant::now();
        assert_eq!(reg.on_result("send_email", false, now), None);
        assert_eq!(reg.on_result("send_email", false, now), None);
        assert_eq!(
            reg.on_result("send_email", false, now),
            Some(CircuitTransition::Tripped)
        );
        let snap = reg.snapshot("send_email", now).unwrap();
        assert_eq!(snap.state, "open");
        assert!(snap.last_trip.is_some());
    }

    #[test]
    fn open_breaker_short_circuits_until_cooldown() {
        let reg = registry();
        let t0 = Instant::now();
        for _ in 0..3 {
            reg.on_result("send_email", false, t0);
        }
        // Immediately after trip: short-circuit with retry_after ~= cooldown.
        match reg.on_dispatch("send_email", t0) {
            DispatchDecision::ShortCircuit { retry_after, .. } => {
                assert!(retry_after <= Duration::from_secs(60));
                assert!(retry_after > Duration::from_secs(59));
            }
            DispatchDecision::Allow => panic!("expected ShortCircuit, got Allow"),
        }
        // Half-way through cooldown: still short-circuit.
        assert!(matches!(
            reg.on_dispatch("send_email", t0 + Duration::from_secs(30)),
            DispatchDecision::ShortCircuit { .. }
        ));
    }

    #[test]
    fn failures_outside_window_do_not_count() {
        let reg = registry();
        let t0 = Instant::now();
        reg.on_result("send_email", false, t0);
        reg.on_result("send_email", false, t0);
        // Third failure arrives after the 30s window — the first two have aged out.
        assert_eq!(
            reg.on_result("send_email", false, t0 + Duration::from_secs(31)),
            None,
            "stale failures should not contribute to the threshold"
        );
        assert_eq!(reg.snapshot("send_email", t0 + Duration::from_secs(31)).unwrap().state, "closed");
    }

    #[test]
    fn half_open_admits_single_probe_then_closes_on_success() {
        let reg = registry();
        let t0 = Instant::now();
        for _ in 0..3 {
            reg.on_result("send_email", false, t0);
        }
        let probe_time = t0 + Duration::from_secs(61);
        // First dispatch after cooldown is admitted as the probe.
        assert_eq!(reg.on_dispatch("send_email", probe_time), DispatchDecision::Allow);
        assert_eq!(reg.snapshot("send_email", probe_time).unwrap().state, "half_open");
        // While the probe is in flight, other dispatches short-circuit.
        assert!(matches!(
            reg.on_dispatch("send_email", probe_time),
            DispatchDecision::ShortCircuit { .. }
        ));
        // Probe succeeds: breaker closes.
        assert_eq!(
            reg.on_result("send_email", true, probe_time),
            Some(CircuitTransition::Closed)
        );
        assert_eq!(reg.snapshot("send_email", probe_time).unwrap().state, "closed");
        assert_eq!(reg.on_dispatch("send_email", probe_time), DispatchDecision::Allow);
    }

    #[test]
    fn half_open_reopens_on_probe_failure() {
        let reg = registry();
        let t0 = Instant::now();
        for _ in 0..3 {
            reg.on_result("send_email", false, t0);
        }
        let probe_time = t0 + Duration::from_secs(61);
        assert_eq!(reg.on_dispatch("send_email", probe_time), DispatchDecision::Allow);
        // Probe fails: breaker re-opens.
        assert_eq!(
            reg.on_result("send_email", false, probe_time),
            Some(CircuitTransition::Tripped)
        );
        assert_eq!(reg.snapshot("send_email", probe_time).unwrap().state, "open");
        // And the cooldown clock restarts from the probe failure.
        assert!(matches!(
            reg.on_dispatch("send_email", probe_time),
            DispatchDecision::ShortCircuit { .. }
        ));
    }

    #[test]
    fn force_open_short_circuits_and_ignores_results() {
        let reg = registry();
        let now = Instant::now();
        reg.force_open("send_email", now);
        let snap = reg.snapshot("send_email", now).unwrap();
        assert_eq!(snap.state, "open");
        assert!(snap.forced_open);
        assert!(matches!(
            reg.on_dispatch("send_email", now),
            DispatchDecision::ShortCircuit { .. }
        ));
        // Even far past the cooldown, a forced-open breaker never probes.
        assert!(matches!(
            reg.on_dispatch("send_email", now + Duration::from_secs(600)),
            DispatchDecision::ShortCircuit { .. }
        ));
        // Organic successes do not auto-close a forced-open breaker.
        assert_eq!(reg.on_result("send_email", true, now), None);
        assert_eq!(reg.snapshot("send_email", now).unwrap().state, "open");
    }

    #[test]
    fn force_close_resets_to_closed() {
        let reg = registry();
        let now = Instant::now();
        for _ in 0..3 {
            reg.on_result("send_email", false, now);
        }
        assert_eq!(reg.snapshot("send_email", now).unwrap().state, "open");
        reg.force_close("send_email");
        let snap = reg.snapshot("send_email", now).unwrap();
        assert_eq!(snap.state, "closed");
        assert!(!snap.forced_open);
        assert_eq!(snap.rolling_failure_count, 0);
        assert_eq!(reg.on_dispatch("send_email", now), DispatchDecision::Allow);
    }

    #[test]
    fn list_returns_all_policies_sorted() {
        let mut p = HashMap::new();
        p.insert("zeta".to_string(), policy());
        p.insert("alpha".to_string(), policy());
        let reg = CircuitBreakerRegistry::new(p);
        let now = Instant::now();
        let snaps = reg.list(now);
        assert_eq!(snaps.len(), 2);
        assert_eq!(snaps[0].activity_name, "alpha");
        assert_eq!(snaps[1].activity_name, "zeta");
    }

    #[test]
    fn snapshot_reports_time_until_probe() {
        let reg = registry();
        let t0 = Instant::now();
        for _ in 0..3 {
            reg.on_result("send_email", false, t0);
        }
        let snap = reg.snapshot("send_email", t0 + Duration::from_secs(20)).unwrap();
        let remaining = snap.time_until_probe_secs.expect("open breaker reports probe ETA");
        // 60s cooldown - 20s elapsed ~= 40s.
        assert!((remaining - 40.0).abs() < 1.0, "remaining was {remaining}");
    }

    #[test]
    fn threshold_clamped_to_at_least_one() {
        let p = CircuitBreakerPolicy::new(0, Duration::from_secs(10), Duration::from_secs(10));
        assert_eq!(p.failure_threshold, 1);
    }
}
