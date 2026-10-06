//! Adaptive concurrency limit per activity type (issue #1836).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::policy::AdaptiveLimitPolicy;
use crate::telemetry::MetricsRecorder;

/// Growth headroom that each update adds to the cap.
pub const QUEUE_SIZE: f64 = 4.0;

/// Shortest delay for a task that lost the race for a slot.
pub const MIN_LIMIT_DEFER: Duration = Duration::from_millis(50);

/// Longest delay for a task that lost the race for a slot.
pub const MAX_LIMIT_DEFER: Duration = Duration::from_secs(5);

/// How one attempt ended, as the limit sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SampleOutcome {
    /// The dependency answered.
    Answered,
    /// The attempt failed with a retryable failure.
    Overloaded,
}

/// Which activity types have an adaptive limit, and with which policy.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AdaptiveLimitConfig {
    default_policy: Option<AdaptiveLimitPolicy>,
    overrides: HashMap<String, Option<AdaptiveLimitPolicy>>,
}

impl AdaptiveLimitConfig {
    /// A config with no limit for any activity type.
    #[must_use]
    pub fn disabled() -> Self {
        Self::default()
    }

    /// Set the policy for every activity type without an override.
    #[must_use]
    pub fn with_default(self, _policy: Option<AdaptiveLimitPolicy>) -> Self {
        self
    }

    /// Set the policy for one activity type.
    #[must_use]
    pub fn with_activity(
        self,
        _activity_name: impl Into<String>,
        _policy: Option<AdaptiveLimitPolicy>,
    ) -> Self {
        self
    }

    /// The policy for every activity type without an override.
    #[must_use]
    pub const fn default_policy(&self) -> Option<AdaptiveLimitPolicy> {
        self.default_policy
    }

    /// The per-type overrides, sorted by activity name.
    #[must_use]
    pub fn overrides(&self) -> std::collections::BTreeMap<String, Option<AdaptiveLimitPolicy>> {
        std::collections::BTreeMap::new()
    }

    /// The policy that applies to `activity_name`.
    #[must_use]
    pub const fn policy_for(&self, _activity_name: &str) -> Option<AdaptiveLimitPolicy> {
        None
    }
}

/// A view of the limit state of one activity type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LimitSnapshot {
    /// The cap on in-flight attempts.
    pub limit: u32,
    /// Attempts that hold a permit.
    pub in_flight: u32,
    /// The no-load latency estimate, when known.
    pub baseline: Option<Duration>,
}

/// Outcome of [`AdaptiveLimitRegistry::try_acquire`].
#[derive(Debug)]
pub enum Acquire {
    /// The activity type has no limit. Run the attempt.
    Untracked,
    /// Run the attempt. Report its result through the permit.
    Acquired(LimitPermit),
    /// The type is at its cap. Defer the task by `retry_after`.
    Limited {
        /// Delay before the task is claimable again.
        retry_after: Duration,
    },
}

/// One in-flight attempt. Dropping it frees the slot without a sample.
#[derive(Debug)]
pub struct LimitPermit {
    _private: (),
}

impl LimitPermit {
    /// Report the handler latency and outcome, and free the slot.
    pub fn complete(self, _latency: Duration, _outcome: SampleOutcome) {}
}

/// In-process registry of per-activity-type adaptive limits.
#[derive(Default)]
pub struct AdaptiveLimitRegistry {
    config: AdaptiveLimitConfig,
    _states: Mutex<HashMap<String, ()>>,
    metrics: Option<Arc<dyn MetricsRecorder>>,
}

impl std::fmt::Debug for AdaptiveLimitRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdaptiveLimitRegistry")
            .field("config", &self.config)
            .field("metrics", &self.metrics.is_some())
            .finish_non_exhaustive()
    }
}

impl AdaptiveLimitRegistry {
    /// Build a registry from `config`.
    #[must_use]
    pub fn new(config: AdaptiveLimitConfig) -> Self {
        Self {
            config,
            _states: Mutex::new(HashMap::new()),
            metrics: None,
        }
    }

    /// Publish the limit gauges through `metrics`.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// The config this registry enforces.
    #[must_use]
    pub const fn config(&self) -> &AdaptiveLimitConfig {
        &self.config
    }

    /// Take a slot for one attempt of `activity_name`.
    #[must_use]
    pub fn try_acquire(self: &Arc<Self>, _activity_name: &str) -> Acquire {
        Acquire::Untracked
    }

    /// The activity types that are at their cap, sorted by name.
    #[must_use]
    pub fn saturated(&self) -> Vec<String> {
        Vec::new()
    }

    /// The limit state of `activity_name`, or `None` when it has no state.
    #[must_use]
    pub const fn snapshot(&self, _activity_name: &str) -> Option<LimitSnapshot> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "charge_card";
    const B: &str = "send_email";

    fn registry(policy: AdaptiveLimitPolicy) -> Arc<AdaptiveLimitRegistry> {
        Arc::new(AdaptiveLimitRegistry::new(
            AdaptiveLimitConfig::disabled().with_default(Some(policy)),
        ))
    }

    fn permit(reg: &Arc<AdaptiveLimitRegistry>, name: &str) -> LimitPermit {
        match reg.try_acquire(name) {
            Acquire::Acquired(permit) => permit,
            other => panic!("expected a permit, got {other:?}"),
        }
    }

    fn limit(reg: &AdaptiveLimitRegistry, name: &str) -> u32 {
        reg.snapshot(name).expect("state").limit
    }

    /// Hold `n` permits, then complete each with `latency` and `outcome`.
    fn round(
        reg: &Arc<AdaptiveLimitRegistry>,
        name: &str,
        n: u32,
        latency: Duration,
        outcome: SampleOutcome,
    ) {
        let permits: Vec<LimitPermit> = (0..n).map(|_| permit(reg, name)).collect();
        for p in permits {
            p.complete(latency, outcome);
        }
    }

    /// Fill every slot, then complete each with `latency`. Repeat `rounds`
    /// times.
    fn busy_rounds(reg: &Arc<AdaptiveLimitRegistry>, name: &str, rounds: u32, latency: Duration) {
        for _ in 0..rounds {
            let n = limit(reg, name);
            round(reg, name, n, latency, SampleOutcome::Answered);
        }
    }

    const MS_100: Duration = Duration::from_millis(100);

    #[test]
    fn policy_sanitizes_bad_values() {
        let p = AdaptiveLimitPolicy {
            min_limit: 0,
            max_limit: 0,
            tolerance: f64::NAN,
            backoff_ratio: f64::NAN,
            probe_interval: 0,
        }
        .sanitized();
        assert_eq!(p.min_limit, 1);
        assert_eq!(p.max_limit, 1);
        assert!((p.tolerance - 1.0).abs() < f64::EPSILON);
        assert!((p.backoff_ratio - AdaptiveLimitPolicy::DEFAULT_BACKOFF_RATIO).abs() < 1e-12);
        assert_eq!(p.probe_interval, AdaptiveLimitPolicy::MIN_PROBE_INTERVAL);
        let low = AdaptiveLimitPolicy {
            backoff_ratio: 0.0,
            tolerance: 0.2,
            ..AdaptiveLimitPolicy::default()
        }
        .sanitized();
        assert!((low.backoff_ratio - 0.5).abs() < f64::EPSILON);
        assert!((low.tolerance - 1.0).abs() < f64::EPSILON);
        assert_eq!(AdaptiveLimitPolicy::new(9, 3).max_limit, 9);
    }

    #[test]
    fn config_is_off_by_default() {
        let config = AdaptiveLimitConfig::default();
        assert_eq!(config.policy_for(A), None);
        let reg = Arc::new(AdaptiveLimitRegistry::default());
        assert!(matches!(reg.try_acquire(A), Acquire::Untracked));
        assert_eq!(reg.snapshot(A), None);
    }

    #[test]
    fn per_type_override_wins_over_the_default() {
        let custom = AdaptiveLimitPolicy::new(2, 8);
        let config = AdaptiveLimitConfig::disabled()
            .with_default(Some(AdaptiveLimitPolicy::default()))
            .with_activity(A, Some(custom))
            .with_activity(B, None);
        assert_eq!(config.policy_for(A), Some(custom));
        assert_eq!(config.policy_for(B), None);
        assert_eq!(config.policy_for("other"), config.default_policy());
        assert_eq!(config.overrides().len(), 2);
    }

    #[test]
    fn config_sanitizes_a_policy_built_from_public_fields() {
        let raw = AdaptiveLimitPolicy {
            min_limit: 0,
            ..AdaptiveLimitPolicy::default()
        };
        let config = AdaptiveLimitConfig::disabled().with_activity(A, Some(raw));
        assert_eq!(config.policy_for(A).map(|p| p.min_limit), Some(1));
    }

    /// The limit starts low, at the probe cap, so the first baseline is
    /// measured near no load.
    #[test]
    fn a_new_type_starts_at_the_probe_cap() {
        let reg = registry(AdaptiveLimitPolicy::default());
        let p = permit(&reg, A);
        let snap = reg.snapshot(A).expect("state");
        assert_eq!(snap.limit, 4);
        assert_eq!(snap.in_flight, 1);
        assert_eq!(snap.baseline, None);
        drop(p);
    }

    #[test]
    fn acquire_stops_at_the_cap_and_a_drop_frees_a_slot() {
        let reg = registry(AdaptiveLimitPolicy::default());
        let held: Vec<LimitPermit> = (0..4).map(|_| permit(&reg, A)).collect();
        let Acquire::Limited { retry_after } = reg.try_acquire(A) else {
            panic!("the fifth attempt must be limited");
        };
        assert!((MIN_LIMIT_DEFER..=MAX_LIMIT_DEFER).contains(&retry_after));
        assert_eq!(reg.saturated(), vec![A.to_owned()]);
        drop(held);
        assert_eq!(reg.snapshot(A).expect("state").in_flight, 0);
        assert!(reg.saturated().is_empty());
        assert!(matches!(reg.try_acquire(A), Acquire::Acquired(_)));
    }

    #[test]
    fn exhausting_one_type_leaves_other_types_unaffected() {
        let reg = registry(AdaptiveLimitPolicy::default());
        let _held: Vec<LimitPermit> = (0..4).map(|_| permit(&reg, A)).collect();
        assert!(matches!(reg.try_acquire(B), Acquire::Acquired(_)));
        assert_eq!(reg.saturated(), vec![A.to_owned()]);
    }

    /// Flat latency at full use is room to grow.
    #[test]
    fn flat_latency_at_full_use_grows_the_limit() {
        let reg = registry(AdaptiveLimitPolicy::default());
        busy_rounds(&reg, A, 10, MS_100);
        assert!(limit(&reg, A) > 10, "limit {}", limit(&reg, A));
        let baseline = reg.snapshot(A).and_then(|s| s.baseline);
        assert_eq!(baseline, Some(MS_100));
    }

    /// A type that uses less than half of its cap gives no growth signal.
    #[test]
    fn an_app_limited_type_does_not_grow() {
        let reg = registry(AdaptiveLimitPolicy::default());
        busy_rounds(&reg, A, 3, MS_100);
        let before = limit(&reg, A);
        for _ in 0..200 {
            round(&reg, A, 1, MS_100, SampleOutcome::Answered);
        }
        assert_eq!(limit(&reg, A), before);
    }

    /// Latency far above the baseline cuts the limit, by at most half each
    /// sample.
    #[test]
    fn inflated_latency_shrinks_the_limit() {
        let reg = registry(AdaptiveLimitPolicy::default());
        busy_rounds(&reg, A, 10, MS_100);
        let before = limit(&reg, A);
        busy_rounds(&reg, A, 3, MS_100 * 10);
        let after = limit(&reg, A);
        assert!(after < before, "{before} -> {after}");
        assert!(after >= 1);
    }

    #[test]
    fn a_retryable_failure_backs_off_by_the_ratio() {
        let reg = registry(AdaptiveLimitPolicy::default());
        busy_rounds(&reg, A, 15, MS_100);
        let before = f64::from(limit(&reg, A));
        permit(&reg, A).complete(MS_100, SampleOutcome::Overloaded);
        let after = f64::from(limit(&reg, A));
        assert!(
            (after - (before * 0.9).floor()).abs() <= 1.0,
            "{before} -> {after}"
        );
    }

    #[test]
    fn failures_never_push_the_limit_below_the_floor() {
        let reg = registry(AdaptiveLimitPolicy::new(2, 50));
        for _ in 0..100 {
            permit(&reg, A).complete(MS_100, SampleOutcome::Overloaded);
        }
        assert_eq!(limit(&reg, A), 2);
    }

    #[test]
    fn growth_never_passes_the_ceiling() {
        let reg = registry(AdaptiveLimitPolicy::new(1, 12));
        busy_rounds(&reg, A, 50, MS_100);
        assert_eq!(limit(&reg, A), 12);
    }

    /// A probe drops the cap and forgets the baseline. Samples from attempts
    /// that started before the probe do not count.
    #[test]
    fn a_probe_resets_the_baseline_and_ignores_older_attempts() {
        let policy = AdaptiveLimitPolicy {
            probe_interval: 40,
            ..AdaptiveLimitPolicy::default()
        };
        let reg = registry(policy);
        busy_rounds(&reg, A, 4, MS_100);
        let stale = permit(&reg, A);
        let mut steps = 0;
        while reg.snapshot(A).and_then(|s| s.baseline).is_some() {
            round(&reg, A, 1, MS_100, SampleOutcome::Answered);
            steps += 1;
            assert!(steps <= 40, "no probe after {steps} samples");
        }
        assert_eq!(limit(&reg, A), 4, "a probe drops the cap");
        stale.complete(MS_100 * 50, SampleOutcome::Answered);
        assert_eq!(reg.snapshot(A).and_then(|s| s.baseline), None);
        round(&reg, A, 3, MS_100 * 2, SampleOutcome::Answered);
        round(&reg, A, 1, MS_100 * 2, SampleOutcome::Answered);
        assert_eq!(
            reg.snapshot(A).and_then(|s| s.baseline),
            Some(MS_100 * 2),
            "the baseline comes from attempts after the probe"
        );
    }

    /// A deferral waits about one baseline latency, inside the bounds.
    #[test]
    fn limited_delay_follows_the_baseline() {
        let reg = registry(AdaptiveLimitPolicy::new(1, 1));
        round(&reg, A, 1, Duration::from_millis(400), SampleOutcome::Answered);
        for _ in 0..3 {
            round(&reg, A, 1, Duration::from_millis(400), SampleOutcome::Answered);
        }
        let _held = permit(&reg, A);
        for _ in 0..50 {
            let Acquire::Limited { retry_after } = reg.try_acquire(A) else {
                panic!("expected Limited");
            };
            assert!(
                retry_after >= Duration::from_millis(400) && retry_after <= Duration::from_millis(800),
                "{retry_after:?}"
            );
        }
    }

    /// Records every limit sample, in order.
    #[derive(Default)]
    struct LimitLog(Mutex<Vec<(u32, u32, Option<f64>)>>);

    impl MetricsRecorder for LimitLog {
        fn record_activity_concurrency_limit(
            &self,
            _activity: &str,
            limit: u32,
            in_flight: u32,
            baseline_secs: Option<f64>,
        ) {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push((limit, in_flight, baseline_secs));
        }
    }

    /// Every change publishes the state under the lock, so the last sample
    /// always equals the state, even under concurrent use.
    #[test]
    fn the_last_metric_sample_matches_the_state_under_concurrency() {
        let log = Arc::new(LimitLog::default());
        let reg = Arc::new(
            AdaptiveLimitRegistry::new(
                AdaptiveLimitConfig::disabled()
                    .with_default(Some(AdaptiveLimitPolicy::new(1, 64))),
            )
            .with_metrics(log.clone()),
        );
        let threads: Vec<_> = (0..8)
            .map(|i| {
                let reg = Arc::clone(&reg);
                std::thread::spawn(move || {
                    for j in 0..300_u64 {
                        if let Acquire::Acquired(p) = reg.try_acquire(A) {
                            let outcome = if (i + j) % 17 == 0 {
                                SampleOutcome::Overloaded
                            } else {
                                SampleOutcome::Answered
                            };
                            p.complete(Duration::from_millis(10 + j % 7), outcome);
                        }
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().expect("thread");
        }
        let last = *log
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .last()
            .expect("the gauges were published");
        let snap = reg.snapshot(A).expect("state");
        assert_eq!(last.0, snap.limit);
        assert_eq!(last.1, 0);
        assert_eq!(last.2, snap.baseline.map(|b| b.as_secs_f64()));
    }
}

/// Closed-loop simulation against a downstream with a knee (issue #1836).
///
/// The downstream answers in `base` while the concurrency is at or below
/// `knee`. Above the knee, latency grows in proportion to the concurrency,
/// as for a fixed pool of `knee` servers. Demand never runs out, so the
/// worker always has a task to start.
#[cfg(test)]
mod simulation {
    use super::*;
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    const NAME: &str = "downstream_call";

    /// A tiny deterministic generator, so each run is reproducible.
    struct XorShift(u64);

    impl XorShift {
        fn unit(&mut self) -> f64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            #[allow(clippy::cast_precision_loss)]
            let v = (self.0 >> 11) as f64 / (1_u64 << 53) as f64;
            v
        }
    }

    struct Downstream {
        knee: u32,
        base: Duration,
        /// Relative latency noise, uniform in `[-noise, +noise]`.
        noise: f64,
        /// Fail every call that starts above the knee.
        fail_above_knee: bool,
    }

    struct Trace {
        /// The limit after each completed attempt.
        limits: Vec<u32>,
        /// The concurrency at the start of each attempt.
        concurrency: Vec<u32>,
    }

    fn simulate(policy: AdaptiveLimitPolicy, downstream: &Downstream, samples: usize) -> Trace {
        let reg = Arc::new(AdaptiveLimitRegistry::new(
            AdaptiveLimitConfig::disabled().with_activity(NAME, Some(policy)),
        ));
        let mut rng = XorShift(0x9E37_79B9_7F4A_7C15);
        // Completion time in nanoseconds of virtual time, and a slot index.
        let mut due: BinaryHeap<Reverse<(u128, usize)>> = BinaryHeap::new();
        let mut running: Vec<Option<(LimitPermit, Duration, SampleOutcome)>> = Vec::new();
        let mut now: u128 = 0;
        let mut trace = Trace {
            limits: Vec::with_capacity(samples),
            concurrency: Vec::new(),
        };
        while trace.limits.len() < samples {
            while let Acquire::Acquired(permit) = reg.try_acquire(NAME) {
                let n = reg.snapshot(NAME).expect("state").in_flight;
                trace.concurrency.push(n);
                let load = (f64::from(n) / f64::from(downstream.knee)).max(1.0);
                let jitter = downstream.noise.mul_add(rng.unit().mul_add(2.0, -1.0), 1.0);
                let latency = downstream.base.mul_f64(load * jitter);
                let outcome = if downstream.fail_above_knee && n > downstream.knee {
                    SampleOutcome::Overloaded
                } else {
                    SampleOutcome::Answered
                };
                let slot = running.len();
                running.push(Some((permit, latency, outcome)));
                due.push(Reverse((now + latency.as_nanos(), slot)));
            }
            let Reverse((at, slot)) = due.pop().expect("an attempt is in flight");
            now = at;
            let (permit, latency, outcome) = running[slot].take().expect("live slot");
            permit.complete(latency, outcome);
            trace.limits.push(reg.snapshot(NAME).expect("state").limit);
        }
        trace
    }

    fn median(values: &[u32]) -> u32 {
        let mut sorted = values.to_vec();
        sorted.sort_unstable();
        sorted[sorted.len() / 2]
    }

    fn mean(values: &[u32]) -> f64 {
        let sum: f64 = values.iter().map(|v| f64::from(*v)).sum();
        #[allow(clippy::cast_precision_loss)]
        let n = values.len() as f64;
        sum / n
    }

    /// The fixed point of the gradient rule against a linear knee is
    /// `tolerance * knee + QUEUE_SIZE`. The limit must settle near it,
    /// which is near the knee, and far below the ceiling.
    #[test]
    fn the_limit_converges_near_the_knee() {
        let policy = AdaptiveLimitPolicy::default();
        let knee = 40;
        let downstream = Downstream {
            knee,
            base: Duration::from_millis(100),
            noise: 0.0,
            fail_above_knee: false,
        };
        let trace = simulate(policy, &downstream, 20_000);
        let settled = median(&trace.limits[10_000..]);
        let fixed_point = policy.tolerance.mul_add(f64::from(knee), QUEUE_SIZE);
        assert!(
            settled >= knee && f64::from(settled) <= fixed_point + 2.0,
            "settled at {settled}; knee {knee}; fixed point {fixed_point}"
        );
        let peak = *trace.limits.iter().max().expect("samples");
        assert!(
            f64::from(peak) <= fixed_point * 1.2,
            "peak {peak} overshoots the fixed point {fixed_point}"
        );
        assert!(peak < policy.max_limit / 2, "peak {peak} nears the ceiling");
    }

    /// With latency noise, the limit still settles near the knee.
    #[test]
    fn the_limit_converges_under_latency_noise() {
        let policy = AdaptiveLimitPolicy::default();
        let knee = 40;
        let downstream = Downstream {
            knee,
            base: Duration::from_millis(100),
            noise: 0.2,
            fail_above_knee: false,
        };
        let trace = simulate(policy, &downstream, 20_000);
        let settled = median(&trace.limits[10_000..]);
        let fixed_point = policy.tolerance.mul_add(f64::from(knee), QUEUE_SIZE);
        assert!(
            f64::from(settled) >= 0.75 * f64::from(knee)
                && f64::from(settled) <= fixed_point + 2.0,
            "settled at {settled}; knee {knee}; fixed point {fixed_point}"
        );
    }

    /// A long run shows no upward drift. A smoothed baseline would ratchet
    /// up under steady load. The probed minimum does not.
    #[test]
    fn the_limit_does_not_grow_without_bound() {
        let policy = AdaptiveLimitPolicy::default();
        let knee = 40;
        let downstream = Downstream {
            knee,
            base: Duration::from_millis(100),
            noise: 0.1,
            fail_above_knee: false,
        };
        let trace = simulate(policy, &downstream, 100_000);
        let early = mean(&trace.limits[20_000..30_000]);
        let late = mean(&trace.limits[90_000..]);
        assert!(late <= early * 1.05, "drift: {early:.1} -> {late:.1}");
        let fixed_point = policy.tolerance.mul_add(f64::from(knee), QUEUE_SIZE);
        let peak = *trace.limits.iter().max().expect("samples");
        assert!(
            f64::from(peak) <= fixed_point * 1.2,
            "peak {peak} over a long run; fixed point {fixed_point}"
        );
        let peak_concurrency = *trace.concurrency.iter().max().expect("starts");
        assert!(f64::from(peak_concurrency) <= fixed_point * 1.2);
    }

    /// A downstream that rejects calls above its knee keeps the limit near
    /// the knee through the error backoff, even with flat latency.
    #[test]
    fn errors_above_the_knee_hold_the_limit_near_the_knee() {
        let policy = AdaptiveLimitPolicy::default();
        let knee = 40;
        let downstream = Downstream {
            knee,
            base: Duration::from_millis(100),
            noise: 0.0,
            fail_above_knee: true,
        };
        let trace = simulate(
            AdaptiveLimitPolicy {
                tolerance: 10.0,
                ..policy
            },
            &downstream,
            20_000,
        );
        let settled = median(&trace.limits[10_000..]);
        assert!(
            f64::from(settled) >= 0.7 * f64::from(knee)
                && f64::from(settled) <= 1.2 * f64::from(knee),
            "settled at {settled}; knee {knee}"
        );
    }
}
