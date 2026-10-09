//! Adaptive concurrency limit per activity type (issue #1836).
//!
//! The slot tuner grows the worker slots when tasks wait. That is the wrong
//! direction when a dependency is the bottleneck. More calls then only add
//! latency and errors.
//!
//! An adaptive limit caps the in-flight attempts of one activity type on one
//! worker. The cap follows the handler latency and the retryable failures.
//! It never reads a queue wait or a permit wait.
//!
//! ## Rules
//!
//! The limit collects samples in windows. A window holds about one cap of
//! samples, so the cap moves about once per round trip.
//!
//! 1. **Baseline.** The baseline is the lowest window mean latency since the
//!    last probe. It estimates the latency at no load.
//! 2. **Gradient.** The gradient is `tolerance * baseline / mean latency`,
//!    clamped to the range from 0.5 to 1. The cap moves 20 % of the way to
//!    `cap * gradient + QUEUE_SIZE`. Thus the cap grows while latency stays
//!    near the baseline, and it shrinks when latency inflates.
//! 3. **Backoff.** A window is overloaded when the retryable failures pass
//!    `error_threshold` of its completions. An overloaded window cuts the
//!    cap by `backoff_ratio`. A rare failure is noise and does not cut it.
//! 4. **App limit.** A window that used less than half of the cap does not
//!    move the cap. Low demand says nothing about the dependency.
//! 5. **Probe.** After `probe_interval` samples, the next window that is not
//!    overloaded starts a probe. The cap drops to [`QUEUE_SIZE`], or stays
//!    lower, and the baseline is cleared. Answers from attempts that started
//!    before the probe do not count. Their failures still count, but they
//!    cannot close the probe window. The probe window ends with its first
//!    answers and sets a fresh baseline at a low concurrency. Then the cap
//!    returns to its value from before the probe.
//! 6. **Bounds.** The cap stays in `[min_limit, max_limit]`.
//!
//! A new type starts with a probe at [`QUEUE_SIZE`], clamped to the bounds,
//! so its first baseline is measured at a low concurrency.
//!
//! The worker decides what a sample is. A success is an answer. A timeout
//! and a retryable failure are overload. A non-retryable failure, a panic
//! and any other cancelled attempt give no sample.
//!
//! ## Why a probed minimum
//!
//! Netflix Gradient2 compares the latency with a smoothed long-term average.
//! Under steady load that average catches up with the latency. The gradient
//! then returns to 1, and the cap grows again. The cap ratchets up without
//! bound. A minimum that a probe measures again does not drift, so the cap
//! settles. Against a dependency whose latency grows in proportion to the
//! concurrency above a knee, the fixed point is
//! `tolerance * knee + QUEUE_SIZE`. The simulation tests prove it.
//!
//! ## Worker integration
//!
//! - The claim skips a type at its cap, also on a row with capability
//!   requirements. Its tasks stay `PENDING` for another worker or a later
//!   poll.
//! - A claim can still race past the cap, for example when two claim
//!   loops claim at once, of one worker or of two. The dispatch gate then defers the row with the fenced
//!   retry-budget write. The deferral uses no attempt and appends no event.
//! - The gate runs before the retry budget, so a deferral spends no budget
//!   token. A circuit short-circuit and a half-open probe take no slot.
//!
//! ## Scope
//!
//! The state is in process and per worker, like the retry budget. N workers
//! allow up to N caps. The limit never touches the event log, so replay is
//! unaffected. Local activities and the SQLite backend do not use it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use crate::policy::AdaptiveLimitPolicy;
use crate::telemetry::MetricsRecorder;

/// Growth headroom that each update adds to the cap. It is also the cap
/// during a probe.
pub const QUEUE_SIZE: f64 = 4.0;

/// Shortest delay for a task that lost the race for a slot.
pub const MIN_LIMIT_DEFER: Duration = Duration::from_millis(50);

/// Longest delay for a task that lost the race for a slot.
pub const MAX_LIMIT_DEFER: Duration = Duration::from_secs(5);

/// Weight of the new target in each update.
const SMOOTHING: f64 = 0.2;

/// Lowest gradient. One window cannot set a target below half of the cap.
const MIN_GRADIENT: f64 = 0.5;

/// Fewest samples in one window. The first window after a probe sets the
/// baseline.
const PROBE_SAMPLES: u32 = 4;

/// How one attempt ended, as the limit sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SampleOutcome {
    /// The dependency answered.
    Answered,
    /// The attempt failed with a retryable failure or timed out.
    Overloaded,
}

/// Which activity types have an adaptive limit, and with which policy.
///
/// The default config limits no type. A per-type override replaces the
/// default policy for one activity name. An override of `None` turns the
/// limit off for that type.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AdaptiveLimitConfig {
    default_policy: Option<AdaptiveLimitPolicy>,
    overrides: HashMap<String, Option<AdaptiveLimitPolicy>>,
}

impl AdaptiveLimitConfig {
    /// A config with no limit for any activity type. This is the default.
    #[must_use]
    pub fn disabled() -> Self {
        Self::default()
    }

    /// Set the policy for every activity type without an override. `None`
    /// turns the default off.
    #[must_use]
    pub fn with_default(mut self, policy: Option<AdaptiveLimitPolicy>) -> Self {
        self.default_policy = policy.map(AdaptiveLimitPolicy::sanitized);
        self
    }

    /// Set the policy for one activity type. `None` turns the limit off for
    /// that type.
    #[must_use]
    pub fn with_activity(
        mut self,
        activity_name: impl Into<String>,
        policy: Option<AdaptiveLimitPolicy>,
    ) -> Self {
        self.overrides.insert(
            activity_name.into(),
            policy.map(AdaptiveLimitPolicy::sanitized),
        );
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
        self.overrides
            .iter()
            .map(|(name, policy)| (name.clone(), *policy))
            .collect()
    }

    /// The policy that applies to `activity_name`, or `None` when it has no
    /// limit.
    #[must_use]
    pub fn policy_for(&self, activity_name: &str) -> Option<AdaptiveLimitPolicy> {
        self.overrides
            .get(activity_name)
            .copied()
            .unwrap_or(self.default_policy)
    }
}

/// A view of the limit state of one activity type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[non_exhaustive]
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

/// Samples gathered since the cap last moved.
#[derive(Debug, Default)]
struct Window {
    /// Samples in the window, failures included.
    samples: u32,
    /// Answers in the window.
    answers: u32,
    /// Sum of the answer latencies.
    latency_sum: Duration,
    /// Highest in-flight count at the start of a sampled attempt.
    max_in_flight: u32,
    /// Retryable failures in the window, from any epoch.
    failures: u32,
    /// Completions in the window, from any epoch. The failure share uses
    /// it, so the drain of older attempts after a probe does not inflate
    /// the share.
    completions: u32,
}

impl Window {
    fn mean_latency(&self) -> Option<Duration> {
        (self.answers > 0).then(|| self.latency_sum / self.answers)
    }
}

/// The limit state of one activity type.
#[derive(Debug)]
struct Limiter {
    policy: AdaptiveLimitPolicy,
    /// The cap as a real number. The integer cap is its floor.
    limit: f64,
    in_flight: u32,
    /// The lowest window mean latency since the last probe.
    baseline: Option<Duration>,
    /// Each probe adds 1. An answer from an older epoch does not count.
    epoch: u64,
    /// Samples since the last probe.
    samples: u32,
    /// The next closed window is the probe window. It sets the baseline.
    probing: bool,
    /// The cap to restore after the probe window.
    resume_limit: f64,
    window: Window,
}

impl Limiter {
    /// A new limiter starts with a probe, so its first baseline is measured
    /// at a low concurrency.
    fn new(policy: AdaptiveLimitPolicy) -> Self {
        let mut limiter = Self {
            policy,
            limit: QUEUE_SIZE.clamp(f64::from(policy.min_limit), f64::from(policy.max_limit)),
            in_flight: 0,
            baseline: None,
            epoch: 0,
            samples: 0,
            probing: false,
            resume_limit: 0.0,
            window: Window::default(),
        };
        limiter.start_probe();
        limiter
    }

    fn floor(&self) -> f64 {
        f64::from(self.policy.min_limit)
    }

    fn ceiling(&self) -> f64 {
        f64::from(self.policy.max_limit)
    }

    /// The integer cap on in-flight attempts.
    fn cap(&self) -> u32 {
        // The value is finite and inside the policy range, so the cast is
        // exact after the floor.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let cap = self.limit.floor() as u32;
        cap.clamp(self.policy.min_limit, self.policy.max_limit)
    }

    /// Samples that close a window: about one round trip of the current
    /// cap, and never fewer than [`PROBE_SAMPLES`].
    fn window_size(&self) -> u32 {
        self.cap().max(PROBE_SAMPLES)
    }

    /// Drop the cap and forget the baseline. The attempts in flight started
    /// at a high concurrency, so their answers do not count. The probe
    /// window then measures the baseline at a low concurrency. A probe never
    /// raises a cap that is already below [`QUEUE_SIZE`].
    fn start_probe(&mut self) {
        self.resume_limit = self.limit;
        self.limit = QUEUE_SIZE
            .min(self.limit)
            .clamp(self.floor(), self.ceiling());
        self.epoch += 1;
        self.baseline = None;
        self.samples = 0;
        self.probing = true;
        self.window = Window::default();
    }

    /// Add one sample to the window, and close the window when it is full.
    ///
    /// A failure from an older epoch counts as a failure. Overload is
    /// overload. It does not fill the window, so it cannot close a probe
    /// window before a fresh sample arrives. Every completion, of any epoch,
    /// counts toward the failure share.
    fn on_sample(&mut self, epoch: u64, latency: Duration, in_flight: u32, outcome: SampleOutcome) {
        self.window.completions = self.window.completions.saturating_add(1);
        match outcome {
            SampleOutcome::Overloaded => {
                self.window.failures += 1;
                if epoch != self.epoch {
                    return;
                }
            }
            SampleOutcome::Answered if epoch == self.epoch => {
                self.window.answers += 1;
                self.window.latency_sum = self.window.latency_sum.saturating_add(latency);
            }
            SampleOutcome::Answered => return,
        }
        self.window.samples += 1;
        self.window.max_in_flight = self.window.max_in_flight.max(in_flight);
        self.samples = self.samples.saturating_add(1);
        if self.window.samples >= self.window_size() {
            let window = std::mem::take(&mut self.window);
            self.close_window(&window);
        }
    }

    /// Move the cap once for a full window.
    ///
    /// A window is overloaded when the share of retryable failures in its
    /// completions passes `error_threshold`. An overloaded window cuts the cap by
    /// `backoff_ratio`. The probe window ends only with an answer, and then
    /// restores the cap from before the probe. Otherwise the cap moves
    /// toward `limit * gradient + QUEUE_SIZE`. The gradient is
    /// `tolerance * baseline / mean latency`, in the range from 0.5 to 1.
    fn close_window(&mut self, window: &Window) {
        let mean = window.mean_latency();
        if let Some(mean) = mean {
            self.baseline = Some(self.baseline.map_or(mean, |b| b.min(mean)));
        }
        let overloaded = f64::from(window.failures)
            > self.policy.error_threshold * f64::from(window.completions);
        if self.probing {
            if overloaded {
                self.resume_limit =
                    (self.resume_limit * self.policy.backoff_ratio).max(self.floor());
                self.limit = self.limit.min(self.resume_limit);
            }
            // The probe needs one answer for its baseline.
            if mean.is_none() {
                return;
            }
            self.probing = false;
            self.limit = self.resume_limit;
            return;
        }
        if overloaded {
            self.limit = (self.limit * self.policy.backoff_ratio).max(self.floor());
            return;
        }
        if self.samples >= self.policy.probe_interval {
            self.start_probe();
            return;
        }
        let (Some(mean), Some(baseline)) = (mean, self.baseline) else {
            return;
        };
        // A type that uses less than half of its cap says nothing about
        // more load.
        if f64::from(window.max_in_flight) < self.limit / 2.0 {
            return;
        }
        // A zero latency would divide by zero. Treat it as no queueing.
        let gradient = if mean.is_zero() {
            1.0
        } else {
            (self.policy.tolerance * baseline.as_secs_f64() / mean.as_secs_f64())
                .clamp(MIN_GRADIENT, 1.0)
        };
        let target = self.limit.mul_add(gradient, QUEUE_SIZE);
        self.limit = SMOOTHING
            .mul_add(target - self.limit, self.limit)
            .clamp(self.floor(), self.ceiling());
    }

    /// The delay for a task that found the type at its cap. A slot frees in
    /// about one handler latency, so the delay is one to two baselines.
    fn defer_delay(&self) -> Duration {
        // Cap the base first, so the product cannot overflow.
        let base = self
            .baseline
            .unwrap_or(MIN_LIMIT_DEFER)
            .min(MAX_LIMIT_DEFER);
        base.mul_f64(1.0 + rand::random::<f64>())
            .clamp(MIN_LIMIT_DEFER, MAX_LIMIT_DEFER)
    }

    fn is_saturated(&self) -> bool {
        self.in_flight >= self.cap()
    }

    fn snapshot(&self) -> LimitSnapshot {
        LimitSnapshot {
            limit: self.cap(),
            in_flight: self.in_flight,
            baseline: self.baseline,
        }
    }
}

/// One in-flight attempt.
///
/// [`complete`](Self::complete) reports the handler latency and outcome.
/// Dropping the permit frees the slot without a sample. Use the drop for an
/// attempt that did not reach the dependency or that was cancelled.
#[derive(Debug)]
pub struct LimitPermit {
    registry: Arc<AdaptiveLimitRegistry>,
    activity_name: String,
    epoch: u64,
    /// In-flight attempts when this one started, itself included.
    in_flight: u32,
    settled: bool,
}

impl LimitPermit {
    /// Report the handler latency and outcome, and free the slot.
    pub fn complete(mut self, latency: Duration, outcome: SampleOutcome) {
        self.settled = true;
        self.registry.release(
            &self.activity_name,
            Some((self.epoch, latency, self.in_flight, outcome)),
        );
    }
}

impl Drop for LimitPermit {
    fn drop(&mut self) {
        if !self.settled {
            self.registry.release(&self.activity_name, None);
        }
    }
}

/// In-process registry of per-activity-type adaptive limits.
///
/// The worker builds one registry and shares it behind an `Arc`.
#[derive(Default)]
pub struct AdaptiveLimitRegistry {
    config: AdaptiveLimitConfig,
    limiters: Mutex<HashMap<String, Limiter>>,
    /// Activity types at their cap. The claim path reads it without the
    /// lock.
    saturated_count: AtomicUsize,
    /// Wakes the worker's idle wait when a type leaves its cap. The worker
    /// uses it as its capacity wake.
    slot_freed: Arc<tokio::sync::Notify>,
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

/// One sample: the epoch, the latency, the in-flight count at start and the
/// outcome.
type Sample = (u64, Duration, u32, SampleOutcome);

impl AdaptiveLimitRegistry {
    /// Build a registry from `config`.
    #[must_use]
    pub fn new(config: AdaptiveLimitConfig) -> Self {
        Self {
            config,
            limiters: Mutex::new(HashMap::new()),
            saturated_count: AtomicUsize::new(0),
            slot_freed: Arc::new(tokio::sync::Notify::new()),
            metrics: None,
        }
    }

    /// Publish the limit gauges through `metrics`. The registry sends the
    /// state after every change, under its lock. A call that changes nothing
    /// sends nothing.
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

    fn lock(&self) -> MutexGuard<'_, HashMap<String, Limiter>> {
        // The state is plain numbers, so a poisoned lock is safe to reuse.
        self.limiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Run `f` on the limiter of `activity_name`, and publish a changed
    /// state. Returns `None` when the type has no limit.
    fn with_limiter<T>(&self, activity_name: &str, f: impl FnOnce(&mut Limiter) -> T) -> Option<T> {
        let policy = self.config.policy_for(activity_name)?;
        let mut limiters = self.lock();
        // Look up first, so the common path does not allocate a key.
        let created = !limiters.contains_key(activity_name);
        if created {
            limiters.insert(activity_name.to_owned(), Limiter::new(policy));
        }
        let limiter = limiters.get_mut(activity_name)?;
        let before = limiter.snapshot();
        let was_saturated = limiter.is_saturated();
        let out = f(limiter);
        let after = limiter.snapshot();
        // The count changes under the lock, so it never goes below 0.
        match (was_saturated, limiter.is_saturated()) {
            (false, true) => {
                self.saturated_count.fetch_add(1, Ordering::Relaxed);
            }
            (true, false) => {
                self.saturated_count.fetch_sub(1, Ordering::Relaxed);
                // A slot frees before the attempt writes its result. Wake the
                // idle poll loop now, not when the dispatch permit drops.
                self.slot_freed.notify_one();
            }
            _ => {}
        }
        // Publish under the lock, so the samples follow the change order.
        if let Some(metrics) = &self.metrics
            && (created || after != before)
        {
            metrics.record_activity_concurrency_limit(activity_name, &after);
        }
        drop(limiters);
        Some(out)
    }

    /// The wake that fires when an activity type leaves its cap. The worker
    /// shares it as its capacity wake.
    #[cfg(feature = "db")]
    #[must_use]
    pub(crate) fn slot_freed_notify(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.slot_freed)
    }

    /// Whether any activity type is at its cap. It reads no lock, so the
    /// claim path can call it on every poll.
    #[must_use]
    pub fn any_saturated(&self) -> bool {
        self.saturated_count.load(Ordering::Relaxed) > 0
    }

    /// Take a slot for one attempt of `activity_name`.
    #[must_use]
    pub fn try_acquire(self: &Arc<Self>, activity_name: &str) -> Acquire {
        let taken = self.with_limiter(activity_name, |limiter| {
            if limiter.in_flight >= limiter.cap() {
                return Err(limiter.defer_delay());
            }
            limiter.in_flight += 1;
            Ok((limiter.epoch, limiter.in_flight))
        });
        match taken {
            None => Acquire::Untracked,
            Some(Err(retry_after)) => Acquire::Limited { retry_after },
            Some(Ok((epoch, in_flight))) => Acquire::Acquired(LimitPermit {
                registry: Arc::clone(self),
                activity_name: activity_name.to_owned(),
                epoch,
                in_flight,
                settled: false,
            }),
        }
    }

    /// Free one slot of `activity_name` and apply its sample, if any.
    fn release(&self, activity_name: &str, sample: Option<Sample>) {
        self.with_limiter(activity_name, |limiter| {
            limiter.in_flight = limiter.in_flight.saturating_sub(1);
            if let Some((epoch, latency, in_flight, outcome)) = sample {
                limiter.on_sample(epoch, latency, in_flight, outcome);
            }
        });
    }

    /// The activity types that are at their cap, sorted by name. The worker
    /// does not claim tasks of these types.
    #[must_use]
    pub fn saturated(&self) -> Vec<String> {
        if !self.any_saturated() {
            return Vec::new();
        }
        let mut names: Vec<String> = self
            .lock()
            .iter()
            .filter(|(_, limiter)| limiter.in_flight >= limiter.cap())
            .map(|(name, _)| name.clone())
            .collect();
        names.sort_unstable();
        names
    }

    /// The delay before a task of `activity_name` should try again, when the
    /// type is at its cap. `None` when the type has a free slot or no limit.
    ///
    /// The delay is one to two baselines, the time in which a slot is likely
    /// to free. The dispatch channel releases such a reference for this delay
    /// instead of its gate backoff.
    #[must_use]
    pub fn saturated_delay(&self, activity_name: &str) -> Option<Duration> {
        if !self.any_saturated() {
            return None;
        }
        self.lock()
            .get(activity_name)
            .filter(|limiter| limiter.is_saturated())
            .map(Limiter::defer_delay)
    }

    /// The limit state of `activity_name`, or `None` when it has no state.
    #[must_use]
    pub fn snapshot(&self, activity_name: &str) -> Option<LimitSnapshot> {
        self.lock().get(activity_name).map(Limiter::snapshot)
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

    /// The current cap. The first call creates the state.
    fn limit(reg: &Arc<AdaptiveLimitRegistry>, name: &str) -> u32 {
        if reg.snapshot(name).is_none() {
            drop(permit(reg, name));
        }
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
            error_threshold: f64::NAN,
            probe_interval: 0,
        }
        .sanitized();
        assert_eq!(p.min_limit, 1);
        assert_eq!(p.max_limit, 1);
        assert!((p.tolerance - 1.0).abs() < f64::EPSILON);
        assert!((p.backoff_ratio - AdaptiveLimitPolicy::DEFAULT_BACKOFF_RATIO).abs() < 1e-12);
        assert!((p.error_threshold - AdaptiveLimitPolicy::DEFAULT_ERROR_THRESHOLD).abs() < 1e-12);
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
        assert!(reg.any_saturated());
        let delay = reg
            .saturated_delay(A)
            .expect("a saturated type has a delay");
        assert!((MIN_LIMIT_DEFER..=MAX_LIMIT_DEFER).contains(&delay));
        assert_eq!(reg.saturated_delay(B), None);
        drop(held);
        assert!(!reg.any_saturated());
        assert_eq!(reg.saturated_delay(A), None);
        assert_eq!(reg.snapshot(A).expect("state").in_flight, 0);
        assert_eq!(reg.saturated(), Vec::<String>::new());
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
        busy_rounds(&reg, A, 20, MS_100);
        assert!(limit(&reg, A) > 15, "limit {}", limit(&reg, A));
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

    /// Latency far above the baseline cuts the limit. The gradient floor
    /// keeps each window target at half of the cap or more.
    #[test]
    fn inflated_latency_shrinks_the_limit() {
        let reg = registry(AdaptiveLimitPolicy::default());
        busy_rounds(&reg, A, 20, MS_100);
        let before = limit(&reg, A);
        busy_rounds(&reg, A, 3, MS_100 * 10);
        let after = limit(&reg, A);
        assert!(after < before, "{before} -> {after}");
        assert!(after >= 1);
    }

    /// A window whose failure share passes the threshold cuts the cap by the
    /// ratio.
    #[test]
    fn a_retryable_failure_backs_off_by_the_ratio() {
        let reg = registry(AdaptiveLimitPolicy::default());
        busy_rounds(&reg, A, 20, MS_100);
        let before = limit(&reg, A);
        let held: Vec<LimitPermit> = (0..before).map(|_| permit(&reg, A)).collect();
        for (i, p) in held.into_iter().enumerate() {
            let outcome = if i % 4 == 0 {
                SampleOutcome::Overloaded
            } else {
                SampleOutcome::Answered
            };
            p.complete(MS_100, outcome);
        }
        let after = f64::from(limit(&reg, A));
        let expected = (f64::from(before) * 0.9).floor();
        assert!((after - expected).abs() <= 1.0, "{before} -> {after}");
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
            assert!(steps <= 100, "no probe after {steps} samples");
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
        round(
            &reg,
            A,
            1,
            Duration::from_millis(400),
            SampleOutcome::Answered,
        );
        for _ in 0..3 {
            round(
                &reg,
                A,
                1,
                Duration::from_millis(400),
                SampleOutcome::Answered,
            );
        }
        let _held = permit(&reg, A);
        for _ in 0..50 {
            let Acquire::Limited { retry_after } = reg.try_acquire(A) else {
                panic!("expected Limited");
            };
            assert!(
                retry_after >= Duration::from_millis(400)
                    && retry_after <= Duration::from_millis(800),
                "{retry_after:?}"
            );
        }
    }

    /// A rare failure is noise, not overload. It must not cut the cap.
    #[test]
    fn a_failure_share_under_the_threshold_does_not_cut_the_cap() {
        let reg = registry(AdaptiveLimitPolicy::default());
        busy_rounds(&reg, A, 30, MS_100);
        let before = limit(&reg, A);
        assert!(before > 20, "limit {before}");
        let held: Vec<LimitPermit> = (0..before).map(|_| permit(&reg, A)).collect();
        for (i, p) in held.into_iter().enumerate() {
            let outcome = if i == 0 {
                SampleOutcome::Overloaded
            } else {
                SampleOutcome::Answered
            };
            p.complete(MS_100, outcome);
        }
        assert!(limit(&reg, A) >= before, "{before} -> {}", limit(&reg, A));
    }

    /// Failures of attempts from before a probe do not close the probe
    /// window. The baseline still comes from answers after the probe.
    #[test]
    fn stale_failures_do_not_close_the_probe_window() {
        let policy = AdaptiveLimitPolicy {
            probe_interval: AdaptiveLimitPolicy::MIN_PROBE_INTERVAL,
            ..AdaptiveLimitPolicy::default()
        };
        let mut limiter = Limiter::new(policy);
        let old = limiter.epoch;
        limiter.limit = 20.0;
        limiter.start_probe();
        for _ in 0..6 {
            limiter.on_sample(old, MS_100, 20, SampleOutcome::Overloaded);
        }
        assert!(limiter.probing, "stale failures closed the probe window");
        assert_eq!(limiter.baseline, None);
        let fresh = limiter.epoch;
        for _ in 0..4 {
            limiter.on_sample(fresh, MS_100, 4, SampleOutcome::Answered);
        }
        assert!(!limiter.probing);
        assert_eq!(limiter.baseline, Some(MS_100));
        assert!(limiter.cap() < 20, "the failures still cut the cap");
    }

    /// A probe window with no answer cannot set a baseline, so the probe
    /// goes on.
    #[test]
    fn a_probe_window_without_answers_keeps_probing() {
        let mut limiter = Limiter::new(AdaptiveLimitPolicy::default());
        let epoch = limiter.epoch;
        for _ in 0..4 {
            limiter.on_sample(epoch, MS_100, 4, SampleOutcome::Overloaded);
        }
        assert!(limiter.probing);
        assert_eq!(limiter.baseline, None);
        assert!(limiter.cap() < 4, "the failures cut the cap");
    }

    /// A probe never raises a cap that is already below the probe cap.
    #[test]
    fn a_probe_never_raises_a_low_cap() {
        let mut limiter = Limiter::new(AdaptiveLimitPolicy::default());
        limiter.limit = 1.0;
        limiter.start_probe();
        assert_eq!(limiter.cap(), 1);
        assert!((limiter.resume_limit - 1.0).abs() < f64::EPSILON);
    }

    /// A huge baseline cannot overflow the delay arithmetic.
    #[test]
    fn the_defer_delay_survives_a_huge_baseline() {
        let mut limiter = Limiter::new(AdaptiveLimitPolicy::default());
        limiter.baseline = Some(Duration::MAX);
        assert_eq!(limiter.defer_delay(), MAX_LIMIT_DEFER);
    }

    /// Records every limit sample, in order.
    #[derive(Default)]
    struct LimitLog(Mutex<Vec<LimitSnapshot>>);

    impl LimitLog {
        fn len(&self) -> usize {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len()
        }
    }

    impl MetricsRecorder for LimitLog {
        fn record_activity_concurrency_limit(&self, _activity: &str, state: &LimitSnapshot) {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(*state);
        }
    }

    /// A slot that leaves saturation wakes the worker's idle wait at once,
    /// before the attempt writes its result.
    #[test]
    fn a_freed_slot_notifies_the_worker() {
        use futures::FutureExt as _;
        let reg = registry(AdaptiveLimitPolicy::new(1, 1));
        let freed = Arc::clone(&reg.slot_freed);
        let held = permit(&reg, A);
        assert!(
            freed.notified().now_or_never().is_none(),
            "no slot freed yet"
        );
        drop(held);
        assert!(
            freed.notified().now_or_never().is_some(),
            "the free must wake"
        );
    }

    /// A refused attempt changes nothing, so it publishes nothing.
    #[test]
    fn a_limited_attempt_publishes_nothing() {
        let log = Arc::new(LimitLog::default());
        let reg = Arc::new(
            AdaptiveLimitRegistry::new(
                AdaptiveLimitConfig::disabled().with_default(Some(AdaptiveLimitPolicy::new(1, 1))),
            )
            .with_metrics(log.clone()),
        );
        let _held = permit(&reg, A);
        let before = log.len();
        assert!(matches!(reg.try_acquire(A), Acquire::Limited { .. }));
        assert_eq!(log.len(), before);
    }

    /// Every change publishes the state under the lock, so the last sample
    /// always equals the state, even under concurrent use.
    #[test]
    fn the_last_metric_sample_matches_the_state_under_concurrency() {
        let log = Arc::new(LimitLog::default());
        let reg = Arc::new(
            AdaptiveLimitRegistry::new(
                AdaptiveLimitConfig::disabled().with_default(Some(AdaptiveLimitPolicy::new(1, 64))),
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
        assert_eq!(Some(last), reg.snapshot(A));
        assert_eq!(last.in_flight, 0);
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
        /// Share of calls that fail with a retryable error whatever the load.
        error_rate: f64,
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
                let noisy = rng.unit() < downstream.error_rate;
                let outcome = if noisy || (downstream.fail_above_knee && n > downstream.knee) {
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
            error_rate: 0.0,
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
            error_rate: 0.0,
            fail_above_knee: false,
        };
        let trace = simulate(policy, &downstream, 20_000);
        let settled = median(&trace.limits[10_000..]);
        let fixed_point = policy.tolerance.mul_add(f64::from(knee), QUEUE_SIZE);
        assert!(
            f64::from(settled) >= 0.75 * f64::from(knee) && f64::from(settled) <= fixed_point + 2.0,
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
            error_rate: 0.0,
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
            error_rate: 0.0,
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

    /// A healthy dependency with a 1 % background error rate keeps about
    /// the cap of an error-free run. Only overload, not noise, may throttle
    /// it.
    #[test]
    fn background_errors_do_not_throttle_a_healthy_dependency() {
        let policy = AdaptiveLimitPolicy::default();
        let run = |error_rate| {
            let downstream = Downstream {
                knee: 1_000,
                error_rate,
                base: Duration::from_millis(100),
                noise: 0.0,
                fail_above_knee: false,
            };
            let trace = simulate(policy, &downstream, 40_000);
            median(&trace.limits[20_000..])
        };
        let clean = run(0.0);
        let noisy = run(0.01);
        assert!(
            f64::from(noisy) >= 0.9 * f64::from(clean),
            "1 % errors cut the cap from {clean} to {noisy}"
        );
    }
}
