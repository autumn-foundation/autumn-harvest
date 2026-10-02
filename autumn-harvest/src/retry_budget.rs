//! Per-activity-type retry budget (issue #1793).
//!
//! A retry policy limits the retries of one task. It does not limit retries in
//! aggregate. During a dependency brownout, every failing task retries, and the
//! retries multiply the load on the dependency.
//!
//! A retry budget caps that load. The worker keeps one token bucket for each
//! activity type:
//!
//! - The bucket starts full at `max_tokens`.
//! - A first attempt deposits `ratio` tokens, up to `max_tokens`, when it
//!   starts. Until then the deposit is pending and no retry can spend it.
//! - A retry spends one token.
//! - Time adds `min_retries_per_sec` tokens each second, up to `max_tokens`.
//!
//! When the bucket holds less than one token, the worker defers the retry. The
//! task row goes back to `PENDING` at a later `scheduled_at`. The deferral does
//! not use an attempt and appends no event. **A deferred retry is never lost.**
//! Only an activity timeout, or a cancel or reset of the owning run, can end
//! it.
//!
//! A half-open circuit-breaker probe is never deferred. Deferred retries are
//! not served in order.
//!
//! See [`RetryBudgetPolicy`](crate::policy::RetryBudgetPolicy) for the knobs
//! and `docs/architecture.md`, design decision 10, for the full rules.
//!
//! ## Scope and durability
//!
//! State is in process and per worker, like
//! [`crate::circuit_breaker`]. It never touches the event log, so replay is
//! unaffected. Each worker process enforces its own budget, so a fleet of N
//! workers allows up to N budgets.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use crate::policy::RetryBudgetPolicy;
use crate::telemetry::MetricsRecorder;

/// Shortest deferral. It keeps a deferred retry from spinning on the claim
/// path.
pub const MIN_RETRY_BUDGET_DEFER: Duration = Duration::from_millis(50);

/// Longest deferral. A deferred retry checks the bucket again at least this
/// often.
pub const MAX_RETRY_BUDGET_DEFER: Duration = Duration::from_secs(60);

/// Tolerance for the spend test. Ten deposits of 0.1 sum to slightly less
/// than 1.0 in floating point, and they must still fund one retry.
const SPEND_EPSILON: f64 = 1e-9;

/// Which activity types have a retry budget, and with which policy.
///
/// The default config gives every activity type the default
/// [`RetryBudgetPolicy`]. A per-type override replaces the default policy for
/// one activity name. An override of `None` turns the budget off for that type.
#[derive(Debug, Clone, PartialEq)]
pub struct RetryBudgetConfig {
    default_policy: Option<RetryBudgetPolicy>,
    overrides: HashMap<String, Option<RetryBudgetPolicy>>,
}

impl Default for RetryBudgetConfig {
    fn default() -> Self {
        Self {
            default_policy: Some(RetryBudgetPolicy::default()),
            overrides: HashMap::new(),
        }
    }
}

impl RetryBudgetConfig {
    /// A config with no budget for any activity type.
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            default_policy: None,
            overrides: HashMap::new(),
        }
    }

    /// Set the policy for every activity type without an override. `None`
    /// turns the default off.
    #[must_use]
    pub fn with_default(mut self, policy: Option<RetryBudgetPolicy>) -> Self {
        self.default_policy = policy.map(sanitize);
        self
    }

    /// Set the policy for one activity type. `None` turns the budget off for
    /// that type.
    #[must_use]
    pub fn with_activity(
        mut self,
        activity_name: impl Into<String>,
        policy: Option<RetryBudgetPolicy>,
    ) -> Self {
        self.overrides
            .insert(activity_name.into(), policy.map(sanitize));
        self
    }

    /// The policy for every activity type without an override.
    #[must_use]
    pub const fn default_policy(&self) -> Option<RetryBudgetPolicy> {
        self.default_policy
    }

    /// The per-type overrides, sorted by activity name.
    #[must_use]
    pub fn overrides(&self) -> std::collections::BTreeMap<String, Option<RetryBudgetPolicy>> {
        self.overrides
            .iter()
            .map(|(name, policy)| (name.clone(), *policy))
            .collect()
    }

    /// The policy that applies to `activity_name`, or `None` when it has no
    /// budget.
    #[must_use]
    pub fn policy_for(&self, activity_name: &str) -> Option<RetryBudgetPolicy> {
        self.overrides
            .get(activity_name)
            .copied()
            .unwrap_or(self.default_policy)
    }
}

/// Re-apply the constructor rules. The policy fields are public, so a caller
/// can set a NaN or a negative value directly.
fn sanitize(policy: RetryBudgetPolicy) -> RetryBudgetPolicy {
    RetryBudgetPolicy::new(policy.ratio, policy.max_tokens, policy.min_retries_per_sec)
}

/// The budget decision for one admitted attempt.
///
/// Give it back to [`RetryBudgetRegistry::commit`] once the attempt starts.
/// Give it back to [`RetryBudgetRegistry::release`] when the attempt does not
/// run, for example when a rate limit defers it.
///
/// The ticket is not `Copy`, so one ticket is settled only once.
#[derive(Debug, PartialEq)]
pub struct BudgetTicket {
    kind: TicketKind,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum TicketKind {
    /// A first attempt. Its deposit counts only when the ticket commits.
    PendingDeposit,
    /// A retry spent one token.
    Spent,
}

/// Outcome of [`RetryBudgetRegistry::admit`].
#[derive(Debug, PartialEq)]
pub enum Admission {
    /// The activity type has no budget. Run the attempt.
    Untracked,
    /// Run the attempt. Commit `ticket` when it starts, or release it when it
    /// does not run.
    Admitted {
        /// Settles the decision through [`RetryBudgetRegistry::commit`] or
        /// [`RetryBudgetRegistry::release`].
        ticket: BudgetTicket,
        /// Tokens left after this decision.
        available: f64,
    },
    /// The bucket is empty. Defer the retry by `retry_after`. Do not run it.
    Deferred {
        /// Delay before the retry is claimable again.
        retry_after: Duration,
        /// Tokens left after this decision.
        available: f64,
        /// The wake-up slot this deferral reserved, if any. Give it to
        /// [`RetryBudgetRegistry::cancel_deferral`] when the deferral is not
        /// persisted.
        reservation: Option<SlotReservation>,
    },
}

/// A wake-up slot that one deferral reserved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotReservation {
    slot: Instant,
    previous: Instant,
}

#[derive(Debug)]
struct Bucket {
    tokens: f64,
    refilled_at: Instant,
    /// Latest wake-up slot given to a deferred retry. Later deferrals are
    /// spaced after it, at the refill rate.
    next_slot: Instant,
    /// Cancelled reservations that are not yet the tail. When the tail is
    /// cancelled, the rollback continues through these.
    cancelled: Vec<SlotReservation>,
}

/// In-process registry of per-activity-type retry budgets.
///
/// The worker builds one registry and shares it behind an `Arc`.
pub struct RetryBudgetRegistry {
    config: RetryBudgetConfig,
    buckets: Mutex<HashMap<String, Bucket>>,
    metrics: Option<Arc<dyn MetricsRecorder>>,
}

impl std::fmt::Debug for RetryBudgetRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetryBudgetRegistry")
            .field("config", &self.config)
            .field("buckets", &self.buckets)
            .field("metrics", &self.metrics.is_some())
            .finish()
    }
}

impl Default for RetryBudgetRegistry {
    fn default() -> Self {
        Self::new(RetryBudgetConfig::default())
    }
}

impl RetryBudgetRegistry {
    /// Build a registry from `config`.
    #[must_use]
    pub fn new(config: RetryBudgetConfig) -> Self {
        Self {
            config,
            buckets: Mutex::new(HashMap::new()),
            metrics: None,
        }
    }

    /// Publish `harvest.retry.budget.available` through `metrics`. The
    /// registry sends a sample after every bucket access, under the bucket
    /// lock.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Give back the wake-up slot of a deferral that was not persisted.
    ///
    /// A slot is freed when no live deferral reserved a slot after it.
    /// Cancellations can arrive in any order. A cancelled slot that is not
    /// yet the tail is freed when every slot after it is cancelled too.
    pub fn cancel_deferral(&self, activity_name: &str, reservation: SlotReservation, now: Instant) {
        self.with_bucket(activity_name, now, |bucket, _| {
            // A slot that has passed needs no rollback, and an old entry
            // can never become the tail again. The gate ignores a tail that
            // has passed, so such a slot is not stored.
            bucket.cancelled.retain(|r| r.slot > now);
            if reservation.slot <= now {
                return;
            }
            bucket.cancelled.push(reservation);
            // Walk back from the tail through every cancelled slot.
            while let Some(i) = bucket
                .cancelled
                .iter()
                .position(|r| r.slot == bucket.next_slot)
            {
                bucket.next_slot = bucket.cancelled.swap_remove(i).previous;
            }
        });
    }

    /// The config this registry enforces.
    #[must_use]
    pub const fn config(&self) -> &RetryBudgetConfig {
        &self.config
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, Bucket>> {
        // The state is plain numbers, so a poisoned lock is safe to reuse.
        self.buckets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Run `f` on the refilled bucket of `activity_name`. Returns `None` when
    /// the type has no budget.
    fn with_bucket<T>(
        &self,
        activity_name: &str,
        now: Instant,
        f: impl FnOnce(&mut Bucket, &RetryBudgetPolicy) -> T,
    ) -> Option<T> {
        let policy = self.config.policy_for(activity_name)?;
        let mut buckets = self.lock();
        // Look up first, so the common path does not allocate a key.
        if !buckets.contains_key(activity_name) {
            buckets.insert(activity_name.to_owned(), Bucket::full(&policy, now));
        }
        let bucket = buckets.get_mut(activity_name)?;
        bucket.refill(&policy, now);
        let out = f(bucket, &policy);
        // Publish under the lock, so the samples follow the mutation order.
        // A sample sent after the unlock could overwrite a newer one.
        if let Some(metrics) = &self.metrics {
            metrics.record_retry_budget_available(activity_name, bucket.tokens);
        }
        drop(buckets);
        Some(out)
    }

    /// Decide whether an attempt of `activity_name` may run at `now`.
    ///
    /// A first attempt (`is_retry == false`) always runs. Its deposit is
    /// pending until [`commit`](Self::commit), so no retry can spend tokens
    /// from an attempt that may never start. A retry runs only when it can
    /// spend one token.
    #[must_use]
    pub fn admit(&self, activity_name: &str, is_retry: bool, now: Instant) -> Admission {
        self.with_bucket(activity_name, now, |bucket, policy| {
            if !is_retry {
                return Admission::Admitted {
                    ticket: BudgetTicket {
                        kind: TicketKind::PendingDeposit,
                    },
                    available: bucket.tokens,
                };
            }
            if bucket.tokens >= 1.0 - SPEND_EPSILON {
                bucket.tokens -= 1.0;
                return Admission::Admitted {
                    ticket: BudgetTicket {
                        kind: TicketKind::Spent,
                    },
                    available: bucket.tokens,
                };
            }
            let (retry_after, reservation) = bucket.reserve_slot(policy, now);
            Admission::Deferred {
                retry_after,
                available: bucket.tokens,
                reservation,
            }
        })
        .unwrap_or(Admission::Untracked)
    }

    /// Settle `ticket` for an attempt that started.
    ///
    /// A first attempt deposits `ratio` tokens here, up to `max_tokens`. A
    /// retry already spent its token, so its commit changes nothing.
    ///
    /// Returns the tokens left, or `None` when the type has no budget.
    // The ticket is taken by value so that one ticket is settled only once.
    #[allow(clippy::needless_pass_by_value)]
    pub fn commit(&self, activity_name: &str, ticket: BudgetTicket, now: Instant) -> Option<f64> {
        self.with_bucket(activity_name, now, |bucket, policy| {
            if ticket.kind == TicketKind::PendingDeposit {
                bucket.tokens = (bucket.tokens + policy.ratio).min(policy.max_tokens);
            }
            bucket.tokens
        })
    }

    /// Settle `ticket` for an attempt that did not run.
    ///
    /// A retry gets its token back, up to `max_tokens`. Without the spend,
    /// the bucket would hold that token, and the cap would apply the same
    /// way. A pending deposit never entered the bucket, so its release
    /// changes nothing.
    ///
    /// Returns the tokens left, or `None` when the type has no budget.
    // The ticket is taken by value so that one ticket is settled only once.
    #[allow(clippy::needless_pass_by_value)]
    pub fn release(&self, activity_name: &str, ticket: BudgetTicket, now: Instant) -> Option<f64> {
        self.with_bucket(activity_name, now, |bucket, policy| {
            if ticket.kind == TicketKind::Spent {
                bucket.tokens = (bucket.tokens + 1.0).min(policy.max_tokens);
            }
            bucket.tokens
        })
    }

    /// Tokens available to `activity_name` at `now`, or `None` when the type
    /// has no budget.
    #[must_use]
    pub fn available(&self, activity_name: &str, now: Instant) -> Option<f64> {
        self.with_bucket(activity_name, now, |bucket, _| bucket.tokens)
    }
}

impl Bucket {
    const fn full(policy: &RetryBudgetPolicy, now: Instant) -> Self {
        Self {
            tokens: policy.max_tokens,
            refilled_at: now,
            next_slot: now,
            cancelled: Vec::new(),
        }
    }

    /// Add the time refill since the last call. Callers read `now` before
    /// they take the lock, so `now` can be earlier than `refilled_at`.
    fn refill(&mut self, policy: &RetryBudgetPolicy, now: Instant) {
        let elapsed = now
            .saturating_duration_since(self.refilled_at)
            .as_secs_f64();
        self.tokens = elapsed
            .mul_add(policy.min_retries_per_sec, self.tokens)
            .min(policy.max_tokens);
        self.refilled_at = self.refilled_at.max(now);
    }

    /// Give a deferred retry a wake-up slot and return the delay to it.
    ///
    /// The first slot is the time at which the time refill gives one token.
    /// While that slot is still pending, each later slot is one refill
    /// interval after the previous slot. Thus the first deferred retries wake
    /// one at a time, at the refill rate.
    ///
    /// A slot later than [`MAX_RETRY_BUDGET_DEFER`] is not reserved. That
    /// retry gets a random delay in the upper half of the cap instead. The
    /// random spread stops a large backlog from waking at one instant.
    fn reserve_slot(
        &mut self,
        policy: &RetryBudgetPolicy,
        now: Instant,
    ) -> (Duration, Option<SlotReservation>) {
        let rate = policy.min_retries_per_sec;
        let (until_token, interval) = if rate > 0.0 {
            let deficit = (1.0 - self.tokens).max(0.0);
            (secs(deficit / rate), secs(1.0 / rate))
        } else {
            (MAX_RETRY_BUDGET_DEFER, MAX_RETRY_BUDGET_DEFER)
        };
        let ready = now + until_token;
        // Space after a reservation that is still pending. An expired one
        // says nothing about the next token, so it does not delay this slot.
        let slot = if self.next_slot > now {
            ready.max(self.next_slot + interval)
        } else {
            ready
        };
        if slot.saturating_duration_since(now) >= MAX_RETRY_BUDGET_DEFER {
            return (overflow_delay(), None);
        }
        let reservation = SlotReservation {
            slot,
            previous: self.next_slot,
        };
        self.next_slot = slot;
        let delay = slot
            .saturating_duration_since(now)
            .max(MIN_RETRY_BUDGET_DEFER);
        (delay, Some(reservation))
    }
}

/// A random delay in the upper half of [`MAX_RETRY_BUDGET_DEFER`].
fn overflow_delay() -> Duration {
    let half = MAX_RETRY_BUDGET_DEFER / 2;
    half + half.mul_f64(rand::random::<f64>())
}

/// Convert seconds to a `Duration`, capped at [`MAX_RETRY_BUDGET_DEFER`].
fn secs(value: f64) -> Duration {
    Duration::try_from_secs_f64(value)
        .unwrap_or(MAX_RETRY_BUDGET_DEFER)
        .min(MAX_RETRY_BUDGET_DEFER)
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "charge_card";
    const B: &str = "send_email";

    fn registry(policy: RetryBudgetPolicy) -> RetryBudgetRegistry {
        RetryBudgetRegistry::new(RetryBudgetConfig::disabled().with_default(Some(policy)))
    }

    const fn is_admitted(a: &Admission) -> bool {
        matches!(a, Admission::Admitted { .. })
    }

    /// Admit a first attempt and commit it, as the worker does once the
    /// attempt starts.
    fn first(reg: &RetryBudgetRegistry, name: &str, now: Instant) {
        let t = ticket(reg.admit(name, false, now));
        reg.commit(name, t, now);
    }

    fn ticket(a: Admission) -> BudgetTicket {
        match a {
            Admission::Admitted { ticket, .. } => ticket,
            other => panic!("expected Admitted, got {other:?}"),
        }
    }

    fn retry_after(a: Admission) -> Duration {
        match a {
            Admission::Deferred { retry_after, .. } => retry_after,
            other => panic!("expected Deferred, got {other:?}"),
        }
    }

    /// Spend every whole token. Return how many retries ran. No retry is
    /// deferred, so no wake-up slot is used.
    fn drain(reg: &RetryBudgetRegistry, name: &str, now: Instant) -> u32 {
        let mut ran = 0;
        while reg.available(name, now).is_some_and(|t| t >= 1.0) {
            assert!(is_admitted(&reg.admit(name, true, now)));
            ran += 1;
            assert!(ran < 10_000, "bucket never ran dry");
        }
        ran
    }

    #[test]
    fn default_config_gives_every_type_the_default_policy() {
        let config = RetryBudgetConfig::default();
        assert_eq!(config.policy_for(A), Some(RetryBudgetPolicy::default()));
        let p = RetryBudgetPolicy::default();
        assert!((p.ratio - 0.1).abs() < f64::EPSILON);
        assert!((p.max_tokens - 10.0).abs() < f64::EPSILON);
        assert!((p.min_retries_per_sec - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn default_registry_is_on() {
        let reg = RetryBudgetRegistry::default();
        assert!(is_admitted(&reg.admit(A, true, Instant::now())));
    }

    #[test]
    fn disabled_config_tracks_nothing() {
        let reg = RetryBudgetRegistry::new(RetryBudgetConfig::disabled());
        let now = Instant::now();
        for _ in 0..1_000 {
            assert_eq!(reg.admit(A, true, now), Admission::Untracked);
        }
        assert_eq!(reg.available(A, now), None);
    }

    #[test]
    fn per_type_override_wins_over_the_default() {
        let custom = RetryBudgetPolicy::new(0.5, 2.0, 0.0);
        let config = RetryBudgetConfig::default()
            .with_activity(A, Some(custom))
            .with_activity(B, None);
        assert_eq!(config.policy_for(A), Some(custom));
        assert_eq!(config.policy_for(B), None);
        assert_eq!(config.policy_for("other"), config.default_policy());
        assert_eq!(config.overrides().len(), 2);

        let reg = RetryBudgetRegistry::new(config);
        let now = Instant::now();
        assert_eq!(drain(&reg, A, now), 2);
        assert_eq!(reg.admit(B, true, now), Admission::Untracked);
    }

    #[test]
    fn bucket_starts_full_and_retries_spend_one_token_each() {
        let reg = registry(RetryBudgetPolicy::new(0.1, 3.0, 0.0));
        let now = Instant::now();
        assert_eq!(reg.available(A, now), Some(3.0));
        assert_eq!(drain(&reg, A, now), 3);
        assert!(matches!(
            reg.admit(A, true, now),
            Admission::Deferred { .. }
        ));
    }

    #[test]
    fn first_attempts_always_run_and_deposit_ratio_tokens() {
        let reg = registry(RetryBudgetPolicy::new(0.5, 2.0, 0.0));
        let now = Instant::now();
        assert_eq!(drain(&reg, A, now), 2);
        for _ in 0..4 {
            first(&reg, A, now);
        }
        assert_eq!(reg.available(A, now), Some(2.0));
        assert_eq!(drain(&reg, A, now), 2);
    }

    #[test]
    fn deposits_never_exceed_capacity() {
        let reg = registry(RetryBudgetPolicy::new(1.0, 5.0, 0.0));
        let now = Instant::now();
        for _ in 0..100 {
            first(&reg, A, now);
        }
        assert_eq!(reg.available(A, now), Some(5.0));
    }

    #[test]
    fn time_refills_at_the_floor_rate() {
        let reg = registry(RetryBudgetPolicy::new(0.0, 4.0, 2.0));
        let t0 = Instant::now();
        assert_eq!(drain(&reg, A, t0), 4);
        let t1 = t0 + Duration::from_secs(1);
        assert_eq!(drain(&reg, A, t1), 2);
        let t2 = t1 + Duration::from_secs(60);
        assert_eq!(reg.available(A, t2), Some(4.0));
    }

    /// The core budget claim. With no time refill, retries that run never
    /// exceed `max_tokens + ratio * first_attempts`.
    #[test]
    fn retries_stay_within_the_budget_under_total_failure() {
        let policy = RetryBudgetPolicy::new(0.1, 10.0, 0.0);
        let reg = registry(policy);
        let now = Instant::now();
        let first_attempts = 500_u32;
        let mut retries_run = 0_u32;
        for _ in 0..first_attempts {
            first(&reg, A, now);
            // Every attempt fails, and each failed task asks to retry 5 times.
            for _ in 0..5 {
                if is_admitted(&reg.admit(A, true, now)) {
                    retries_run += 1;
                }
            }
        }
        let budget = policy
            .ratio
            .mul_add(f64::from(first_attempts), policy.max_tokens);
        assert!(
            f64::from(retries_run) <= budget + 1e-9,
            "{retries_run} retries ran; budget is {budget}"
        );
        assert!(f64::from(retries_run) >= budget - 1.0, "budget unused");
    }

    #[test]
    fn exhausting_one_type_leaves_other_types_unaffected() {
        let reg = registry(RetryBudgetPolicy::new(0.1, 3.0, 0.0));
        let now = Instant::now();
        assert_eq!(drain(&reg, A, now), 3);
        assert!(is_admitted(&reg.admit(B, true, now)));
        assert_eq!(reg.available(B, now), Some(2.0));
    }

    #[test]
    fn release_undoes_a_spent_retry() {
        let reg = registry(RetryBudgetPolicy::new(0.1, 2.0, 0.0));
        let now = Instant::now();
        let t = ticket(reg.admit(A, true, now));
        assert_eq!(reg.available(A, now), Some(1.0));
        assert_eq!(reg.release(A, t, now), Some(2.0));
    }

    /// A first attempt deposits only when it commits.
    #[test]
    fn a_deposit_counts_only_after_commit() {
        let reg = registry(RetryBudgetPolicy::new(0.5, 2.0, 0.0));
        let now = Instant::now();
        assert_eq!(drain(&reg, A, now), 2);
        let pending = ticket(reg.admit(A, false, now));
        assert_eq!(reg.available(A, now), Some(0.0));
        assert_eq!(reg.commit(A, pending, now), Some(0.5));
    }

    /// A pending deposit never entered the bucket, so its release changes
    /// nothing.
    #[test]
    fn releasing_a_pending_deposit_changes_nothing() {
        let reg = registry(RetryBudgetPolicy::new(0.5, 2.0, 0.0));
        let now = Instant::now();
        assert_eq!(drain(&reg, A, now), 2);
        let pending = ticket(reg.admit(A, false, now));
        assert_eq!(reg.release(A, pending, now), Some(0.0));
    }

    #[test]
    fn deferral_waits_for_the_next_token() {
        let reg = registry(RetryBudgetPolicy::new(0.0, 1.0, 2.0));
        let now = Instant::now();
        assert_eq!(drain(&reg, A, now), 1);
        let wait = retry_after(reg.admit(A, true, now));
        assert!(
            wait >= Duration::from_millis(450) && wait <= Duration::from_millis(550),
            "expected about 500 ms, got {wait:?}"
        );
    }

    /// With no live reservation, the first deferral waits only for the next
    /// token, not for a whole refill interval.
    #[test]
    fn first_deferral_waits_only_for_the_missing_fraction() {
        let reg = registry(RetryBudgetPolicy::new(0.9, 1.0, 1.0));
        let now = Instant::now();
        assert_eq!(drain(&reg, A, now), 1);
        first(&reg, A, now);
        let wait = retry_after(reg.admit(A, true, now));
        assert!(
            wait >= Duration::from_millis(90) && wait <= Duration::from_millis(110),
            "expected about 100 ms, got {wait:?}"
        );
    }

    /// A deferral that was not persisted gives its slot back, so the next
    /// deferral is not pushed later for nothing.
    #[test]
    fn cancelling_a_deferral_frees_its_slot() {
        let reg = registry(RetryBudgetPolicy::new(0.0, 1.0, 2.0));
        let now = Instant::now();
        assert_eq!(drain(&reg, A, now), 1);
        let Admission::Deferred {
            retry_after: first,
            reservation: Some(reservation),
            ..
        } = reg.admit(A, true, now)
        else {
            panic!("expected a reserved deferral");
        };
        reg.cancel_deferral(A, reservation, now);
        let second = retry_after(reg.admit(A, true, now));
        assert_eq!(first, second, "the cancelled slot must be free again");
    }

    /// Two reservations cancelled oldest first must both be freed. The
    /// rollback must not stop at a slot that is already cancelled.
    #[test]
    fn cancelling_reservations_in_order_frees_them_all() {
        let reg = registry(RetryBudgetPolicy::new(0.0, 1.0, 2.0));
        let now = Instant::now();
        assert_eq!(drain(&reg, A, now), 1);
        let reserve = |reg: &RetryBudgetRegistry| match reg.admit(A, true, now) {
            Admission::Deferred {
                retry_after,
                reservation: Some(reservation),
                ..
            } => (retry_after, reservation),
            other => panic!("expected a reserved deferral, got {other:?}"),
        };
        let (first_wait, first) = reserve(&reg);
        let (_, second) = reserve(&reg);
        reg.cancel_deferral(A, first, now);
        reg.cancel_deferral(A, second, now);
        let next = retry_after(reg.admit(A, true, now));
        assert_eq!(next, first_wait, "both cancelled slots must be free");
    }

    /// A cancelled slot that has already passed is not stored. A later live
    /// deferral can stay the tail, so nothing would remove that entry.
    #[test]
    fn cancelling_a_passed_slot_stores_nothing() {
        let reg = registry(RetryBudgetPolicy::new(0.0, 1.0, 2.0));
        let now = Instant::now();
        assert_eq!(drain(&reg, A, now), 1);
        let reserve = |reg: &RetryBudgetRegistry| match reg.admit(A, true, now) {
            Admission::Deferred {
                reservation: Some(reservation),
                ..
            } => reservation,
            other => panic!("expected a reserved deferral, got {other:?}"),
        };
        let first = reserve(&reg);
        let _live_tail = reserve(&reg);
        reg.cancel_deferral(A, first, first.slot);
        let stored = reg
            .buckets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(A)
            .map(|bucket| bucket.cancelled.len());
        assert_eq!(stored, Some(0), "a passed slot must not be stored");
    }

    /// Records every gauge sample, in order.
    #[derive(Default)]
    struct GaugeLog(std::sync::Mutex<Vec<f64>>);

    impl MetricsRecorder for GaugeLog {
        fn record_retry_budget_available(&self, _activity: &str, tokens: f64) {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(tokens);
        }
    }

    /// Every bucket change publishes the gauge under the bucket lock, so
    /// the last sample always equals the bucket level, even under
    /// concurrent decisions.
    #[test]
    fn the_last_gauge_sample_matches_the_bucket_under_concurrency() {
        let log = Arc::new(GaugeLog::default());
        let reg = Arc::new(
            RetryBudgetRegistry::new(
                RetryBudgetConfig::disabled()
                    .with_default(Some(RetryBudgetPolicy::new(0.5, 1_000.0, 0.0))),
            )
            .with_metrics(log.clone()),
        );
        let now = Instant::now();
        let threads: Vec<_> = (0..8)
            .map(|i| {
                let reg = Arc::clone(&reg);
                std::thread::spawn(move || {
                    for _ in 0..200 {
                        let t = ticket(reg.admit(A, i % 2 == 0, now));
                        reg.commit(A, t, now);
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
            .expect("the gauge was published");
        assert_eq!(Some(last), reg.available(A, now));
    }

    #[test]
    fn consecutive_deferrals_are_spaced_out() {
        let reg = registry(RetryBudgetPolicy::new(0.0, 1.0, 2.0));
        let now = Instant::now();
        assert_eq!(drain(&reg, A, now), 1);
        let first = retry_after(reg.admit(A, true, now));
        let second = retry_after(reg.admit(A, true, now));
        let third = retry_after(reg.admit(A, true, now));
        assert!(
            second >= first + Duration::from_millis(450),
            "{first:?} {second:?}"
        );
        assert!(
            third >= second + Duration::from_millis(450),
            "{second:?} {third:?}"
        );
    }

    #[test]
    fn deferral_is_capped_when_time_never_refills() {
        let reg = registry(RetryBudgetPolicy::new(0.1, 1.0, 0.0));
        let now = Instant::now();
        assert_eq!(drain(&reg, A, now), 1);
        for _ in 0..100 {
            let wait = retry_after(reg.admit(A, true, now));
            assert!(wait >= MIN_RETRY_BUDGET_DEFER && wait <= MAX_RETRY_BUDGET_DEFER);
        }
    }

    /// A pending deposit does not take up room under the cap, so the time
    /// refill is never lost while the attempt is held.
    #[test]
    fn a_held_deposit_never_displaces_the_time_refill() {
        let reg = registry(RetryBudgetPolicy::new(10.0, 10.0, 1.0));
        let t0 = Instant::now();
        assert_eq!(drain(&reg, A, t0), 10);
        let pending = ticket(reg.admit(A, false, t0));
        let t5 = t0 + Duration::from_secs(5);
        assert_eq!(reg.release(A, pending, t5), Some(5.0));
    }

    /// A released spend gets its token back, but never above the cap.
    #[test]
    fn a_released_spend_stays_under_the_cap() {
        let reg = registry(RetryBudgetPolicy::new(0.0, 10.0, 1.0));
        let t0 = Instant::now();
        let spend = ticket(reg.admit(A, true, t0));
        let t5 = t0 + Duration::from_secs(5);
        assert_eq!(reg.release(A, spend, t5), Some(10.0));
    }

    /// A first attempt that has not started cannot fund a retry. Its
    /// deposit may still be released if the attempt never runs.
    #[test]
    fn a_provisional_deposit_cannot_fund_a_retry() {
        let reg = registry(RetryBudgetPolicy::new(10.0, 10.0, 0.0));
        let now = Instant::now();
        assert_eq!(drain(&reg, A, now), 10);
        let _pending = ticket(reg.admit(A, false, now));
        assert!(matches!(
            reg.admit(A, true, now),
            Admission::Deferred { .. }
        ));
    }

    /// Ten deposits of 0.1 sum to slightly less than 1.0 in floating point.
    /// They must still fund one retry.
    #[test]
    fn ten_deposits_of_a_tenth_fund_one_retry() {
        let reg = registry(RetryBudgetPolicy::new(0.1, 1.0, 0.0));
        let now = Instant::now();
        assert_eq!(drain(&reg, A, now), 1);
        for _ in 0..10 {
            first(&reg, A, now);
        }
        assert!(is_admitted(&reg.admit(A, true, now)));
    }

    /// Deferrals past the cap are spread over the upper half of the cap. They
    /// must not all wake at the same instant.
    #[test]
    fn overflow_deferrals_are_spread_out() {
        let reg = registry(RetryBudgetPolicy::new(0.0, 1.0, 1.0));
        let now = Instant::now();
        assert_eq!(drain(&reg, A, now), 1);
        let waits: Vec<Duration> = (0..500)
            .map(|_| retry_after(reg.admit(A, true, now)))
            .collect();
        let half = MAX_RETRY_BUDGET_DEFER / 2;
        for wait in &waits {
            assert!(*wait >= MIN_RETRY_BUDGET_DEFER && *wait <= MAX_RETRY_BUDGET_DEFER);
        }
        let overflow: Vec<&Duration> = waits.iter().filter(|w| **w >= half).collect();
        let distinct: std::collections::HashSet<u128> =
            overflow.iter().map(|w| w.as_millis()).collect();
        assert!(
            distinct.len() > overflow.len() / 2,
            "overflow deferrals stack up: {} distinct of {}",
            distinct.len(),
            overflow.len()
        );
    }

    #[test]
    fn policy_constructor_rejects_bad_values() {
        let p = RetryBudgetPolicy::new(f64::NAN, 0.0, -3.0);
        assert!(p.ratio.abs() < f64::EPSILON);
        assert!((p.max_tokens - 1.0).abs() < f64::EPSILON);
        assert!(p.min_retries_per_sec.abs() < f64::EPSILON);
    }

    #[test]
    fn config_sanitizes_a_policy_built_from_public_fields() {
        let raw = RetryBudgetPolicy {
            ratio: f64::INFINITY,
            max_tokens: f64::NAN,
            min_retries_per_sec: f64::NAN,
        };
        let config = RetryBudgetConfig::disabled().with_default(Some(raw));
        let p = config.policy_for(A).expect("policy");
        assert!(p.ratio.abs() < f64::EPSILON);
        assert!((p.max_tokens - 1.0).abs() < f64::EPSILON);
        assert!(p.min_retries_per_sec.abs() < f64::EPSILON);
    }
}
