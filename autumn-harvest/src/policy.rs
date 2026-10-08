//! Retry policies, trigger rules, and scheduling types.

use std::time::Duration;

use chrono::{DateTime, Utc};
use croner::Cron;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Compute the next retry delay using exponential backoff.
///
/// `attempt` is 1-based (attempt 1 = first retry, gets `initial`).
#[must_use]
pub fn compute_retry_delay(
    initial: Duration,
    backoff_coefficient: f64,
    max_interval: Duration,
    attempt: u32,
) -> Duration {
    let exp = i32::try_from(attempt.saturating_sub(1)).unwrap_or(i32::MAX);
    let secs = initial.as_secs_f64() * backoff_coefficient.powi(exp);

    // Protect against negative floats and NaN, which would cause from_secs_f64 to panic
    let clamped_secs = if secs.is_nan() || secs < 0.0 {
        0.0
    } else {
        secs
    };

    let delay = Duration::try_from_secs_f64(clamped_secs).unwrap_or(Duration::MAX);
    delay.min(max_interval)
}

/// Retry jitter strategy.
///
/// The default is [`Full`](Self::Full), so tasks that fail together do not
/// retry together (issue #1792). Use [`None`](Self::None) for exact timing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "fuzzing", derive(arbitrary::Arbitrary))]
pub enum JitterPolicy {
    /// Exact backoff, with no jitter.
    None,
    /// Uniform in `[0, base]`.
    #[default]
    Full,
    /// Uniform in `[base/2, base]`.
    Equal,
    /// Uniform in `[initial, min(prev * 3, max)]`.
    Decorrelated,
}

const fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

const fn uniform_inclusive(seed: u64, lo: u64, hi: u64) -> u64 {
    let range = hi.wrapping_sub(lo).wrapping_add(1);
    let offset = if range == 0 {
        mix64(seed)
    } else {
        mix64(seed) % range
    };
    lo.wrapping_add(offset)
}

/// Full jitter in nanoseconds: a value in `[0, hi]`.
const fn full_jitter_nanos(hi: u64, seed: u64) -> u64 {
    uniform_inclusive(seed, 0, hi)
}

/// Equal jitter in nanoseconds: a value in `[hi/2, hi]`.
///
/// For `hi <= 1` the value is `hi`.
const fn equal_jitter_nanos(hi: u64, seed: u64) -> u64 {
    if hi <= 1 {
        return hi;
    }
    uniform_inclusive(seed, hi / 2, hi)
}

/// Full jitter: a deterministic delay in `[0, base]`.
///
/// `stream_seed` and `attempt` select the value, so a replay gets the same delay.
#[must_use]
pub(crate) fn full_jitter(base: Duration, stream_seed: u64, attempt: u32) -> Duration {
    let hi = u64::try_from(base.as_nanos()).unwrap_or(u64::MAX);
    Duration::from_nanos(full_jitter_nanos(hi, stream_seed ^ u64::from(attempt)))
}

/// Equal jitter: a deterministic delay in `[base/2, base]`.
///
/// The delay is at least half of `base`. Thus a loop with no attempt cap cannot
/// become a hot loop.
#[must_use]
pub(crate) fn equal_jitter(base: Duration, stream_seed: u64, attempt: u32) -> Duration {
    let hi = u64::try_from(base.as_nanos()).unwrap_or(u64::MAX);
    Duration::from_nanos(equal_jitter_nanos(hi, stream_seed ^ u64::from(attempt)))
}

/// Compute deterministic retry delay with jitter.
#[must_use]
pub fn compute_retry_delay_with_seed(
    policy: &RetryPolicy,
    attempt: u32,
    stream_seed: u64,
) -> Duration {
    let base = compute_retry_delay(
        policy.initial_interval,
        policy.backoff_coefficient,
        policy.max_interval,
        attempt,
    );
    match policy.jitter {
        JitterPolicy::None => base,
        JitterPolicy::Full => full_jitter(base, stream_seed, attempt),
        JitterPolicy::Equal => equal_jitter(base, stream_seed, attempt),
        JitterPolicy::Decorrelated => {
            let prev = if attempt <= 1 {
                policy.initial_interval
            } else {
                compute_retry_delay(
                    policy.initial_interval,
                    policy.backoff_coefficient,
                    policy.max_interval,
                    attempt - 1,
                )
            };
            let upper = prev.saturating_mul(3).min(policy.max_interval);
            if upper <= policy.initial_interval {
                return upper;
            }
            let lo = u64::try_from(policy.initial_interval.as_nanos()).unwrap_or(u64::MAX);
            let hi = u64::try_from(upper.as_nanos()).unwrap_or(u64::MAX);
            Duration::from_nanos(uniform_inclusive(stream_seed ^ u64::from(attempt), lo, hi))
        }
    }
}

/// How an activity failure is retried.
///
/// ## Examples
///
/// ```rust
/// use std::time::Duration;
/// use autumn_harvest::policy::RetryPolicy;
///
/// let policy = RetryPolicy::exponential(3, Duration::from_secs(1));
/// assert_eq!(policy.max_attempts, 3);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "fuzzing", derive(arbitrary::Arbitrary))]
pub struct RetryPolicy {
    /// Maximum number of attempts (including the first). 1 = no retries.
    pub max_attempts: u32,
    /// Delay before the first retry.
    pub initial_interval: Duration,
    /// Multiplier applied after each retry (`1.0` = fixed delay).
    #[cfg_attr(feature = "fuzzing", arbitrary(with = crate::fuzzing::finite_f64))]
    pub backoff_coefficient: f64,
    /// Upper bound on delay between retries.
    pub max_interval: Duration,
    /// Error type names that must not be retried.
    pub non_retryable_errors: Vec<String>,
    /// Jitter strategy. Defaults to [`JitterPolicy::Full`]. A serialized policy
    /// with no `jitter` key also gets `Full`.
    #[serde(default)]
    pub jitter: JitterPolicy,
}

impl RetryPolicy {
    /// Exponential backoff: doubles each retry, capped at 5 minutes, with Full jitter.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    /// use autumn_harvest::policy::RetryPolicy;
    ///
    /// let policy = RetryPolicy::exponential(3, Duration::from_secs(1));
    /// assert_eq!(policy.backoff_coefficient, 2.0);
    /// ```
    #[must_use]
    #[allow(clippy::missing_const_for_fn)] // vec![] prevents const fn
    pub fn exponential(max_attempts: u32, initial: Duration) -> Self {
        Self {
            max_attempts,
            initial_interval: initial,
            backoff_coefficient: 2.0,
            max_interval: Duration::from_secs(300),
            non_retryable_errors: vec![],
            jitter: JitterPolicy::Full,
        }
    }

    /// Fixed delay: same interval every retry, with Full jitter.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    /// use autumn_harvest::policy::RetryPolicy;
    ///
    /// let policy = RetryPolicy::fixed(3, Duration::from_secs(5));
    /// assert_eq!(policy.backoff_coefficient, 1.0);
    /// ```
    #[must_use]
    #[allow(clippy::missing_const_for_fn)] // vec![] prevents const fn
    pub fn fixed(max_attempts: u32, interval: Duration) -> Self {
        Self {
            max_attempts,
            initial_interval: interval,
            backoff_coefficient: 1.0,
            max_interval: interval,
            non_retryable_errors: vec![],
            jitter: JitterPolicy::Full,
        }
    }

    /// Returns the delay before the given attempt, or `None` if no more retries remain.
    ///
    /// `attempt` is 1-based: 1 = first retry (after the initial failure).
    ///
    /// This uses seed `0`, so every caller gets the same jitter. Use
    /// [`next_delay_with_seed`](Self::next_delay_with_seed) to spread tasks.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    /// use autumn_harvest::policy::{JitterPolicy, RetryPolicy};
    ///
    /// let policy = RetryPolicy::exponential(3, Duration::from_secs(1))
    ///     .with_jitter(JitterPolicy::None);
    /// assert_eq!(policy.next_delay(1), Some(Duration::from_secs(1)));
    /// assert_eq!(policy.next_delay(3), None); // attempt >= max_attempts
    /// ```
    #[must_use]
    pub fn next_delay(&self, attempt: u32) -> Option<Duration> {
        if attempt >= self.max_attempts {
            return None;
        }
        Some(compute_retry_delay_with_seed(self, attempt, 0))
    }

    #[must_use]
    pub const fn with_jitter(mut self, jitter: JitterPolicy) -> Self {
        self.jitter = jitter;
        self
    }

    #[must_use]
    pub fn next_delay_with_seed(&self, attempt: u32, stream_seed: u64) -> Option<Duration> {
        if attempt >= self.max_attempts {
            return None;
        }
        Some(compute_retry_delay_with_seed(self, attempt, stream_seed))
    }

    /// Returns `true` when a failure should skip remaining retries because it
    /// matches an entry in [`non_retryable_errors`](Self::non_retryable_errors).
    ///
    /// Resolution order (per issue #227):
    /// 1. When `typed_error_type` is `Some(...)` — i.e. the payload was the
    ///    typed wire format — match it first. This is the structured class
    ///    name from `ActivityFailure`, stable across log-format changes.
    /// 2. Fall back to a full-string match on the raw error payload — the
    ///    legacy back-compat path for activities returning `Err(String)`.
    ///
    /// `typed_error_type` must be `None` for legacy `Err(String)` payloads.
    /// Passing the synthetic fallback `"Error"` would cause a pre-existing
    /// `non_retryable_errors = ["Error"]` policy to halt retries on every
    /// legacy failure, breaking the back-compat guarantee.
    #[must_use]
    pub fn is_non_retryable(&self, typed_error_type: Option<&str>, raw_error: &str) -> bool {
        self.non_retryable_errors
            .iter()
            .any(|nr| typed_error_type.is_some_and(|et| nr == et) || nr == raw_error)
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::exponential(3, Duration::from_secs(1))
    }
}

/// Resolve the effective next-retry delay for an author-supplied `retry_after` hint (issue #744).
///
/// The hint —
/// [`ActivityFailure::retry_after`](crate::failure::ActivityFailure::retry_after)
/// — is clamped to a builder-configured ceiling:
///
/// - `retry_after = None` -> `None` (no hint; the caller falls through to the
///   policy's own backoff curve for this attempt).
/// - `retry_after = Some(Duration::ZERO)` -> `None`. `Duration` cannot
///   represent a negative value, so "a value `<= 0` falls through to the
///   normal policy delay" (issue #744, AC3) reduces to exactly this case.
/// - `retry_after = Some(d)` where `d > ceiling` -> `Some(ceiling)` (clamped
///   down, never rejected).
/// - `retry_after = Some(d)` where `0 < d <= ceiling` -> `Some(d)` (honored
///   verbatim).
///
/// **`ceiling = Duration::ZERO` is *not* a "disable `retry_after` honoring"
/// switch.** For any positive hint `d`, `d.min(Duration::ZERO) ==
/// Duration::ZERO`, so this resolves to `Some(Duration::ZERO)` — an
/// IMMEDIATE retry — not `None` (fall through to the policy delay). This is
/// the opposite of what an operator setting the ceiling to zero might
/// reasonably expect. There is no separate switch to turn `retry_after`
/// honoring off entirely today; an author who never calls
/// [`ActivityFailure::with_retry_after`](crate::failure::ActivityFailure::with_retry_after)
/// is the only way to opt an activity out.
///
/// This function does **not** consult `non_retryable` or the retry policy's
/// `max_attempts` cap — callers must check those first (a non-retryable
/// failure, or an attempt count that has already exhausted the policy, must
/// never be resurrected by a delay hint).
#[must_use]
pub fn resolve_retry_after_hint(
    retry_after: Option<Duration>,
    ceiling: Duration,
) -> Option<Duration> {
    let hint = retry_after?;
    if hint.is_zero() {
        return None;
    }
    Some(hint.min(ceiling))
}

/// Per-activity circuit-breaker configuration (issue #369).
///
/// When attached to an activity (via the `#[activity(circuit_breaker = ...)]`
/// attribute or builder registration), the worker tracks consecutive failures
/// of that activity within a rolling window. Once `failure_threshold` failures
/// accumulate inside `window`, the breaker **trips open**. `open_mode` then
/// decides what happens to each later dispatch:
///
/// - [`CircuitOpenMode::Defer`] (default): the task goes back to `PENDING`
///   until the next probe. It uses no attempt and appends no event.
/// - [`CircuitOpenMode::FailFast`]: the attempt fails with a non-retryable
///   [`ActivityFailure`](crate::failure::ActivityFailure) of error type
///   `"CircuitOpen"`.
///
/// After `cooldown` elapses the breaker moves to half-open and
/// admits a single probe; success re-closes it, failure re-opens it.
///
/// Circuit state is tracked in-process and per-shard — it never touches the
/// workflow event log, so the append-only contract is unchanged and replay is
/// unaffected. A deferral appends no event. A fail-fast short circuit
/// records an ordinary `ActivityFailed` event.
///
/// ## Examples
///
/// ```rust
/// use std::time::Duration;
/// use autumn_harvest::policy::{CircuitBreakerPolicy, CircuitOpenMode};
///
/// // Trip after 10 failures within 30s; re-probe after 60s.
/// let policy = CircuitBreakerPolicy::new(10, Duration::from_secs(30), Duration::from_secs(60));
/// assert_eq!(policy.failure_threshold, 10);
/// assert_eq!(policy.open_mode, CircuitOpenMode::Defer);
///
/// // A Saga that compensates on `CircuitOpen` needs the fast failure.
/// let fail_fast = policy.with_open_mode(CircuitOpenMode::FailFast);
/// assert_eq!(fail_fast.open_mode, CircuitOpenMode::FailFast);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CircuitBreakerPolicy {
    /// Number of failures within `window` that trips the breaker open.
    /// Must be `>= 1`.
    pub failure_threshold: u32,
    /// Rolling time window over which failures are counted.
    pub window: Duration,
    /// Cooldown after the breaker opens before a single half-open probe is
    /// admitted.
    pub cooldown: Duration,
    /// What a dispatch does while the breaker is open (issue #1809).
    /// A policy serialized before this field existed reads as the default.
    #[serde(default)]
    pub open_mode: CircuitOpenMode,
}

/// What a dispatch does while its circuit breaker is open (issue #1809).
///
/// `docs/adr/0005-activity-timeout-retry-and-open-circuit.md` records why
/// [`Defer`](Self::Defer) is the default.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CircuitOpenMode {
    /// Put the claimed task back to `PENDING` until the next probe. The
    /// deferral uses no attempt and appends no event.
    #[default]
    Defer,
    /// Fail the attempt with a non-retryable `CircuitOpen` failure. Use it
    /// when a workflow must react to the outage at once, for example with a
    /// Saga compensation.
    FailFast,
}

impl CircuitBreakerPolicy {
    /// Construct a circuit-breaker policy.
    ///
    /// `failure_threshold` is clamped to a minimum of 1 — a threshold of 0
    /// would trip on the very first dispatch before any failure is observed,
    /// which is never the intended behaviour.
    #[must_use]
    pub fn new(failure_threshold: u32, window: Duration, cooldown: Duration) -> Self {
        Self {
            failure_threshold: failure_threshold.max(1),
            window,
            cooldown,
            open_mode: CircuitOpenMode::Defer,
        }
    }

    /// Set what a dispatch does while the breaker is open (issue #1809).
    #[must_use]
    pub const fn with_open_mode(mut self, open_mode: CircuitOpenMode) -> Self {
        self.open_mode = open_mode;
        self
    }
}

/// Retry budget for one activity type (issue #1793).
///
/// The worker keeps one token bucket for each activity type. A first attempt
/// deposits `ratio` tokens. A retry spends one token. Time adds
/// `min_retries_per_sec` tokens each second. The bucket holds at most
/// `max_tokens` and starts full.
///
/// An empty bucket defers the retry. It does not drop it. See
/// [`crate::retry_budget`] for the full semantics.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::policy::RetryBudgetPolicy;
///
/// // Let retries add at most 20 % load, with a floor of 2 retries per second.
/// let policy = RetryBudgetPolicy::new(0.2, 20.0, 2.0);
/// assert_eq!(policy.ratio, 0.2);
/// assert_eq!(RetryBudgetPolicy::default().ratio, 0.1);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct RetryBudgetPolicy {
    /// Tokens that each first attempt deposits.
    pub ratio: f64,
    /// Bucket capacity and start level. At least 1.
    pub max_tokens: f64,
    /// Tokens that time adds each second. A value above 0 keeps retries
    /// from starving. With 0, only first-attempt deposits refill the bucket.
    pub min_retries_per_sec: f64,
}

impl RetryBudgetPolicy {
    /// Default deposit for each first attempt: a 10 % retry budget.
    pub const DEFAULT_RATIO: f64 = 0.1;
    /// Default bucket capacity.
    pub const DEFAULT_MAX_TOKENS: f64 = 10.0;
    /// Default time refill, in tokens each second.
    pub const DEFAULT_MIN_RETRIES_PER_SEC: f64 = 1.0;

    /// Construct a retry budget policy.
    ///
    /// A negative or non-finite `ratio` or `min_retries_per_sec` becomes 0.
    /// `max_tokens` becomes at least 1, so one retry can always run.
    #[must_use]
    pub fn new(ratio: f64, max_tokens: f64, min_retries_per_sec: f64) -> Self {
        let rate = |v: f64| if v.is_finite() && v > 0.0 { v } else { 0.0 };
        let cap = if max_tokens.is_finite() {
            max_tokens.max(1.0)
        } else {
            1.0
        };
        Self {
            ratio: rate(ratio),
            max_tokens: cap,
            min_retries_per_sec: rate(min_retries_per_sec),
        }
    }
}

impl Default for RetryBudgetPolicy {
    fn default() -> Self {
        Self::new(
            Self::DEFAULT_RATIO,
            Self::DEFAULT_MAX_TOKENS,
            Self::DEFAULT_MIN_RETRIES_PER_SEC,
        )
    }
}

/// Adaptive concurrency limit for one activity type (issue #1836).
///
/// The worker caps the in-flight attempts of the type. The cap follows the
/// handler latency and the retryable failures. See [`crate::adaptive_limit`]
/// for the rules.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::policy::AdaptiveLimitPolicy;
///
/// // Let the cap move between 2 and 64 in-flight attempts.
/// let policy = AdaptiveLimitPolicy::new(2, 64);
/// assert_eq!(policy.max_limit, 64);
/// assert_eq!(AdaptiveLimitPolicy::default().min_limit, 1);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct AdaptiveLimitPolicy {
    /// Lowest cap. At least 1.
    pub min_limit: u32,
    /// Highest cap. At least `min_limit`.
    pub max_limit: u32,
    /// Latency inflation over the no-load baseline that the limit accepts.
    /// At 1.25, the gradient stays at 1 until latency is 25 % above the
    /// baseline. Above that, the cap grows more slowly and settles. At
    /// least 1.
    pub tolerance: f64,
    /// Factor that an overloaded window applies to the cap. It is in the
    /// range from 0.5 to 1.
    pub backoff_ratio: f64,
    /// Share of retryable failures above which a window is overloaded. It
    /// is in the range from 0 to 1. At 0, one failure cuts the cap. A higher
    /// value keeps rare failures from throttling a healthy dependency.
    pub error_threshold: f64,
    /// Samples between two baseline probes. At least
    /// [`AdaptiveLimitPolicy::MIN_PROBE_INTERVAL`].
    pub probe_interval: u32,
}

impl AdaptiveLimitPolicy {
    /// Default lowest cap.
    pub const DEFAULT_MIN_LIMIT: u32 = 1;
    /// Default highest cap.
    pub const DEFAULT_MAX_LIMIT: u32 = 200;
    /// Default latency tolerance.
    pub const DEFAULT_TOLERANCE: f64 = 1.25;
    /// Default backoff factor for an overloaded window.
    pub const DEFAULT_BACKOFF_RATIO: f64 = 0.9;
    /// Default failure share above which a window is overloaded.
    pub const DEFAULT_ERROR_THRESHOLD: f64 = 0.05;
    /// Default samples between two baseline probes.
    pub const DEFAULT_PROBE_INTERVAL: u32 = 1_000;
    /// Fewest samples between two baseline probes.
    pub const MIN_PROBE_INTERVAL: u32 = 10;

    /// Construct a policy with the given cap range and default tuning.
    #[must_use]
    pub fn new(min_limit: u32, max_limit: u32) -> Self {
        Self {
            min_limit,
            max_limit,
            ..Self::default()
        }
        .sanitized()
    }

    /// Apply the field rules. The fields are public, so a caller can set a
    /// NaN or an inverted range directly.
    ///
    /// `min_limit` becomes at least 1, and `max_limit` at least `min_limit`.
    /// A non-finite or low `tolerance` becomes 1. A `backoff_ratio` outside
    /// the range from 0.5 to 1 is clamped, and NaN becomes the default.
    /// `error_threshold` follows the same rule in the range from 0 to 1.
    #[must_use]
    pub fn sanitized(self) -> Self {
        let min_limit = self.min_limit.max(1);
        let tolerance = if self.tolerance.is_finite() {
            self.tolerance.max(1.0)
        } else {
            1.0
        };
        let backoff_ratio = if self.backoff_ratio.is_nan() {
            Self::DEFAULT_BACKOFF_RATIO
        } else {
            self.backoff_ratio.clamp(0.5, 1.0)
        };
        let error_threshold = if self.error_threshold.is_nan() {
            Self::DEFAULT_ERROR_THRESHOLD
        } else {
            self.error_threshold.clamp(0.0, 1.0)
        };
        Self {
            min_limit,
            max_limit: self.max_limit.max(min_limit),
            tolerance,
            backoff_ratio,
            error_threshold,
            probe_interval: self.probe_interval.max(Self::MIN_PROBE_INTERVAL),
        }
    }
}

impl Default for AdaptiveLimitPolicy {
    fn default() -> Self {
        Self {
            min_limit: Self::DEFAULT_MIN_LIMIT,
            max_limit: Self::DEFAULT_MAX_LIMIT,
            tolerance: Self::DEFAULT_TOLERANCE,
            backoff_ratio: Self::DEFAULT_BACKOFF_RATIO,
            error_threshold: Self::DEFAULT_ERROR_THRESHOLD,
            probe_interval: Self::DEFAULT_PROBE_INTERVAL,
        }
    }
}

/// Failure semantics for mapped nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum MapFailurePolicy {
    /// The first instance failure fails the mapped node; downstream levels are
    /// never dispatched.
    ///
    /// The failing instance's outcome wins even when later instances succeed.
    /// Sibling instances already dispatched are **not** durably cancelled — the
    /// node settles every instance before the DAG terminates, so the recorded
    /// history stays replay-clean (issue #780).
    #[default]
    FailFast,
    /// The collect node receives per-slot success/failure, making partial batch failures observable.
    CollectAll,
}

/// Status of a completed DAG task, used by trigger rules.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::policy::TaskStatus;
///
/// let status = TaskStatus::Succeeded;
/// assert_eq!(status, TaskStatus::Succeeded);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    /// The task executed and returned success.
    Succeeded,
    /// The task returned an error or exhausted its retries.
    Failed,
    /// The task was skipped (e.g., due to a trigger rule evaluating to false).
    Skipped,
}

/// When a DAG task with multiple upstreams should execute.
///
/// With no upstreams (a root node), `AllSuccess` and `AllDone` fire.
/// `OneSuccess`, `OneFailed`, `AllFailed` and `Manual` do not fire, so a root
/// node with one of those rules is always skipped. The skip writes no
/// history marker.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::policy::TriggerRule;
///
/// let rule = TriggerRule::AllSuccess;
/// assert_eq!(rule, TriggerRule::default());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum TriggerRule {
    /// Run when all upstream tasks succeeded (default).
    #[default]
    AllSuccess,
    /// Run when all upstream tasks completed (any terminal state).
    AllDone,
    /// Run when at least one upstream succeeded.
    OneSuccess,
    /// Run when at least one upstream failed.
    OneFailed,
    /// Run when all upstream tasks failed.
    AllFailed,
    /// Never fires. No API triggers a single DAG node, so a `Manual` node is
    /// always skipped, and so is every node that needs it to succeed.
    Manual,
}

impl TriggerRule {
    /// Evaluates the trigger rule against a list (or iterator) of upstream task statuses.
    ///
    /// Returns `true` if the downstream task should be executed, `false` otherwise.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::policy::{TriggerRule, TaskStatus};
    ///
    /// let rule = TriggerRule::AllSuccess;
    /// let statuses = vec![TaskStatus::Succeeded, TaskStatus::Succeeded];
    /// assert!(rule.should_run(&statuses));
    /// ```
    #[must_use]
    pub fn should_run<'a>(
        &self,
        upstream_statuses: impl IntoIterator<Item = &'a TaskStatus>,
    ) -> bool {
        match self {
            Self::AllSuccess => upstream_statuses
                .into_iter()
                .all(|s| *s == TaskStatus::Succeeded),
            Self::AllDone => true,
            Self::OneSuccess => upstream_statuses
                .into_iter()
                .any(|s| *s == TaskStatus::Succeeded),
            Self::OneFailed => upstream_statuses
                .into_iter()
                .any(|s| *s == TaskStatus::Failed),
            Self::AllFailed => {
                let mut iter = upstream_statuses.into_iter().peekable();
                iter.peek().is_some() && iter.all(|s| *s == TaskStatus::Failed)
            }
            Self::Manual => false,
        }
    }
}

/// DAG/workflow execution schedule.
///
/// ## Examples
///
/// ```rust
/// use std::time::Duration;
/// use autumn_harvest::policy::Schedule;
///
/// let sched = Schedule::Interval(Duration::from_secs(60));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Schedule {
    /// Standard cron expression (e.g., `"0 2 * * *"` for daily at 2 AM).
    ///
    /// The cron expression is evaluated in **UTC**. This variant is the
    /// backward-compatible default; existing schedules continue to fire in
    /// UTC on upgrade.
    Cron(String),
    /// Fixed interval from the end of the previous run.
    ///
    /// The period must be a whole number of seconds greater than zero.
    Interval(Duration),
    /// Only runs when triggered manually via API.
    Manual,
    /// Timezone-aware cron expression.
    ///
    /// Identical to `Cron` but the expression is evaluated in the declared
    /// IANA timezone rather than UTC, so `"0 9 * * 1-5"` in
    /// `"America/Los_Angeles"` fires at **9:00 AM Pacific year-round**,
    /// regardless of DST transitions.
    ///
    /// ## DST disambiguation
    ///
    /// - **Spring-forward (clocks skip an hour):** a cron expression whose
    ///   local-time resolution falls inside the skipped window is **not**
    ///   back-fired. The scheduler advances to the first valid local instant
    ///   on the following day (or the next matching wall-clock time after the
    ///   gap). No spurious firing occurs at the wrong UTC time.
    /// - **Fall-back (clocks repeat an hour):** a cron expression whose
    ///   local-time resolution falls inside the repeated window fires exactly
    ///   **once** — on the first occurrence of that local time (the pre-rollback
    ///   instant). The repeated hour does not trigger a second firing.
    ///
    /// ## Validation
    ///
    /// `tz` must be a valid IANA timezone name (e.g. `"America/Los_Angeles"`,
    /// `"Europe/London"`, `"Asia/Tokyo"`, `"UTC"`). Unknown names are rejected
    /// at builder/registration time with
    /// [`HarvestBuilderError::UnknownTimezone`](crate::builder::HarvestBuilderError::UnknownTimezone),
    /// not at first scheduler tick.
    ///
    /// ## Backward compatibility
    ///
    /// `Schedule::Cron(expr)` schedules retain UTC semantics on upgrade; the
    /// new variant is strictly opt-in. Existing persisted schedules are
    /// unaffected.
    CronInTimezone {
        /// The cron expression (same syntax as `Schedule::Cron`).
        expr: String,
        /// IANA timezone name (e.g. `"America/Los_Angeles"`).
        tz: String,
    },
}

impl Schedule {
    /// Returns the IANA timezone name for this schedule.
    ///
    /// `Cron`, `Interval`, and `Manual` return `"UTC"` (backward-compatible
    /// default). `CronInTimezone` returns its declared timezone.
    #[must_use]
    pub const fn timezone_str(&self) -> &str {
        match self {
            Self::CronInTimezone { tz, .. } => tz.as_str(),
            Self::Cron(_) | Self::Interval(_) | Self::Manual => "UTC",
        }
    }
}

/// What happens when a new schedule firing collides with a still-running previous
/// run from the same schedule.
///
/// ## Decision matrix
///
/// | Policy | When to use | In-flight run | New firing | Subsequent firings while busy | Durability |
/// |---|---|---|---|---|---|
/// | `Skip` | Default; predictable load, idempotent schedules | Continues | Dropped | Each evaluated at next tick | N/A |
/// | `BufferOne` | Long-running jobs that must catch up by exactly one slot | Continues | Queued (one slot) | Dropped while slot occupied | Durable in DB |
/// | `BufferAll` | Backfill/replay; every missed slot must eventually run | Continues | Queued (up to `buffer_all_max`) | Dropped past cap | Durable in DB |
/// | `CancelOther` | Wedged runs; always prefer the latest firing | Cancelled gracefully | Started immediately | Normal | N/A |
/// | `TerminateOther` | Same as `CancelOther` but with immediate force-stop | Terminated immediately | Started immediately | Normal | N/A |
/// | `AllowAll` | Independent runs that may overlap (issue #1985) | Continues | Started immediately | Each started | N/A |
///
/// The default is [`Skip`](OverlapPolicy::Skip), which preserves pre-existing behaviour.
///
/// `BufferOne` / `BufferAll` store pending firings durably in `harvest_schedules`
/// so they survive scheduler restarts and leader handoffs.
///
/// `CancelOther` / `TerminateOther` require the cancellation contract from
/// issue #238, which is implemented in this codebase.
///
/// `AllowAll` ignores `max_active_runs`, as Temporal does. One tick starts at
/// most `scheduler::ALLOW_ALL_MAX_STARTS_PER_TICK` runs. Per-workflow
/// concurrency limits, throttles and admission gates still apply. A throttle
/// slower than the schedule cadence lets its pending backlog grow. Runs can
/// overlap, so `last_completion_result` carryover can be stale.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OverlapPolicy {
    /// Drop the new firing when the previous run is still active (default).
    #[default]
    Skip,
    /// Buffer at most one pending firing; drop subsequent firings while the
    /// buffer slot is occupied (records `reason = "buffered_slot_full"`).
    BufferOne,
    /// Buffer every missed firing up to `buffer_all_max`; drop firings past
    /// the cap (records `reason = "buffer_full"`).
    BufferAll,
    /// Cancel the in-flight run and start the new one.
    CancelOther,
    /// Terminate the in-flight run immediately and start the new one.
    TerminateOther,
    /// Start the new run and keep the in-flight runs (issue #1985).
    AllowAll,
}

impl OverlapPolicy {
    /// Every accepted value, in declaration order. Error messages and API
    /// docs quote this list.
    pub const VALID_VALUES: &'static str =
        "skip, buffer_one, buffer_all, cancel_other, terminate_other, allow_all";

    /// The `snake_case` string used to store this policy in `harvest_schedules`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Skip => "skip",
            Self::BufferOne => "buffer_one",
            Self::BufferAll => "buffer_all",
            Self::CancelOther => "cancel_other",
            Self::TerminateOther => "terminate_other",
            Self::AllowAll => "allow_all",
        }
    }

    /// Parse an `overlap_policy` column value from the database.
    ///
    /// Unknown values fall back to [`Skip`](Self::Skip) to preserve the
    /// append-only-schema invariant: a deployment using an older binary
    /// reading a newer enum value degrades to the safe default.
    #[must_use]
    pub fn from_db(s: &str) -> Self {
        match s {
            "buffer_one" => Self::BufferOne,
            "buffer_all" => Self::BufferAll,
            "cancel_other" => Self::CancelOther,
            "terminate_other" => Self::TerminateOther,
            "allow_all" => Self::AllowAll,
            _ => Self::Skip,
        }
    }

    /// Parse an `overlap_policy` value from user-supplied input (e.g. an API request).
    ///
    /// Unlike [`from_db`](Self::from_db) this is strict: an unknown value returns
    /// `Err` so callers can surface a 400 response rather than silently applying
    /// the `Skip` fallback.
    ///
    /// # Errors
    ///
    /// Returns `Err(s)` when `s` is not a recognised variant name.
    pub fn from_user_input(s: &str) -> Result<Self, &str> {
        match s {
            "skip" => Ok(Self::Skip),
            "buffer_one" => Ok(Self::BufferOne),
            "buffer_all" => Ok(Self::BufferAll),
            "cancel_other" => Ok(Self::CancelOther),
            "terminate_other" => Ok(Self::TerminateOther),
            "allow_all" => Ok(Self::AllowAll),
            _ => Err(s),
        }
    }
}

/// What the scheduler does when a fire date falls on a calendar-excluded day.
///
/// Calendars are sets of dates (e.g. federal holidays) that a schedule should
/// avoid. `SkipPolicy` declares the fallback when the natural fire date is one
/// of those excluded days.
///
/// See [`crate::calendar`] for the [`crate::calendar::apply_skip_policy`] implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkipPolicy {
    /// Suppress the firing entirely (increment `harvest.schedule.skipped`).
    #[default]
    Skip,
    /// Defer to the first subsequent non-excluded day.
    RunNextBusinessDay,
    /// Advance to the most recent preceding non-excluded day.
    RunPrevBusinessDay,
}

impl SkipPolicy {
    /// The `snake_case` string used to store this policy in `harvest_schedules`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Skip => "skip",
            Self::RunNextBusinessDay => "run_next_business_day",
            Self::RunPrevBusinessDay => "run_prev_business_day",
        }
    }

    /// Parse a `skip_policy` column value from the database.
    ///
    /// Unknown values fall back to [`Skip`](Self::Skip) to preserve the
    /// append-only-schema invariant.
    #[must_use]
    pub fn from_db(s: &str) -> Self {
        match s {
            "run_next_business_day" => Self::RunNextBusinessDay,
            "run_prev_business_day" => Self::RunPrevBusinessDay,
            _ => Self::Skip,
        }
    }

    /// Parse a `skip_policy` value from user-supplied input (e.g. an API request).
    ///
    /// # Errors
    ///
    /// Returns `Err(s)` when `s` is not a recognised variant name.
    pub fn from_user_input(s: &str) -> Result<Self, &str> {
        match s {
            "skip" => Ok(Self::Skip),
            "run_next_business_day" => Ok(Self::RunNextBusinessDay),
            "run_prev_business_day" => Ok(Self::RunPrevBusinessDay),
            _ => Err(s),
        }
    }
}

/// How the scheduler handles missed fire slots after scheduler downtime.
///
/// This is a three-mode replacement for the binary `catchup: bool` flag.
/// When a `CatchupPolicy` is set on a [`WorkflowSchedule`] it takes precedence
/// over the `catchup` bool; unset schedules fall back to the bool semantics
/// unchanged.
///
/// ## Variants
///
/// | Policy | Fires | Drops |
/// |---|---|---|
/// | `SkipAll` | 1 (the oldest overdue slot) | all others silently counted |
/// | `MostRecent` | 1 (the **newest** missed slot) | all older slots counted |
/// | `Window(d)` | all slots whose scheduled time ≥ now − d | all older slots counted |
/// | `Unbounded` | **all** missed slots | none |
///
/// Every dropped slot increments `harvest.schedule.skipped` with reason
/// `catchup_window_exceeded`.  `SkipAll` never records a drop because it reuses
/// the existing single-slot path with no history of what was skipped; use
/// `MostRecent` when you want an audit trail.
///
/// ## Examples
///
/// ```rust
/// use std::time::Duration;
/// use autumn_harvest::policy::{CatchupPolicy, Schedule, WorkflowSchedule};
///
/// // Fire only the most recent missed slot (recommended for most schedules).
/// let sched = WorkflowSchedule::new("billing", Schedule::Cron("*/15 * * * *".to_string()))
///     .with_catchup_policy(CatchupPolicy::MostRecent);
///
/// // Fire all slots that fell within the last 2 hours.
/// let sched2 = WorkflowSchedule::new("import", Schedule::Cron("0 * * * *".to_string()))
///     .with_catchup_window(Duration::from_secs(2 * 3600));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatchupPolicy {
    /// Drop all missed slots after the first (the oldest overdue slot fires).
    ///
    /// Identical to `catchup = false` today.  No drops are recorded because
    /// the existing binary path doesn't enumerate them.
    SkipAll,
    /// Fire exactly the **most recent** missed slot; count all others as
    /// dropped with reason `catchup_window_exceeded`.
    MostRecent,
    /// Fire all missed slots whose scheduled time is `≥ now − window`;
    /// older slots are counted as dropped with reason `catchup_window_exceeded`.
    Window(Duration),
    /// Fire every missed slot without limit.
    ///
    /// Identical to `catchup = true` today.  Use when you want the original
    /// thunder-herd behaviour or have a bounded outage window already.
    Unbounded,
}

impl CatchupPolicy {
    /// The `snake_case` mode string used in `harvest_schedules.catchup_policy`.
    ///
    /// `Window(d)` serialises to `"window"`; the duration is stored separately
    /// in `catchup_window_secs`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SkipAll => "skip_all",
            Self::MostRecent => "most_recent",
            Self::Window(_) => "window",
            Self::Unbounded => "unbounded",
        }
    }

    /// Resolve the effective policy from the database columns.
    ///
    /// `mode` is the `catchup_policy` column value (NULL when the row was
    /// written by an older binary).  `window_secs` is `catchup_window_secs`.
    /// `catchup_bool` is the legacy `catchup` column used as a fallback when
    /// `mode` is NULL or unrecognised.
    ///
    /// Unknown mode strings degrade to the bool fallback so a deploy using an
    /// older binary reading a row written by a newer one never panics.
    #[must_use]
    pub fn from_db(mode: Option<&str>, window_secs: Option<i64>, catchup_bool: bool) -> Self {
        match mode {
            Some("skip_all") => Self::SkipAll,
            Some("most_recent") => Self::MostRecent,
            Some("window") => {
                let secs = u64::try_from(window_secs.unwrap_or(0).max(0)).unwrap_or(0);
                Self::Window(Duration::from_secs(secs))
            }
            Some("unbounded") => Self::Unbounded,
            // NULL or unrecognised: fall back to the legacy bool.
            _ => {
                if catchup_bool {
                    Self::Unbounded
                } else {
                    Self::SkipAll
                }
            }
        }
    }

    /// The DB column value pair `(catchup_policy, catchup_window_secs)` for
    /// this policy. `None` values are written as SQL NULL.
    #[must_use]
    pub fn to_db_columns(self) -> (Option<&'static str>, Option<i64>) {
        match self {
            Self::SkipAll => (Some("skip_all"), None),
            Self::MostRecent => (Some("most_recent"), None),
            Self::Window(d) => (
                Some("window"),
                Some(i64::try_from(d.as_secs()).unwrap_or(i64::MAX)),
            ),
            Self::Unbounded => (Some("unbounded"), None),
        }
    }

    /// `true` when this policy requires enumeration of every missed slot
    /// (needed for calendar-advance and buffer-advance branch decisions in the
    /// scheduler tick loop).
    #[must_use]
    pub const fn is_catchup_enabled(self) -> bool {
        !matches!(self, Self::SkipAll)
    }

    /// Strictly parse an operator-supplied catchup policy from API input.
    ///
    /// Unlike [`Self::from_db`] (which is lenient for backward compatibility and
    /// degrades unknown modes to the legacy `catchup` bool), unknown modes are
    /// rejected so bad API input surfaces as `400` rather than silently picking a
    /// fallback. `"window"` reads its duration from `window_secs` (defaulting to
    /// `0`, which fires only the slot at exactly `now`).
    ///
    /// # Errors
    ///
    /// Returns `Err(mode)` when `mode` is not a recognised variant name.
    pub fn from_user_input(mode: &str, window_secs: Option<i64>) -> Result<Self, &str> {
        match mode {
            "skip_all" => Ok(Self::SkipAll),
            "most_recent" => Ok(Self::MostRecent),
            "unbounded" => Ok(Self::Unbounded),
            "window" => {
                let secs = u64::try_from(window_secs.unwrap_or(0).max(0)).unwrap_or(0);
                Ok(Self::Window(Duration::from_secs(secs)))
            }
            other => Err(other),
        }
    }
}

/// Per-workflow cron/interval schedule — the lightweight alternative to a
/// single-node DAG when all you need is "run this workflow on a schedule."
///
/// Register via [`crate::builder::HarvestBuilder::workflow_schedule`].
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::policy::{Schedule, WorkflowSchedule};
///
/// let sched = WorkflowSchedule::new("daily_billing_report", Schedule::Cron("0 3 * * *".to_string()));
/// assert_eq!(sched.max_active_runs, 1);
/// assert!(!sched.catchup);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowSchedule {
    /// The registered workflow name to start on each firing.
    pub workflow_name: String,
    /// When this schedule was promoted from a `#[dag]` definition, the
    /// original DAG name is stored here so the DAG management API can still
    /// list, pause, and resume the schedule via `GET /dags` and
    /// `PATCH /dags/{name}`.  `None` for pure workflow schedules.
    pub dag_name: Option<String>,
    /// Cron or interval schedule. `Schedule::Manual` is accepted but will
    /// never fire automatically — use the API to trigger it instead.
    pub schedule: Schedule,
    /// Input JSON passed to every scheduled run.
    ///
    /// For multi-parameter workflows use the `[arg1, arg2, ...]` array form.
    /// Defaults to `Value::Null`.
    pub input: serde_json::Value,
    /// Whether to back-fill missed runs when the scheduler was down.
    /// Defaults to `false`.
    pub catchup: bool,
    /// Maximum number of concurrently running scheduled executions for this
    /// workflow. Enforced cluster-wide against non-terminal
    /// `harvest_workflow_executions` rows.
    ///
    /// Defaults to `1`.
    pub max_active_runs: u32,
    /// Initial paused state. Defaults to `false`.
    pub paused: bool,
    /// Task queue name for dispatched runs. Defaults to `"default"`.
    pub queue_name: String,
    /// Maximum spread window for staggering schedule fires.
    ///
    /// The actual fire time is shifted forward by a deterministic offset in
    /// `[0, jitter)` derived from `(schedule_id, scheduled_fire_time)`.
    /// [`WorkflowSchedule::new`] sets [`default_schedule_jitter`]:
    /// [`DEFAULT_CRON_JITTER`] for a cron with no seconds field, else zero. Set
    /// `Duration::ZERO` to opt out. A deserialized schedule with no `jitter` key
    /// gets zero.
    ///
    /// ## Example
    ///
    /// ```rust
    /// use std::time::Duration;
    /// use autumn_harvest::policy::{Schedule, WorkflowSchedule};
    ///
    /// // Spread 100 hourly schedules over the first 5 minutes of every hour.
    /// let sched = WorkflowSchedule::new(
    ///     "nightly_report",
    ///     Schedule::Cron("0 * * * *".to_string()),
    /// )
    /// .with_jitter(Duration::from_secs(300));
    /// ```
    #[serde(default)]
    pub jitter: Duration,
    /// What to do when a new firing collides with a still-running execution
    /// from the same schedule. Defaults to [`OverlapPolicy::Skip`].
    #[serde(default)]
    pub overlap_policy: OverlapPolicy,
    /// Maximum number of pending firings stored under [`OverlapPolicy::BufferAll`].
    /// Past this cap, additional firings are dropped and recorded as skipped with
    /// `reason = "buffer_full"`. Defaults to `100`.
    #[serde(default = "default_buffer_all_max")]
    pub buffer_all_max: u32,
    /// Per-run execution timeout propagated to every workflow started by this
    /// schedule. `None` = no deadline enforced (today's behaviour).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_timeout: Option<std::time::Duration>,
    /// Chain-scoped lifetime cap for this schedule (issue #617). Distinct from
    /// [`execution_timeout`](Self::execution_timeout): anchored at the first run's
    /// start and carried verbatim across every continue-as-new.
    ///
    /// **Currently inert** — at exact parity with [`execution_timeout`](Self::execution_timeout),
    /// this per-schedule value is NOT persisted on `harvest_schedules` and NOT
    /// read by the scheduler tick. The *functional* schedule-level chain default
    /// is delivered by the workflow-type `#[workflow(chain_execution_timeout = "…")]`
    /// attribute (inherited through `WorkflowInfo` at tick time), plus the
    /// fleet-wide ceiling ([`HarvestBuilder::max_workflow_chain_timeout`]). The
    /// builder method is kept for API symmetry with #243's third surface.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain_execution_timeout: Option<std::time::Duration>,
    /// Optional named calendar to consult before each firing.
    ///
    /// When `Some("us-federal-holidays")`, the scheduler looks up the
    /// `harvest_calendars` row with that name and applies [`skip_policy`](Self::skip_policy)
    /// on fire dates that fall on an excluded day. `None` = today's behaviour
    /// (no calendar filtering).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub calendar: Option<String>,
    /// What to do when the fire date falls on a calendar-excluded day.
    ///
    /// Ignored when [`calendar`](Self::calendar) is `None`.
    /// Default: [`SkipPolicy::Skip`] (suppress the firing).
    #[serde(default)]
    pub skip_policy: SkipPolicy,
    /// Auto-pause after this many consecutive `FAILED`/`TIMED_OUT` execution completions.
    ///
    /// `None` (the default) disables auto-pause — existing schedules are unaffected.
    /// When set, the scheduler pauses the schedule automatically and emits the
    /// `harvest.schedule.auto_paused` metric. Resume via the management API to restart.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consecutive_failure_limit: Option<u32>,
    /// Absolute UTC cutoff for this schedule (issue #478).
    ///
    /// When the due fire time is `>= end_at`, the scheduler does **not** start a run and
    /// transitions the schedule to the terminal exhausted state.  `None` (the default) means
    /// fire forever — today's behaviour, fully backward-compatible.
    ///
    /// Composes with all existing knobs.  A firing suppressed by `OverlapPolicy::Skip`,
    /// a calendar `skip_policy`, or `paused` does **not** consume the `max_runs` budget
    /// and does **not** trigger `end_at` exhaustion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_at: Option<DateTime<Utc>>,
    /// Total run budget for this schedule (issue #478).
    ///
    /// Decremented atomically at the moment a run is actually started (not merely
    /// scheduled).  When the budget reaches zero the schedule transitions to the terminal
    /// exhausted state.  `None` (the default) means no budget — fire forever.
    ///
    /// Only actually-started runs consume the budget.  Firings suppressed by
    /// `OverlapPolicy::Skip`, a calendar `skip_policy`, or `paused` do **not** consume a
    /// slot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_runs: Option<u32>,
    /// Bounded catchup policy for missed fire slots after scheduler downtime (issue #484).
    ///
    /// When `Some`, this takes precedence over the `catchup` bool and selects
    /// one of three modes: `MostRecent` (fire one, drop the rest), `Window(d)`
    /// (fire slots within the last `d`), or `Unbounded` (fire all — same as
    /// `catchup = true`).  `SkipAll` is equivalent to `catchup = false`.
    ///
    /// `None` (the default) preserves the existing `catchup` bool behaviour with
    /// no behavior change for unmodified schedules.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catchup_policy: Option<CatchupPolicy>,
    /// Default retry policy for runs started by this schedule (issue #523).
    ///
    /// When `Some`, each run started by this schedule uses this as its retry policy
    /// (unless the workflow type declares its own default or the start API provides
    /// a per-start override). `None` (the default) disables schedule-level retry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_policy: Option<RetryPolicy>,
    /// Register this schedule on **every writable shard** rather than the single
    /// shard chosen by rendezvous hashing / the default shard (issue #796).
    ///
    /// `false` (the default) preserves today's behaviour exactly: a non-DAG
    /// schedule is pinned to `router.default_shard()` and a DAG schedule to its
    /// rendezvous shard. When `true`, the scheduler upserts one
    /// `harvest_schedules` row per writable shard (one row per shard database,
    /// so the `UNIQUE(workflow_name)` constraint is not violated) and each
    /// shard's fire mints an execution on that shard.
    ///
    /// **Supported only for DAG schedules and the built-in synthetic liveness
    /// canary** (names starting with `__harvest_canary_probe`, issue #796, AC4).
    /// The canary sets this so a single write-blocked/dead shard surfaces as a
    /// failing/stale probe for that shard.
    ///
    /// For any other (plain, non-DAG) workflow this flag is **rejected at build
    /// time** (`HarvestBuilderError::AllWritableShardsUnsupported`). The reason:
    /// registration honours the flag for any schedule, but the fire path
    /// (`scheduled_fire_encodes_shard`) only encodes the shard id into the minted
    /// `ExecutionId` for DAGs and canaries — it derives that decision purely from
    /// the workflow name and DAG-ness of the persisted `harvest_schedules` row,
    /// which carries no `all_writable_shards` column. So a plain workflow opting
    /// in would register on every writable shard yet mint every execution with
    /// `ExecutionId::new()` (the router's default shard), producing duplicate
    /// runs on the default shard and cross-shard write inconsistency. Making it
    /// general-purpose would require persisting the flag as a `harvest_schedules`
    /// column — a migration — which issue #796 (AC10) deliberately avoids; the
    /// canary/DAG path stays migration-free via name-based shard encoding.
    #[serde(default)]
    pub all_writable_shards: bool,
}

const fn default_buffer_all_max() -> u32 {
    100
}

impl WorkflowSchedule {
    /// Create a new workflow schedule with sensible defaults.
    ///
    /// Defaults: `input = null`, `catchup = false`, `max_active_runs = 1`,
    /// `paused = false`, `queue_name = "default"`, `overlap_policy = Skip`,
    /// `buffer_all_max = 100`, `calendar = None`, `skip_policy = Skip`,
    /// `catchup_policy = None` (falls back to the `catchup` bool),
    /// `jitter = default_schedule_jitter(&schedule)`.
    #[must_use]
    pub fn new(workflow_name: impl Into<String>, schedule: Schedule) -> Self {
        let jitter = default_schedule_jitter(&schedule);
        Self {
            workflow_name: workflow_name.into(),
            dag_name: None,
            schedule,
            input: serde_json::Value::Null,
            catchup: false,
            max_active_runs: 1,
            paused: false,
            queue_name: "default".to_string(),
            jitter,
            overlap_policy: OverlapPolicy::Skip,
            buffer_all_max: 100,
            execution_timeout: None,
            chain_execution_timeout: None,
            calendar: None,
            skip_policy: SkipPolicy::Skip,
            consecutive_failure_limit: None,
            end_at: None,
            max_runs: None,
            catchup_policy: None,
            retry_policy: None,
            all_writable_shards: false,
        }
    }

    /// Register this schedule on every writable shard (issue #796).
    ///
    /// Opt-in per-writable-shard coverage: the scheduler upserts one row per
    /// writable shard and each shard's fire mints an execution on that shard.
    /// The default (`false`) pins the schedule to a single shard exactly as
    /// today. Used by the built-in synthetic liveness canary so a single
    /// dead/write-blocked shard surfaces as a failing probe for that shard.
    #[must_use]
    pub const fn with_all_writable_shards(mut self) -> Self {
        self.all_writable_shards = true;
        self
    }

    /// Set the per-run execution timeout for this schedule.
    ///
    /// Every workflow started by this schedule will have its `deadline_at` set
    /// to `started_at + timeout`.  `None` disables the per-run deadline.
    #[must_use]
    pub const fn with_execution_timeout(mut self, timeout: Duration) -> Self {
        self.execution_timeout = Some(timeout);
        self
    }

    /// Set the chain-scoped lifetime cap for this schedule (issue #617).
    ///
    /// **Currently inert** — mirroring #243's [`with_execution_timeout`](Self::with_execution_timeout),
    /// the value set here is NOT persisted on `harvest_schedules` and NOT read by
    /// the scheduler tick. To give scheduled runs a chain cap, declare it on the
    /// workflow type via `#[workflow(chain_execution_timeout = "…")]` (inherited
    /// through `WorkflowInfo` at tick) and/or set the fleet-wide ceiling with
    /// [`HarvestBuilder::max_workflow_chain_timeout`]. This builder method is kept
    /// for API symmetry with #243's third surface.
    #[must_use]
    pub const fn with_chain_execution_timeout(mut self, timeout: Duration) -> Self {
        self.chain_execution_timeout = Some(timeout);
        self
    }

    /// Set the JSON input passed to each scheduled run.
    #[must_use]
    pub fn with_input(mut self, input: serde_json::Value) -> Self {
        self.input = input;
        self
    }

    /// Set the task queue each scheduled run is dispatched onto.
    ///
    /// Defaults to `"default"`. Used by the built-in synthetic liveness canary
    /// (issue #796) to route each per-queue probe onto the queue it exercises.
    #[must_use]
    pub fn with_queue_name(mut self, queue: impl Into<String>) -> Self {
        self.queue_name = queue.into();
        self
    }

    /// Enable or disable catchup for missed runs.
    #[must_use]
    pub const fn with_catchup(mut self, catchup: bool) -> Self {
        self.catchup = catchup;
        self
    }

    /// Override the maximum number of concurrent scheduled runs.
    #[must_use]
    pub const fn with_max_active_runs(mut self, max: u32) -> Self {
        self.max_active_runs = max;
        self
    }

    /// Set the initial paused state.
    #[must_use]
    pub const fn with_paused(mut self, paused: bool) -> Self {
        self.paused = paused;
        self
    }

    /// Set the overlap policy for this schedule.
    ///
    /// Determines what happens when a new firing collides with a still-running
    /// execution from the same schedule. See [`OverlapPolicy`] for semantics.
    #[must_use]
    pub const fn with_overlap_policy(mut self, policy: OverlapPolicy) -> Self {
        self.overlap_policy = policy;
        self
    }

    /// Set the maximum buffer size for [`OverlapPolicy::BufferAll`].
    ///
    /// Firings beyond this cap are dropped and recorded as skipped with
    /// `reason = "buffer_full"`. Has no effect for other overlap policies.
    #[must_use]
    pub const fn with_buffer_all_max(mut self, max: u32) -> Self {
        self.buffer_all_max = max;
        self
    }

    /// Set the maximum jitter window for this schedule.
    ///
    /// The scheduler shifts the effective fire time forward by a deterministic
    /// offset in `[0, jitter)` computed from `(schedule_id, scheduled_fire_time)`.
    /// Identical inputs always produce the same offset, so backfills and restarts
    /// never re-roll the spread.
    ///
    /// Validation at build time rejects values that would cause consecutive fires
    /// to collide (`jitter >= period` for `Interval` schedules) or exceed the
    /// 1-hour sane upper bound for `Cron` schedules.
    ///
    /// `Duration::ZERO` turns off the default cron jitter.
    #[must_use]
    pub const fn with_jitter(mut self, jitter: Duration) -> Self {
        self.jitter = jitter;
        self
    }

    /// Attach a named calendar to this schedule.
    ///
    /// On each firing the scheduler consults the `harvest_calendars` table for
    /// a calendar with this name and applies [`skip_policy`](Self::skip_policy)
    /// when the fire date is excluded. Pass `None` to remove a previously set
    /// calendar (disables calendar filtering).
    #[must_use]
    pub fn with_calendar(mut self, calendar: impl Into<Option<String>>) -> Self {
        self.calendar = calendar.into();
        self
    }

    /// Set the skip policy applied when the fire date falls on an excluded calendar day.
    ///
    /// Has no effect when [`calendar`](Self::calendar) is `None`.
    #[must_use]
    pub const fn with_skip_policy(mut self, policy: SkipPolicy) -> Self {
        self.skip_policy = policy;
        self
    }

    /// Auto-pause this schedule after `limit` consecutive `FAILED` or `TIMED_OUT` execution
    /// completions. The scheduler emits `harvest.schedule.auto_paused` and stops
    /// firing until the operator resumes via `POST /admin/schedules/{id}/resume`.
    ///
    /// Passing `None` disables auto-pause (the default). Passing `Some(0)` is
    /// treated as disabled — a limit of zero would auto-pause on the very first
    /// tick before any execution has a chance to run.
    #[must_use]
    pub const fn with_consecutive_failure_limit(mut self, limit: u32) -> Self {
        self.consecutive_failure_limit = Some(limit);
        self
    }

    /// Set a bounded catchup policy for missed fire slots after scheduler downtime (issue #484).
    ///
    /// When set, this takes precedence over the `catchup` bool.  See
    /// [`CatchupPolicy`] for the full semantics of each variant.
    ///
    /// Calling this is the preferred way to configure catchup behaviour; the
    /// `with_catchup(bool)` builder is still accepted for backward compatibility
    /// but `with_catchup_policy` wins when both are set.
    ///
    /// # Example
    ///
    /// ```rust
    /// use autumn_harvest::policy::{CatchupPolicy, Schedule, WorkflowSchedule};
    ///
    /// let sched = WorkflowSchedule::new("billing", Schedule::Cron("*/15 * * * *".to_string()))
    ///     .with_catchup_policy(CatchupPolicy::MostRecent);
    /// ```
    #[must_use]
    pub const fn with_catchup_policy(mut self, policy: CatchupPolicy) -> Self {
        self.catchup_policy = Some(policy);
        self
    }

    /// Configure a bounded catchup window for missed fire slots after scheduler
    /// downtime (issue #484).
    ///
    /// Equivalent to `.with_catchup_policy(CatchupPolicy::Window(window))`.
    /// `window` must be non-negative (enforced by the type; `Duration::ZERO`
    /// is valid and fires only slots at exactly `now`).
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::time::Duration;
    /// use autumn_harvest::policy::{Schedule, WorkflowSchedule};
    ///
    /// // Fire missed slots only from the last 2 hours.
    /// let sched = WorkflowSchedule::new("import", Schedule::Cron("0 * * * *".to_string()))
    ///     .with_catchup_window(Duration::from_secs(2 * 3600));
    /// ```
    #[must_use]
    pub const fn with_catchup_window(mut self, window: Duration) -> Self {
        self.catchup_policy = Some(CatchupPolicy::Window(window));
        self
    }

    /// Set an absolute UTC cutoff for this schedule (issue #478).
    ///
    /// When the due fire time is `>= end_at`, the scheduler does not start a run and
    /// transitions the schedule to a terminal exhausted state.  The exhaustion is
    /// distinct from an operator `paused` state so the two are distinguishable via
    /// `GET /admin/schedules`.
    ///
    /// Composes cleanly with `max_runs`, `OverlapPolicy`, calendar filtering, and
    /// `paused`.  A skipped firing does not trigger exhaustion.
    #[must_use]
    pub const fn with_end_at(mut self, end_at: DateTime<Utc>) -> Self {
        self.end_at = Some(end_at);
        self
    }

    /// Set a total run budget for this schedule (issue #478).
    ///
    /// Each actually-started run decrements the budget atomically.  Firings suppressed
    /// by overlap policy, calendar filtering, or `paused` do **not** consume a slot.
    /// When the budget reaches zero the schedule transitions to the terminal exhausted
    /// state.
    ///
    /// `max_runs = 0` is normalised to `None` (no limit) so callers cannot
    /// accidentally produce a schedule that never fires.
    #[must_use]
    pub const fn with_max_runs(mut self, max: u32) -> Self {
        self.max_runs = if max == 0 { None } else { Some(max) };
        self
    }

    /// Set a remaining-action budget for this schedule (issue #543).
    ///
    /// An alias for [`with_max_runs`](Self::with_max_runs) — `max_runs`/`runs_started`
    /// on `harvest_schedules` already implement the "declare a firing budget, decrement
    /// exactly-once per actually-started run under the HA claim (#350), never consumed
    /// by a suppressed firing" contract issue #543 asks for. This method exists so
    /// callers can spell the intent as "N actions remaining" without needing to know
    /// the underlying total-budget representation.
    #[must_use]
    pub const fn with_limited_actions(self, limit: u32) -> Self {
        self.with_max_runs(limit)
    }

    /// Set a default retry policy for runs started by this schedule (issue #523).
    ///
    /// Each run started by this schedule will use this as its retry policy
    /// (unless overridden by a per-start API call). `None` disables retry.
    #[must_use]
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = Some(policy);
        self
    }
}

/// Validate a [`Schedule`] value, returning an error string if it is invalid.
///
/// For [`Schedule::Cron`] expressions this parses the expression using
/// `croner` (5-field or 6-field with seconds). A [`Schedule::Interval`] period
/// must be a whole number of seconds greater than zero.
///
/// # Errors
///
/// Returns a human-readable error string if the cron expression is
/// syntactically invalid. Also returns one if the interval is zero or has a
/// fractional second.
pub fn validate_schedule(schedule: &Schedule) -> Result<(), String> {
    match schedule {
        Schedule::Cron(expr) => Cron::new(expr)
            .with_seconds_optional()
            .parse()
            .map(|_| ())
            .map_err(|e| format!("invalid cron expression '{expr}': {e}")),
        Schedule::CronInTimezone { expr, tz } => {
            // Validate the IANA timezone name first so callers get a clear error.
            if tz.parse::<chrono_tz::Tz>().is_err() {
                return Err(format!(
                    "unknown timezone '{tz}'; use an IANA timezone name (e.g. \"America/Los_Angeles\", \"UTC\")"
                ));
            }
            Cron::new(expr)
                .with_seconds_optional()
                .parse()
                .map(|_| ())
                .map_err(|e| format!("invalid cron expression '{expr}': {e}"))
        }
        // A zero (or non-positive) interval never advances, so a due catchup tick
        // would spin forever (issue #484 / Codex #3223). Reject it at registration
        // time; the runtime `next_run_after` also treats it as non-firing as a
        // belt-and-braces guard.
        Schedule::Interval(period) if period.is_zero() => {
            Err("interval schedule period must be greater than zero".to_string())
        }
        // `schedule_expr` drops a fraction on write. A sub-second period reads
        // back as zero (issue #1967). The scheduler ticks once a second, so it
        // cannot keep a shorter cadence.
        Schedule::Interval(period) if period.subsec_nanos() != 0 => Err(format!(
            "interval schedule period must be a whole number of seconds, got {period:?}"
        )),
        Schedule::Interval(_) | Schedule::Manual => Ok(()),
    }
}

/// Default fire jitter for a cron schedule with no seconds field (issue #1792).
pub const DEFAULT_CRON_JITTER: Duration = Duration::from_secs(10);

/// Return the default fire jitter for `schedule`.
///
/// A cron with fewer than six fields, such as `"0 9 * * *"` or `"@hourly"`,
/// fires at most once a minute. A 10 s offset thus cannot move a fire past the
/// next slot. A six-field cron has a seconds field. It can fire more often than
/// every 10 s, so it gets zero. Interval and manual schedules also get zero.
#[must_use]
pub fn default_schedule_jitter(schedule: &Schedule) -> Duration {
    match schedule {
        Schedule::Cron(expr) | Schedule::CronInTimezone { expr, .. }
            if expr.split_whitespace().count() < 6 =>
        {
            DEFAULT_CRON_JITTER
        }
        _ => Duration::ZERO,
    }
}

/// Maximum jitter allowed for a [`Schedule::Cron`] schedule (1 hour).
pub const MAX_CRON_JITTER: Duration = Duration::from_secs(3600);

/// Validate a jitter window against a schedule's natural period.
///
/// # Rules
///
/// - `Duration::ZERO` is always valid (disables jitter).
/// - For `Schedule::Interval(period)`: `jitter` must be `< period` so that two
///   consecutive fired slots cannot collide.
/// - For `Schedule::Cron(_)`: `jitter` must be `<= 1 hour`.
/// - For `Schedule::Manual`: any value is accepted (jitter has no effect).
///
/// # Errors
///
/// Returns a human-readable error string describing the violated constraint.
pub fn validate_jitter(schedule: &Schedule, jitter: Duration) -> Result<(), String> {
    if jitter.is_zero() {
        return Ok(());
    }
    match schedule {
        Schedule::Interval(period) => {
            if jitter >= *period {
                return Err(format!(
                    "jitter ({jitter:?}) must be less than the interval period ({period:?})"
                ));
            }
        }
        Schedule::Cron(_) | Schedule::CronInTimezone { .. } => {
            if jitter > MAX_CRON_JITTER {
                return Err(format!(
                    "jitter ({jitter:?}) exceeds the 1-hour maximum for cron schedules"
                ));
            }
        }
        Schedule::Manual => {}
    }
    Ok(())
}

/// Compute the deterministic jitter offset for a scheduled fire.
///
/// The offset is a pure function of `(schedule_id, fire_time)` so that:
/// - Scheduler restarts and leader handoffs never re-roll the value.
/// - Backfills under the same `(schedule_id, fire_time)` reproduce the same
///   effective fire time.
///
/// Returns `Duration::ZERO` when `jitter` is zero.
///
/// The hash uses `seahash` over `[schedule_id_bytes (16) || fire_time_nanos_le (8)]`,
/// mirroring the shard-router pattern already present in this crate.
#[must_use]
pub fn compute_jitter_offset(
    schedule_id: Uuid,
    fire_time: DateTime<Utc>,
    jitter: Duration,
) -> Duration {
    if jitter.is_zero() {
        return Duration::ZERO;
    }
    let jitter_nanos = u64::try_from(jitter.as_nanos()).unwrap_or(u64::MAX);
    let fire_nanos = fire_time
        .timestamp_nanos_opt()
        .unwrap_or_default()
        .cast_unsigned();
    let mut bytes = [0u8; 24];
    bytes[..16].copy_from_slice(schedule_id.as_bytes());
    bytes[16..].copy_from_slice(&fire_nanos.to_le_bytes());
    let hash = seahash::hash(&bytes);
    Duration::from_nanos(hash % jitter_nanos)
}

/// Resolve the effective activity retry policy at schedule time (issue #620).
///
/// Precedence, highest first:
///   call-site override → activity `#[activity(retry = …)]` default →
///   builder-level default (`WorkerConfig::with_default_activity_retry_policy`).
///
/// Returns `None` when nothing is set anywhere, preserving today's implicit
/// fallback (the enqueue path's own `max_attempts` default). Opt-in: an unset
/// builder default is a pure no-op via `.or(None)`.
///
/// The sole non-test consumer is the `db`-gated worker dispatch path; unused
/// under `--no-default-features` (the pure precedence is still test-covered).
#[must_use]
#[cfg_attr(not(feature = "db"), allow(dead_code))]
pub(crate) fn resolve_effective_retry(
    call: Option<RetryPolicy>,
    activity: Option<RetryPolicy>,
    builder: Option<RetryPolicy>,
) -> Option<RetryPolicy> {
    call.or(activity).or(builder)
}

/// Resolve the effective activity `start_to_close` at schedule time (issue #620).
///
/// Same precedence as [`resolve_effective_retry`]: call-site override →
/// activity default → builder default. `None` means that no timeout applies.
/// `WorkerConfig` sets a 10-minute builder default (issue #1808).
///
/// The sole non-test consumer is the `db`-gated worker dispatch path; unused
/// under `--no-default-features` (the pure precedence is still test-covered).
#[must_use]
#[cfg_attr(not(feature = "db"), allow(dead_code))]
pub(crate) fn resolve_effective_start_to_close(
    call: Option<Duration>,
    activity: Option<Duration>,
    builder: Option<Duration>,
) -> Option<Duration> {
    call.or(activity).or(builder)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    // ── Circuit open mode (issue #1809) ────────────────────────────────────

    #[test]
    fn circuit_breaker_policy_defers_by_default() {
        let policy = CircuitBreakerPolicy::new(3, Duration::from_secs(30), Duration::from_secs(60));
        assert_eq!(policy.open_mode, CircuitOpenMode::Defer);
        assert_eq!(CircuitOpenMode::default(), CircuitOpenMode::Defer);
    }

    #[test]
    fn circuit_breaker_policy_with_open_mode_sets_fail_fast() {
        let policy = CircuitBreakerPolicy::new(3, Duration::from_secs(30), Duration::from_secs(60))
            .with_open_mode(CircuitOpenMode::FailFast);
        assert_eq!(policy.open_mode, CircuitOpenMode::FailFast);
    }

    #[test]
    fn circuit_breaker_policy_without_open_mode_deserializes_to_defer() {
        let mut json = serde_json::to_value(CircuitBreakerPolicy::new(
            3,
            Duration::from_secs(30),
            Duration::from_secs(60),
        ))
        .unwrap();
        json.as_object_mut().unwrap().remove("open_mode");
        let policy: CircuitBreakerPolicy = serde_json::from_value(json).unwrap();
        assert_eq!(policy.open_mode, CircuitOpenMode::Defer);
    }

    #[test]
    fn circuit_open_mode_serializes_as_snake_case() {
        assert_eq!(
            serde_json::to_value(CircuitOpenMode::FailFast).unwrap(),
            serde_json::json!("fail_fast")
        );
        assert_eq!(
            serde_json::to_value(CircuitOpenMode::Defer).unwrap(),
            serde_json::json!("defer")
        );
    }

    // ── Retry-After hint clamp/resolve (issue #744) ────────────────────────────
    //
    // RED PHASE: `resolve_retry_after_hint` does not exist yet -- these tests
    // fail to COMPILE against the missing `crate::policy` symbol until the
    // green phase adds it.

    #[test]
    fn resolve_retry_after_hint_none_falls_through() {
        assert_eq!(
            resolve_retry_after_hint(None, Duration::from_secs(900)),
            None,
            "no hint present -> fall through to the policy's own delay"
        );
    }

    #[test]
    fn resolve_retry_after_hint_zero_falls_through() {
        // Duration cannot be negative, so "<= 0" reduces to exactly ZERO.
        assert_eq!(
            resolve_retry_after_hint(Some(Duration::ZERO), Duration::from_secs(900)),
            None,
            "a zero-duration hint must fall through to the policy delay (AC3)"
        );
    }

    #[test]
    fn resolve_retry_after_hint_zero_ceiling_clamps_a_positive_hint_to_zero_not_none() {
        // Confirmed edge case (issue #744 review): `ceiling = ZERO` is NOT a
        // "disable the feature" switch. A positive hint clamps down to
        // Duration::ZERO (an immediate retry), it does NOT fall through to
        // `None` (the policy delay) the way a genuinely absent/zero HINT
        // would. Contrast with `resolve_retry_after_hint_zero_falls_through`
        // above, which tests the opposite axis (hint == ZERO).
        assert_eq!(
            resolve_retry_after_hint(Some(Duration::from_secs(30)), Duration::ZERO),
            Some(Duration::ZERO),
            "a zero ceiling clamps a positive hint down to zero, it does not disable honoring"
        );
    }

    #[test]
    fn resolve_retry_after_hint_under_ceiling_is_honored_verbatim() {
        assert_eq!(
            resolve_retry_after_hint(Some(Duration::from_secs(30)), Duration::from_secs(900)),
            Some(Duration::from_secs(30)),
        );
    }

    #[test]
    fn resolve_retry_after_hint_at_ceiling_is_honored_verbatim() {
        assert_eq!(
            resolve_retry_after_hint(Some(Duration::from_secs(900)), Duration::from_secs(900)),
            Some(Duration::from_secs(900)),
            "a hint exactly at the ceiling must not be treated as over-ceiling"
        );
    }

    #[test]
    fn resolve_retry_after_hint_over_ceiling_is_clamped_not_rejected() {
        assert_eq!(
            resolve_retry_after_hint(Some(Duration::from_secs(3600)), Duration::from_secs(900)),
            Some(Duration::from_secs(900)),
            "an over-ceiling hint must clamp down, never error/reject (AC3)"
        );
    }

    #[test]
    fn resolve_retry_after_hint_one_nanosecond_is_honored() {
        // The smallest possible positive Duration must not be treated as "zero".
        assert_eq!(
            resolve_retry_after_hint(Some(Duration::from_nanos(1)), Duration::from_secs(900)),
            Some(Duration::from_nanos(1)),
        );
    }

    #[test]
    fn resolve_retry_after_hint_100_randomized_hints_never_resolve_below_the_clamp() {
        // Success-metric property test (issue #744): for 100 varied hint /
        // ceiling combinations, the resolved delay is never less than
        // `min(hint, ceiling)` when the hint is a positive duration -- i.e. a
        // retry scheduled from this resolution can never fire earlier than
        // the (possibly clamped) hinted time. Deterministic PRNG, no sleeping.
        fn mix(seed: &mut u64) -> u64 {
            *seed ^= *seed << 13;
            *seed ^= *seed >> 7;
            *seed ^= *seed << 17;
            *seed
        }

        let mut seed: u64 = 0x744_u64.wrapping_add(1);
        for _ in 0..100 {
            let hint_secs = mix(&mut seed) % 7200; // 0..2h
            let ceiling_secs = 1 + (mix(&mut seed) % 1800); // 1s..30m, never zero
            let hint = Duration::from_secs(hint_secs);
            let ceiling = Duration::from_secs(ceiling_secs);

            let resolved = resolve_retry_after_hint(Some(hint), ceiling);

            if hint.is_zero() {
                assert_eq!(resolved, None, "zero hint must fall through");
            } else {
                let expected = hint.min(ceiling);
                assert_eq!(
                    resolved,
                    Some(expected),
                    "hint={hint:?} ceiling={ceiling:?} must resolve to exactly min(hint, ceiling) \
                     -- exact equality is a strictly stronger guarantee than \"resolved >= \
                     min(hint, ceiling)\", which this exact-equality assertion already implies"
                );
            }
        }
    }

    // ── Builder-level activity default floor (issue #620) ─────────────────────
    //
    // RED PHASE: `resolve_effective_retry` / `resolve_effective_start_to_close`
    // do not exist yet — these tests fail to COMPILE against the missing
    // `crate::policy` symbols until the green phase adds them. The precedence
    // the green phase must implement is:
    //   call-site override  →  activity default  →  builder default  →  None
    // i.e. `call.or(activity).or(builder)`.

    #[test]
    fn resolve_effective_retry_precedence() {
        // Distinct policies so the returned identity is unambiguous.
        let call = RetryPolicy::fixed(5, Duration::from_millis(10));
        let activity = RetryPolicy::fixed(2, Duration::from_millis(10));
        let builder = RetryPolicy::fixed(9, Duration::from_millis(10));

        // All three present → call-site wins.
        assert_eq!(
            resolve_effective_retry(Some(call), Some(activity.clone()), Some(builder.clone()))
                .map(|p| p.max_attempts),
            Some(5),
        );

        // No call-site → activity default wins over builder default.
        assert_eq!(
            resolve_effective_retry(None, Some(activity), Some(builder.clone()))
                .map(|p| p.max_attempts),
            Some(2),
        );

        // No call-site, no activity default → builder default applies.
        assert_eq!(
            resolve_effective_retry(None, None, Some(builder)).map(|p| p.max_attempts),
            Some(9),
        );

        // Nothing set anywhere → None (implicit fallback handled downstream).
        assert!(resolve_effective_retry(None, None, None).is_none());
    }

    #[test]
    fn resolve_effective_start_to_close_precedence() {
        let call = Duration::from_secs(5);
        let activity = Duration::from_secs(30);
        let builder = Duration::from_secs(300);

        // All three present → call-site wins.
        assert_eq!(
            resolve_effective_start_to_close(Some(call), Some(activity), Some(builder)),
            Some(call),
        );

        // No call-site → activity default wins over builder default.
        assert_eq!(
            resolve_effective_start_to_close(None, Some(activity), Some(builder)),
            Some(activity),
        );

        // No call-site, no activity default → builder default applies.
        assert_eq!(
            resolve_effective_start_to_close(None, None, Some(builder)),
            Some(builder),
        );

        // Nothing set anywhere → None (no timeout enforced).
        assert_eq!(resolve_effective_start_to_close(None, None, None), None);
    }

    // ── Schedule jitter ───────────────────────────────────────────────────────

    #[test]
    fn workflow_schedule_manual_jitter_defaults_to_zero() {
        let sched = WorkflowSchedule::new("my_workflow", Schedule::Manual);
        assert_eq!(sched.jitter, Duration::ZERO);
    }

    #[test]
    fn workflow_schedule_cron_defaults_to_small_jitter() {
        let cron = WorkflowSchedule::new("wf", Schedule::Cron("0 * * * *".to_string()));
        assert_eq!(cron.jitter, DEFAULT_CRON_JITTER);
        assert_eq!(DEFAULT_CRON_JITTER, Duration::from_secs(10));
        let alias = WorkflowSchedule::new("wf", Schedule::Cron("@hourly".to_string()));
        assert_eq!(alias.jitter, DEFAULT_CRON_JITTER);
        let zoned = WorkflowSchedule::new(
            "wf",
            Schedule::CronInTimezone {
                expr: "0 9 * * *".to_string(),
                tz: "Europe/Paris".to_string(),
            },
        );
        assert_eq!(zoned.jitter, DEFAULT_CRON_JITTER);
        assert!(validate_jitter(&cron.schedule, cron.jitter).is_ok());
    }

    #[test]
    fn workflow_schedule_default_jitter_is_zero_when_it_could_collide() {
        // A cron with a seconds field can fire more often than the default window.
        let seconds = Schedule::Cron("*/5 * * * * *".to_string());
        assert_eq!(default_schedule_jitter(&seconds), Duration::ZERO);
        let zoned_seconds = Schedule::CronInTimezone {
            expr: "0 */5 * * * *".to_string(),
            tz: "UTC".to_string(),
        };
        assert_eq!(default_schedule_jitter(&zoned_seconds), Duration::ZERO);
        let interval = Schedule::Interval(Duration::from_secs(5));
        assert_eq!(default_schedule_jitter(&interval), Duration::ZERO);
        assert_eq!(default_schedule_jitter(&Schedule::Manual), Duration::ZERO);
    }

    #[test]
    fn default_cron_jitter_is_whole_seconds_below_one_minute() {
        // `harvest_schedules.jitter_secs` stores whole seconds. A fractional
        // default would lose its fraction.
        assert_eq!(DEFAULT_CRON_JITTER.subsec_nanos(), 0);
        assert!(DEFAULT_CRON_JITTER > Duration::ZERO);
        assert!(DEFAULT_CRON_JITTER < Duration::from_secs(60));
    }

    #[test]
    fn workflow_schedule_cron_jitter_opt_out_is_zero() {
        let sched = WorkflowSchedule::new("wf", Schedule::Cron("0 * * * *".to_string()))
            .with_jitter(Duration::ZERO);
        assert_eq!(sched.jitter, Duration::ZERO);
    }

    #[test]
    fn workflow_schedule_with_jitter_sets_duration() {
        let sched =
            WorkflowSchedule::new("my_wf", Schedule::Manual).with_jitter(Duration::from_secs(300));
        assert_eq!(sched.jitter, Duration::from_secs(300));
    }

    #[test]
    fn validate_jitter_zero_always_accepted() {
        assert!(validate_jitter(&Schedule::Manual, Duration::ZERO).is_ok());
        assert!(
            validate_jitter(&Schedule::Interval(Duration::from_secs(60)), Duration::ZERO).is_ok()
        );
        assert!(validate_jitter(&Schedule::Cron("0 * * * *".to_string()), Duration::ZERO).is_ok());
    }

    #[test]
    fn validate_jitter_interval_gte_period_is_error() {
        let period = Duration::from_secs(60);
        assert!(
            validate_jitter(&Schedule::Interval(period), Duration::from_secs(60)).is_err(),
            "jitter equal to period must be rejected"
        );
        assert!(
            validate_jitter(&Schedule::Interval(period), Duration::from_secs(90)).is_err(),
            "jitter greater than period must be rejected"
        );
    }

    #[test]
    fn validate_jitter_interval_lt_period_is_ok() {
        let period = Duration::from_secs(60);
        assert!(validate_jitter(&Schedule::Interval(period), Duration::from_secs(59)).is_ok());
        assert!(validate_jitter(&Schedule::Interval(period), Duration::from_secs(1)).is_ok());
    }

    #[test]
    fn validate_jitter_cron_gt_one_hour_is_error() {
        let cron = Schedule::Cron("0 * * * *".to_string());
        assert!(validate_jitter(&cron, Duration::from_secs(3601)).is_err());
        assert!(validate_jitter(&cron, Duration::from_secs(7200)).is_err());
    }

    #[test]
    fn validate_jitter_cron_lte_one_hour_is_ok() {
        let cron = Schedule::Cron("0 * * * *".to_string());
        assert!(validate_jitter(&cron, Duration::from_secs(3600)).is_ok());
        assert!(validate_jitter(&cron, Duration::from_secs(300)).is_ok());
    }

    #[test]
    fn compute_jitter_offset_deterministic() {
        use chrono::{DateTime, Utc};
        use uuid::Uuid;
        let id = Uuid::from_u128(42);
        let fire_time = "2026-04-01T10:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let jitter = Duration::from_secs(300);
        let first = compute_jitter_offset(id, fire_time, jitter);
        for _ in 0..999 {
            assert_eq!(compute_jitter_offset(id, fire_time, jitter), first);
        }
    }

    #[test]
    fn compute_jitter_offset_zero_jitter_returns_zero() {
        use chrono::{DateTime, Utc};
        use uuid::Uuid;
        let id = Uuid::from_u128(1);
        let fire_time = "2026-04-01T10:00:00Z".parse::<DateTime<Utc>>().unwrap();
        assert_eq!(
            compute_jitter_offset(id, fire_time, Duration::ZERO),
            Duration::ZERO
        );
    }

    #[test]
    fn compute_jitter_offset_within_bounds() {
        use chrono::{DateTime, Utc};
        use uuid::Uuid;
        let id = Uuid::from_u128(12345);
        let fire_time = "2026-04-01T10:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let jitter = Duration::from_secs(300);
        let offset = compute_jitter_offset(id, fire_time, jitter);
        assert!(
            offset < jitter,
            "offset {offset:?} must be < jitter {jitter:?}"
        );
    }

    #[test]
    fn compute_jitter_offset_uniform_distribution() {
        use chrono::{DateTime, Utc};
        use uuid::Uuid;
        let fire_time = "2026-04-01T10:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let jitter = Duration::from_secs(300);
        let num_ids: u128 = 10_000;
        let num_buckets = 10usize;
        let mut buckets = vec![0u32; num_buckets];
        for i in 0..num_ids {
            let id = Uuid::from_u128(i);
            let offset = compute_jitter_offset(id, fire_time, jitter);
            let bucket_width = jitter.as_nanos() / num_buckets as u128;
            let bucket = ((offset.as_nanos() / bucket_width) as usize).min(num_buckets - 1);
            buckets[bucket] += 1;
        }
        for (i, &count) in buckets.iter().enumerate() {
            assert!(
                count > 500 && count < 1500,
                "bucket {i} has {count} items; expected ~1000 (range 500–1500)"
            );
        }
    }

    #[test]
    fn retry_policy_constructors_default_to_full_jitter() {
        assert_eq!(JitterPolicy::default(), JitterPolicy::Full);
        assert_eq!(RetryPolicy::default().jitter, JitterPolicy::Full);
        let exp = RetryPolicy::exponential(3, Duration::from_secs(1));
        assert_eq!(exp.jitter, JitterPolicy::Full);
        let fixed = RetryPolicy::fixed(3, Duration::from_secs(1));
        assert_eq!(fixed.jitter, JitterPolicy::Full);
    }

    #[test]
    fn retry_policy_without_jitter_key_deserializes_to_full() {
        let json = serde_json::json!({
            "max_attempts": 3,
            "initial_interval": {"secs": 1, "nanos": 0},
            "backoff_coefficient": 2.0,
            "max_interval": {"secs": 300, "nanos": 0},
            "non_retryable_errors": [],
        });
        let policy: RetryPolicy = serde_json::from_value(json).expect("valid policy");
        assert_eq!(policy.jitter, JitterPolicy::Full);
    }

    #[test]
    fn retry_policy_explicit_none_jitter_round_trips() {
        let policy = RetryPolicy::default().with_jitter(JitterPolicy::None);
        let json = serde_json::to_value(&policy).expect("serializes");
        let back: RetryPolicy = serde_json::from_value(json).expect("deserializes");
        assert_eq!(back.jitter, JitterPolicy::None);
    }

    /// Tasks that fail together must not retry together (issue #1792).
    #[test]
    fn default_retry_policy_jitters_delays_across_tasks() {
        let policy = RetryPolicy::default();
        for attempt in 1..policy.max_attempts {
            let delays: std::collections::HashSet<Duration> = (0..100_u64)
                .map(|task| policy.next_delay_with_seed(attempt, mix64(task)).unwrap())
                .collect();
            assert!(
                delays.len() > 1,
                "attempt {attempt}: 100 tasks got one delay {delays:?}"
            );
        }
    }

    #[test]
    fn exponential_backoff_doubles() {
        let policy =
            RetryPolicy::exponential(5, Duration::from_secs(1)).with_jitter(JitterPolicy::None);
        assert_eq!(policy.next_delay(1), Some(Duration::from_secs(1)));
        assert_eq!(policy.next_delay(2), Some(Duration::from_secs(2)));
        assert_eq!(policy.next_delay(3), Some(Duration::from_secs(4)));
    }

    #[test]
    fn fixed_backoff_stays_constant() {
        let policy = RetryPolicy::fixed(3, Duration::from_secs(5)).with_jitter(JitterPolicy::None);
        assert_eq!(policy.next_delay(1), Some(Duration::from_secs(5)));
        assert_eq!(policy.next_delay(2), Some(Duration::from_secs(5)));
    }

    #[test]
    fn no_retry_after_max_attempts() {
        let policy = RetryPolicy::exponential(3, Duration::from_secs(1));
        assert_eq!(policy.next_delay(3), None);
    }

    #[test]
    fn retry_jitter_none_ignores_the_seed() {
        let policy =
            RetryPolicy::exponential(5, Duration::from_secs(1)).with_jitter(JitterPolicy::None);
        for attempt in 1..5 {
            assert_eq!(
                policy.next_delay(attempt),
                policy.next_delay_with_seed(attempt, 123_456_789)
            );
        }
    }

    #[test]
    fn retry_jitter_bounds_over_10k_seeds() {
        let base =
            RetryPolicy::exponential(8, Duration::from_millis(200)).with_jitter(JitterPolicy::None);
        for attempt in 1..6 {
            let base_delay = base.next_delay(attempt).unwrap();
            for seed in 0..10_000_u64 {
                let full = base
                    .clone()
                    .with_jitter(JitterPolicy::Full)
                    .next_delay_with_seed(attempt, seed)
                    .unwrap();
                assert!(full <= base_delay);
                let equal = base
                    .clone()
                    .with_jitter(JitterPolicy::Equal)
                    .next_delay_with_seed(attempt, seed)
                    .unwrap();
                assert!(equal >= base_delay / 2);
                assert!(equal <= base_delay);
                let deco = base
                    .clone()
                    .with_jitter(JitterPolicy::Decorrelated)
                    .next_delay_with_seed(attempt, seed)
                    .unwrap();
                let prev = if attempt == 1 {
                    base.initial_interval
                } else {
                    base.next_delay(attempt - 1).unwrap()
                };
                let upper = prev.saturating_mul(3).min(base.max_interval);
                assert!(deco >= base.initial_interval);
                assert!(deco <= upper);
            }
        }
    }

    #[test]
    fn exponential_caps_at_max_interval() -> Result<(), String> {
        let policy = RetryPolicy {
            max_attempts: 10,
            initial_interval: Duration::from_secs(60),
            backoff_coefficient: 2.0,
            max_interval: Duration::from_secs(120),
            non_retryable_errors: vec![],
            jitter: JitterPolicy::None,
        };
        assert_eq!(
            policy.next_delay(6).ok_or("no delay")?,
            Duration::from_secs(120)
        );
        Ok(())
    }

    #[test]
    fn trigger_rule_all_success_requires_all_success() {
        assert!(
            TriggerRule::AllSuccess.should_run(&[TaskStatus::Succeeded, TaskStatus::Succeeded])
        );
        assert!(!TriggerRule::AllSuccess.should_run(&[TaskStatus::Succeeded, TaskStatus::Failed]));
    }

    #[test]
    fn trigger_rule_all_done_runs_on_any_completion() {
        assert!(TriggerRule::AllDone.should_run(&[TaskStatus::Succeeded, TaskStatus::Failed]));
    }

    #[test]
    fn trigger_rule_one_success() {
        assert!(TriggerRule::OneSuccess.should_run(&[TaskStatus::Failed, TaskStatus::Succeeded]));
        assert!(!TriggerRule::OneSuccess.should_run(&[TaskStatus::Failed]));
    }

    #[test]
    fn trigger_rule_one_failed() {
        assert!(TriggerRule::OneFailed.should_run(&[TaskStatus::Succeeded, TaskStatus::Failed]));
        assert!(!TriggerRule::OneFailed.should_run(&[TaskStatus::Succeeded]));
    }

    #[test]
    fn trigger_rule_all_failed() {
        assert!(TriggerRule::AllFailed.should_run(&[TaskStatus::Failed, TaskStatus::Failed]));
        assert!(!TriggerRule::AllFailed.should_run(&[TaskStatus::Succeeded, TaskStatus::Failed]));
    }

    #[test]
    fn trigger_rule_manual_never_fires() {
        assert!(!TriggerRule::Manual.should_run(&[TaskStatus::Succeeded]));
        assert!(!TriggerRule::Manual.should_run(&[]));
    }

    #[test]
    fn trigger_rule_empty_slice_matches_the_documented_root_behavior() {
        // A root node has no upstreams. Only these two rules fire for it.
        assert!(TriggerRule::AllSuccess.should_run(&[]));
        assert!(TriggerRule::AllDone.should_run(&[]));
        // The other four rules skip a root node.
        assert!(!TriggerRule::OneSuccess.should_run(&[]));
        assert!(!TriggerRule::OneFailed.should_run(&[]));
        assert!(!TriggerRule::AllFailed.should_run(&[]));
        assert!(!TriggerRule::Manual.should_run(&[]));
    }

    // ── Bounded schedules (issue #543 / #478) ───────────────────────────────────

    #[test]
    fn workflow_schedule_with_limited_actions_sets_max_runs() {
        let sched = WorkflowSchedule::new("my_wf", Schedule::Manual).with_limited_actions(5);
        assert_eq!(sched.max_runs, Some(5));
    }

    #[test]
    fn workflow_schedule_with_limited_actions_zero_normalises_to_none() {
        let sched = WorkflowSchedule::new("my_wf", Schedule::Manual).with_limited_actions(0);
        assert_eq!(
            sched.max_runs, None,
            "limited_actions(0) must behave identically to max_runs(0) — no limit, \
             not an unfireable schedule"
        );
    }

    #[test]
    fn workflow_schedule_with_limited_actions_matches_with_max_runs() {
        let via_limited = WorkflowSchedule::new("my_wf", Schedule::Manual).with_limited_actions(3);
        let via_max_runs = WorkflowSchedule::new("my_wf", Schedule::Manual).with_max_runs(3);
        assert_eq!(via_limited.max_runs, via_max_runs.max_runs);
    }

    #[test]
    fn workflow_schedule_end_at_and_limited_actions_default_to_none() {
        let sched = WorkflowSchedule::new("my_wf", Schedule::Manual);
        assert_eq!(sched.end_at, None);
        assert_eq!(
            sched.max_runs, None,
            "unbounded by default — today's behaviour"
        );
    }

    #[test]
    fn workflow_schedule_with_limited_actions_composes_with_jitter_and_overlap() {
        let sched = WorkflowSchedule::new("my_wf", Schedule::Manual)
            .with_limited_actions(10)
            .with_jitter(Duration::from_secs(60))
            .with_overlap_policy(OverlapPolicy::CancelOther);
        assert_eq!(sched.max_runs, Some(10));
        assert_eq!(sched.jitter, Duration::from_secs(60));
        assert_eq!(sched.overlap_policy, OverlapPolicy::CancelOther);
    }

    // ── SkipPolicy ────────────────────────────────────────────────────────────

    #[test]
    fn skip_policy_default_is_skip() {
        assert_eq!(SkipPolicy::default(), SkipPolicy::Skip);
    }

    #[test]
    fn skip_policy_as_str_round_trips() {
        let cases = [
            (SkipPolicy::Skip, "skip"),
            (SkipPolicy::RunNextBusinessDay, "run_next_business_day"),
            (SkipPolicy::RunPrevBusinessDay, "run_prev_business_day"),
        ];
        for (policy, s) in cases {
            assert_eq!(policy.as_str(), s, "as_str mismatch for {policy:?}");
            assert_eq!(SkipPolicy::from_db(s), policy, "from_db mismatch for {s}");
        }
    }

    #[test]
    fn skip_policy_from_db_unknown_defaults_to_skip() {
        assert_eq!(SkipPolicy::from_db("unknown"), SkipPolicy::Skip);
        assert_eq!(SkipPolicy::from_db(""), SkipPolicy::Skip);
    }

    #[test]
    fn skip_policy_serde_round_trips() {
        let policies = [
            SkipPolicy::Skip,
            SkipPolicy::RunNextBusinessDay,
            SkipPolicy::RunPrevBusinessDay,
        ];
        for policy in policies {
            let json = serde_json::to_string(&policy).expect("serialize");
            let back: SkipPolicy = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, policy, "serde round-trip failed for {policy:?}");
        }
    }

    #[test]
    fn workflow_schedule_calendar_defaults_to_none() {
        let sched = WorkflowSchedule::new("my_wf", Schedule::Manual);
        assert!(sched.calendar.is_none());
    }

    #[test]
    fn workflow_schedule_with_calendar_sets_field() {
        let sched = WorkflowSchedule::new("my_wf", Schedule::Manual)
            .with_calendar("us-federal-holidays".to_string());
        assert_eq!(sched.calendar.as_deref(), Some("us-federal-holidays"));
    }

    #[test]
    fn workflow_schedule_skip_policy_defaults_to_skip() {
        let sched = WorkflowSchedule::new("my_wf", Schedule::Manual);
        assert_eq!(sched.skip_policy, SkipPolicy::Skip);
    }

    #[test]
    fn workflow_schedule_with_skip_policy_sets_field() {
        let sched = WorkflowSchedule::new("my_wf", Schedule::Manual)
            .with_skip_policy(SkipPolicy::RunNextBusinessDay);
        assert_eq!(sched.skip_policy, SkipPolicy::RunNextBusinessDay);
    }

    // ── OverlapPolicy ─────────────────────────────────────────────────────────

    #[test]
    fn overlap_policy_default_is_skip() {
        assert_eq!(OverlapPolicy::default(), OverlapPolicy::Skip);
    }

    #[test]
    fn overlap_policy_as_str_round_trips() {
        let cases = [
            (OverlapPolicy::Skip, "skip"),
            (OverlapPolicy::BufferOne, "buffer_one"),
            (OverlapPolicy::BufferAll, "buffer_all"),
            (OverlapPolicy::CancelOther, "cancel_other"),
            (OverlapPolicy::TerminateOther, "terminate_other"),
            (OverlapPolicy::AllowAll, "allow_all"),
        ];
        for (policy, s) in cases {
            assert_eq!(policy.as_str(), s, "as_str mismatch for {policy:?}");
            assert_eq!(
                OverlapPolicy::from_db(s),
                policy,
                "from_db mismatch for {s}"
            );
        }
    }

    #[test]
    fn overlap_policy_from_user_input_accepts_every_variant() {
        let cases = [
            ("skip", OverlapPolicy::Skip),
            ("buffer_one", OverlapPolicy::BufferOne),
            ("buffer_all", OverlapPolicy::BufferAll),
            ("cancel_other", OverlapPolicy::CancelOther),
            ("terminate_other", OverlapPolicy::TerminateOther),
            ("allow_all", OverlapPolicy::AllowAll),
        ];
        for (s, policy) in cases {
            assert_eq!(OverlapPolicy::from_user_input(s), Ok(policy));
        }
        assert_eq!(OverlapPolicy::from_user_input("bogus"), Err("bogus"));
    }

    #[test]
    fn overlap_policy_valid_values_names_every_variant() {
        let all = [
            OverlapPolicy::Skip,
            OverlapPolicy::BufferOne,
            OverlapPolicy::BufferAll,
            OverlapPolicy::CancelOther,
            OverlapPolicy::TerminateOther,
            OverlapPolicy::AllowAll,
        ];
        let listed: Vec<&str> = OverlapPolicy::VALID_VALUES.split(", ").collect();
        assert_eq!(listed, all.map(OverlapPolicy::as_str));
    }

    #[test]
    fn overlap_policy_allow_all_serialises_as_snake_case() {
        let json = serde_json::to_string(&OverlapPolicy::AllowAll).expect("serialize");
        assert_eq!(json, "\"allow_all\"");
    }

    #[test]
    fn overlap_policy_from_db_unknown_defaults_to_skip() {
        assert_eq!(OverlapPolicy::from_db("unknown_value"), OverlapPolicy::Skip);
        assert_eq!(OverlapPolicy::from_db(""), OverlapPolicy::Skip);
    }

    #[test]
    fn workflow_schedule_overlap_policy_defaults_to_skip() {
        let sched = WorkflowSchedule::new("my_wf", Schedule::Manual);
        assert_eq!(sched.overlap_policy, OverlapPolicy::Skip);
    }

    #[test]
    fn workflow_schedule_with_overlap_policy_sets_field() {
        let sched = WorkflowSchedule::new("my_wf", Schedule::Manual)
            .with_overlap_policy(OverlapPolicy::BufferOne);
        assert_eq!(sched.overlap_policy, OverlapPolicy::BufferOne);
    }

    #[test]
    fn workflow_schedule_buffer_all_max_defaults_to_100() {
        let sched = WorkflowSchedule::new("my_wf", Schedule::Manual);
        assert_eq!(sched.buffer_all_max, 100);
    }

    #[test]
    fn workflow_schedule_with_buffer_all_max_sets_field() {
        let sched = WorkflowSchedule::new("my_wf", Schedule::Manual).with_buffer_all_max(50);
        assert_eq!(sched.buffer_all_max, 50);
    }

    #[test]
    fn workflow_schedule_all_writable_shards_defaults_to_false() {
        // AC1: opt-in only — the default preserves single-shard placement.
        let sched = WorkflowSchedule::new("my_wf", Schedule::Manual);
        assert!(!sched.all_writable_shards);
    }

    #[test]
    fn workflow_schedule_with_all_writable_shards_sets_field() {
        let sched = WorkflowSchedule::new("my_wf", Schedule::Manual).with_all_writable_shards();
        assert!(sched.all_writable_shards);
    }

    #[test]
    fn workflow_schedule_all_writable_shards_serde_back_compat() {
        // #[serde(default)]: a schedule serialized before issue #796 (no
        // `all_writable_shards` key) must deserialize to `false`.
        let legacy = r#"{
            "workflow_name": "my_wf",
            "dag_name": null,
            "schedule": "Manual",
            "input": null,
            "catchup": false,
            "max_active_runs": 1,
            "paused": false,
            "queue_name": "default"
        }"#;
        let sched: WorkflowSchedule =
            serde_json::from_str(legacy).expect("legacy schedule JSON must deserialize");
        assert!(!sched.all_writable_shards);

        // Round-trip preserves an opted-in schedule.
        let opted = WorkflowSchedule::new("my_wf", Schedule::Manual).with_all_writable_shards();
        let json = serde_json::to_string(&opted).expect("serialize");
        let back: WorkflowSchedule = serde_json::from_str(&json).expect("deserialize");
        assert!(back.all_writable_shards);
    }

    #[test]
    fn overlap_policy_serde_round_trips() {
        let policies = [
            OverlapPolicy::Skip,
            OverlapPolicy::BufferOne,
            OverlapPolicy::BufferAll,
            OverlapPolicy::CancelOther,
            OverlapPolicy::TerminateOther,
            OverlapPolicy::AllowAll,
        ];
        for policy in policies {
            let json = serde_json::to_string(&policy).expect("serialize");
            let back: OverlapPolicy = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, policy, "serde round-trip failed for {policy:?}");
        }
    }

    // ── CronInTimezone schedule ───────────────────────────────────────────────

    #[test]
    fn cron_in_timezone_validate_ok() {
        let sched = Schedule::CronInTimezone {
            expr: "0 9 * * 1-5".to_string(),
            tz: "America/Los_Angeles".to_string(),
        };
        assert!(validate_schedule(&sched).is_ok());
    }

    #[test]
    fn cron_in_timezone_unknown_tz_rejected() {
        let sched = Schedule::CronInTimezone {
            expr: "0 9 * * *".to_string(),
            tz: "Not/ATimezone".to_string(),
        };
        let err = validate_schedule(&sched).unwrap_err();
        assert!(
            err.contains("Not/ATimezone"),
            "error should name the bad timezone: {err}"
        );
    }

    #[test]
    fn cron_in_timezone_invalid_expr_rejected() {
        let sched = Schedule::CronInTimezone {
            expr: "not a cron".to_string(),
            tz: "UTC".to_string(),
        };
        assert!(validate_schedule(&sched).is_err());
    }

    #[test]
    fn zero_interval_schedule_rejected() {
        // A zero interval never advances and would spin a catchup tick forever
        // (issue #484 / Codex #3223); reject it at validation time.
        let err = validate_schedule(&Schedule::Interval(Duration::ZERO)).unwrap_err();
        assert!(
            err.contains("greater than zero"),
            "error should explain the zero-interval rejection: {err}"
        );
        // A positive interval remains valid.
        assert!(validate_schedule(&Schedule::Interval(Duration::from_secs(1))).is_ok());
    }

    #[test]
    fn subsecond_interval_schedule_rejected() {
        // The stored form holds whole seconds only (issue #1967).
        for interval in [
            Duration::from_nanos(1),
            Duration::from_millis(500),
            Duration::from_millis(1_500),
        ] {
            let err = validate_schedule(&Schedule::Interval(interval)).unwrap_err();
            assert!(
                err.contains("whole number of seconds"),
                "{interval:?} must be rejected: {err}"
            );
        }
        assert!(validate_schedule(&Schedule::Interval(Duration::from_secs(u64::MAX))).is_ok());
    }

    #[test]
    fn cron_in_timezone_validate_jitter_applies_cron_rules() {
        let sched = Schedule::CronInTimezone {
            expr: "0 * * * *".to_string(),
            tz: "Europe/London".to_string(),
        };
        assert!(
            validate_jitter(&sched, Duration::from_secs(3601)).is_err(),
            "jitter > 1 hour must be rejected for CronInTimezone"
        );
        assert!(
            validate_jitter(&sched, Duration::from_secs(3600)).is_ok(),
            "jitter == 1 hour must be accepted for CronInTimezone"
        );
    }

    #[test]
    fn cron_in_timezone_serde_round_trips() {
        let sched = Schedule::CronInTimezone {
            expr: "30 9 * * 1-5".to_string(),
            tz: "America/New_York".to_string(),
        };
        let json = serde_json::to_string(&sched).expect("serialize");
        let back: Schedule = serde_json::from_str(&json).expect("deserialize");
        assert!(
            matches!(&back, Schedule::CronInTimezone { expr, tz } if expr == "30 9 * * 1-5" && tz == "America/New_York"),
            "serde round-trip failed: {back:?}"
        );
    }

    // ── compute_retry_delay ───────────────────────────────────────────────────

    #[test]
    fn compute_retry_delay_exponential() {
        let d1 = compute_retry_delay(Duration::from_secs(1), 2.0, Duration::from_secs(300), 1);
        let d2 = compute_retry_delay(Duration::from_secs(1), 2.0, Duration::from_secs(300), 2);
        assert_eq!(d1, Duration::from_secs(1));
        assert_eq!(d2, Duration::from_secs(2));
    }

    #[test]
    fn compute_retry_delay_caps_at_max() {
        let d = compute_retry_delay(
            Duration::from_secs(60),
            2.0,
            Duration::from_secs(120),
            6, // would be 60 * 2^5 = 1920s without cap
        );
        assert_eq!(d, Duration::from_secs(120));
    }

    // ── CatchupPolicy ─────────────────────────────────────────────────────────

    #[test]
    fn catchup_policy_as_str_round_trips() {
        assert_eq!(CatchupPolicy::SkipAll.as_str(), "skip_all");
        assert_eq!(CatchupPolicy::MostRecent.as_str(), "most_recent");
        assert_eq!(
            CatchupPolicy::Window(Duration::from_secs(900)).as_str(),
            "window"
        );
        assert_eq!(CatchupPolicy::Unbounded.as_str(), "unbounded");
    }

    #[test]
    fn catchup_policy_from_db_explicit_modes() {
        assert_eq!(
            CatchupPolicy::from_db(Some("skip_all"), None, false),
            CatchupPolicy::SkipAll,
        );
        assert_eq!(
            CatchupPolicy::from_db(Some("most_recent"), None, false),
            CatchupPolicy::MostRecent,
        );
        assert_eq!(
            CatchupPolicy::from_db(Some("window"), Some(900), false),
            CatchupPolicy::Window(Duration::from_secs(900)),
        );
        assert_eq!(
            CatchupPolicy::from_db(Some("unbounded"), None, false),
            CatchupPolicy::Unbounded,
        );
    }

    #[test]
    fn catchup_policy_from_db_null_falls_back_to_bool() {
        // NULL mode + catchup=false → SkipAll (legacy "no catchup")
        assert_eq!(
            CatchupPolicy::from_db(None, None, false),
            CatchupPolicy::SkipAll,
        );
        // NULL mode + catchup=true → Unbounded (legacy "fire all")
        assert_eq!(
            CatchupPolicy::from_db(None, None, true),
            CatchupPolicy::Unbounded,
        );
    }

    #[test]
    fn catchup_policy_from_db_unknown_mode_falls_back_to_bool() {
        // An unrecognised mode value (written by a future binary) degrades to
        // the bool fallback rather than panicking.
        assert_eq!(
            CatchupPolicy::from_db(Some("fire_three"), None, false),
            CatchupPolicy::SkipAll,
        );
        assert_eq!(
            CatchupPolicy::from_db(Some("fire_three"), None, true),
            CatchupPolicy::Unbounded,
        );
    }

    #[test]
    fn catchup_policy_to_db_columns() {
        assert_eq!(
            CatchupPolicy::SkipAll.to_db_columns(),
            (Some("skip_all"), None)
        );
        assert_eq!(
            CatchupPolicy::MostRecent.to_db_columns(),
            (Some("most_recent"), None)
        );
        assert_eq!(
            CatchupPolicy::Window(Duration::from_secs(900)).to_db_columns(),
            (Some("window"), Some(900)),
        );
        assert_eq!(
            CatchupPolicy::Unbounded.to_db_columns(),
            (Some("unbounded"), None)
        );
    }

    #[test]
    fn catchup_policy_from_user_input_strict() {
        assert_eq!(
            CatchupPolicy::from_user_input("skip_all", None),
            Ok(CatchupPolicy::SkipAll)
        );
        assert_eq!(
            CatchupPolicy::from_user_input("most_recent", None),
            Ok(CatchupPolicy::MostRecent)
        );
        assert_eq!(
            CatchupPolicy::from_user_input("unbounded", None),
            Ok(CatchupPolicy::Unbounded)
        );
        assert_eq!(
            CatchupPolicy::from_user_input("window", Some(900)),
            Ok(CatchupPolicy::Window(Duration::from_secs(900)))
        );
        // Window with no/negative secs clamps to 0 rather than erroring.
        assert_eq!(
            CatchupPolicy::from_user_input("window", None),
            Ok(CatchupPolicy::Window(Duration::from_secs(0)))
        );
        assert_eq!(
            CatchupPolicy::from_user_input("window", Some(-5)),
            Ok(CatchupPolicy::Window(Duration::from_secs(0)))
        );
        // Unknown modes are rejected (unlike the lenient `from_db`).
        assert_eq!(CatchupPolicy::from_user_input("bogus", None), Err("bogus"));
        assert_eq!(CatchupPolicy::from_user_input("", None), Err(""));
    }

    #[test]
    fn catchup_policy_is_catchup_enabled() {
        assert!(!CatchupPolicy::SkipAll.is_catchup_enabled());
        assert!(CatchupPolicy::MostRecent.is_catchup_enabled());
        assert!(CatchupPolicy::Window(Duration::from_secs(60)).is_catchup_enabled());
        assert!(CatchupPolicy::Unbounded.is_catchup_enabled());
    }

    #[test]
    fn workflow_schedule_catchup_policy_defaults_to_none() {
        let sched = WorkflowSchedule::new("my_wf", Schedule::Manual);
        assert!(sched.catchup_policy.is_none());
    }

    #[test]
    fn workflow_schedule_with_catchup_policy_sets_field() {
        let sched = WorkflowSchedule::new("my_wf", Schedule::Manual)
            .with_catchup_policy(CatchupPolicy::MostRecent);
        assert_eq!(sched.catchup_policy, Some(CatchupPolicy::MostRecent));
    }

    #[test]
    fn workflow_schedule_with_catchup_window_sets_window_variant() {
        let sched = WorkflowSchedule::new("my_wf", Schedule::Manual)
            .with_catchup_window(Duration::from_secs(3600));
        assert_eq!(
            sched.catchup_policy,
            Some(CatchupPolicy::Window(Duration::from_secs(3600))),
        );
    }

    #[test]
    fn catchup_policy_window_missing_secs_defaults_to_zero() {
        // window mode with NULL catchup_window_secs is safe: fires slot at exactly now.
        let p = CatchupPolicy::from_db(Some("window"), None, false);
        assert_eq!(p, CatchupPolicy::Window(Duration::ZERO));
    }
}

#[test]
fn compute_retry_delay_attempt_zero() {
    let d = compute_retry_delay(Duration::from_secs(1), 2.0, Duration::from_secs(300), 0);
    assert_eq!(d, Duration::from_secs(1));
}

#[test]
fn compute_retry_delay_zero_initial_with_overflowing_power() {
    // 2.0^4999 is infinite, and 0 * inf is NaN. The clamp turns NaN into 0.
    let d = compute_retry_delay(Duration::ZERO, 2.0, Duration::from_secs(300), 5000);
    assert_eq!(d, Duration::ZERO);
}

#[test]
fn compute_retry_delay_negative_nan() {
    let d = compute_retry_delay(
        Duration::from_secs(1),
        f64::NAN,
        Duration::from_secs(300),
        2,
    );
    assert_eq!(d, Duration::from_secs(0));

    let d2 = compute_retry_delay(Duration::from_secs(1), -1.0, Duration::from_secs(300), 2);
    assert_eq!(d2, Duration::from_secs(0));
}

/// Kani proofs of the retry-delay bounds (issue #1819).
///
/// The `kani` CI job runs them. See `docs/testing/formal-methods.md`.
///
/// The jitter proofs replace `mix64` with a stub that returns any `u64`. The
/// bounds hold for every stub value, so they hold for the real mixer. The
/// seed is any `u64` too, and `mix64` is a bijection, so the stub loses no
/// case. With the real mixer, CBMC does not finish.
///
/// The proofs work in nanoseconds. A proof through `Duration` must relate
/// `as_nanos` to `from_nanos`, and CBMC does not finish that either.
#[cfg(kani)]
mod kani_proofs {
    use super::*;

    fn any_mix(_x: u64) -> u64 {
        kani::any()
    }

    /// The jitter draw stays in `[lo, hi]` for every seed.
    #[kani::proof]
    #[kani::stub(mix64, any_mix)]
    fn uniform_inclusive_stays_in_range() {
        let lo: u64 = kani::any();
        let hi: u64 = kani::any();
        kani::assume(lo <= hi);
        let v = uniform_inclusive(kani::any(), lo, hi);
        assert!(lo <= v && v <= hi);
        kani::cover!(v == hi);
    }

    /// Full jitter never exceeds its base.
    #[kani::proof]
    #[kani::stub(mix64, any_mix)]
    fn full_jitter_is_at_most_base() {
        let hi: u64 = kani::any();
        assert!(full_jitter_nanos(hi, kani::any()) <= hi);
    }

    /// Equal jitter stays in `[base/2, base]`. A loop with no attempt cap
    /// therefore cannot become a hot loop.
    #[kani::proof]
    #[kani::stub(mix64, any_mix)]
    fn equal_jitter_stays_in_upper_half() {
        let hi: u64 = kani::any();
        let v = equal_jitter_nanos(hi, kani::any());
        assert!(hi / 2 <= v && v <= hi);
    }

    /// The backoff never panics for any coefficient that is not NaN.
    ///
    /// The final `min` makes `d <= max` true by construction. The value of the
    /// proof is that no float conversion panics on the way.
    ///
    /// CBMC reports any NaN result as an error, but the code handles NaN on
    /// purpose. The proof therefore excludes a NaN coefficient and an initial
    /// interval of 0, because `0 * inf` is NaN. The unit tests
    /// `compute_retry_delay_negative_nan` and
    /// `compute_retry_delay_zero_initial_with_overflowing_power` pin those cases.
    #[kani::proof]
    fn retry_delay_never_exceeds_max_interval() {
        let coefficient: f64 = kani::any();
        kani::assume(!coefficient.is_nan());
        let initial = Duration::from_millis(u64::from(kani::any::<u32>()));
        kani::assume(!initial.is_zero());
        let max = Duration::from_millis(u64::from(kani::any::<u32>()));
        let d = compute_retry_delay(initial, coefficient, max, kani::any());
        assert!(d <= max);
    }
}
