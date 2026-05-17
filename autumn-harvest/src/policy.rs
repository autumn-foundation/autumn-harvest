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
pub struct RetryPolicy {
    /// Maximum number of attempts (including the first). 1 = no retries.
    pub max_attempts: u32,
    /// Delay before the first retry.
    pub initial_interval: Duration,
    /// Multiplier applied after each retry (`1.0` = fixed delay).
    pub backoff_coefficient: f64,
    /// Upper bound on delay between retries.
    pub max_interval: Duration,
    /// Error type names that must not be retried.
    pub non_retryable_errors: Vec<String>,
}

impl RetryPolicy {
    /// Exponential backoff: doubles each retry, capped at 5 minutes.
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
        }
    }

    /// Fixed delay: same interval every retry.
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
        }
    }

    /// Returns the delay before the given attempt, or `None` if no more retries remain.
    ///
    /// `attempt` is 1-based: 1 = first retry (after the initial failure).
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    /// use autumn_harvest::policy::RetryPolicy;
    ///
    /// let policy = RetryPolicy::exponential(3, Duration::from_secs(1));
    /// assert_eq!(policy.next_delay(1), Some(Duration::from_secs(1)));
    /// assert_eq!(policy.next_delay(3), None); // attempt >= max_attempts
    /// ```
    #[must_use]
    pub fn next_delay(&self, attempt: u32) -> Option<Duration> {
        if attempt >= self.max_attempts {
            return None;
        }
        Some(compute_retry_delay(
            self.initial_interval,
            self.backoff_coefficient,
            self.max_interval,
            attempt,
        ))
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
/// All rules vacuously fire when `upstream_statuses` is empty (no dependencies).
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
    /// Never auto-trigger; must be triggered manually.
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
    Cron(String),
    /// Fixed interval from the end of the previous run.
    Interval(Duration),
    /// Only runs when triggered manually via API.
    Manual,
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
///
/// The default is [`Skip`](OverlapPolicy::Skip), which preserves pre-existing behaviour.
///
/// `BufferOne` / `BufferAll` store pending firings durably in `harvest_schedules`
/// so they survive scheduler restarts and leader handoffs.
///
/// `CancelOther` / `TerminateOther` require the cancellation contract from
/// issue #238, which is implemented in this codebase.
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
}

impl OverlapPolicy {
    /// The `snake_case` string used to store this policy in `harvest_schedules`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Skip => "skip",
            Self::BufferOne => "buffer_one",
            Self::BufferAll => "buffer_all",
            Self::CancelOther => "cancel_other",
            Self::TerminateOther => "terminate_other",
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
            _ => Err(s),
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
    /// Defaults to `Duration::ZERO` (no jitter — today's behaviour).
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
}

const fn default_buffer_all_max() -> u32 {
    100
}

impl WorkflowSchedule {
    /// Create a new workflow schedule with sensible defaults.
    ///
    /// Defaults: `input = null`, `catchup = false`, `max_active_runs = 1`,
    /// `paused = false`, `queue_name = "default"`, `overlap_policy = Skip`,
    /// `buffer_all_max = 100`.
    #[must_use]
    pub fn new(workflow_name: impl Into<String>, schedule: Schedule) -> Self {
        Self {
            workflow_name: workflow_name.into(),
            dag_name: None,
            schedule,
            input: serde_json::Value::Null,
            catchup: false,
            max_active_runs: 1,
            paused: false,
            queue_name: "default".to_string(),
            jitter: Duration::ZERO,
            overlap_policy: OverlapPolicy::Skip,
            buffer_all_max: 100,
        }
    }

    /// Set the JSON input passed to each scheduled run.
    #[must_use]
    pub fn with_input(mut self, input: serde_json::Value) -> Self {
        self.input = input;
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
    #[must_use]
    pub const fn with_jitter(mut self, jitter: Duration) -> Self {
        self.jitter = jitter;
        self
    }
}

/// Validate a [`Schedule`] value, returning an error string if it is invalid.
///
/// For [`Schedule::Cron`] expressions this parses the expression using
/// `croner` (5-field or 6-field with seconds). For other variants the schedule
/// is always valid.
///
/// # Errors
///
/// Returns a human-readable error string if the cron expression is
/// syntactically invalid.
pub fn validate_schedule(schedule: &Schedule) -> Result<(), String> {
    if let Schedule::Cron(expr) = schedule {
        Cron::new(expr)
            .with_seconds_optional()
            .parse()
            .map(|_| ())
            .map_err(|e| format!("invalid cron expression '{expr}': {e}"))
    } else {
        Ok(())
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
        Schedule::Cron(_) => {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    // ── Schedule jitter ───────────────────────────────────────────────────────

    #[test]
    fn workflow_schedule_jitter_defaults_to_zero() {
        let sched = WorkflowSchedule::new("my_workflow", Schedule::Manual);
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
    fn exponential_backoff_doubles() {
        let policy = RetryPolicy::exponential(5, Duration::from_secs(1));
        assert_eq!(policy.next_delay(1), Some(Duration::from_secs(1)));
        assert_eq!(policy.next_delay(2), Some(Duration::from_secs(2)));
        assert_eq!(policy.next_delay(3), Some(Duration::from_secs(4)));
    }

    #[test]
    fn fixed_backoff_stays_constant() {
        let policy = RetryPolicy::fixed(3, Duration::from_secs(5));
        assert_eq!(policy.next_delay(1), Some(Duration::from_secs(5)));
        assert_eq!(policy.next_delay(2), Some(Duration::from_secs(5)));
    }

    #[test]
    fn no_retry_after_max_attempts() {
        let policy = RetryPolicy::exponential(3, Duration::from_secs(1));
        assert_eq!(policy.next_delay(3), None);
    }

    #[test]
    fn exponential_caps_at_max_interval() -> Result<(), String> {
        let policy = RetryPolicy {
            max_attempts: 10,
            initial_interval: Duration::from_secs(60),
            backoff_coefficient: 2.0,
            max_interval: Duration::from_secs(120),
            non_retryable_errors: vec![],
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
    fn trigger_rule_vacuous_empty_slice() {
        // All rules fire vacuously when there are no upstreams
        assert!(TriggerRule::AllSuccess.should_run(&[]));
        assert!(TriggerRule::AllDone.should_run(&[]));
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
    fn overlap_policy_serde_round_trips() {
        let policies = [
            OverlapPolicy::Skip,
            OverlapPolicy::BufferOne,
            OverlapPolicy::BufferAll,
            OverlapPolicy::CancelOther,
            OverlapPolicy::TerminateOther,
        ];
        for policy in policies {
            let json = serde_json::to_string(&policy).expect("serialize");
            let back: OverlapPolicy = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, policy, "serde round-trip failed for {policy:?}");
        }
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
}

#[test]
fn compute_retry_delay_attempt_zero() {
    let d = compute_retry_delay(Duration::from_secs(1), 2.0, Duration::from_secs(300), 0);
    assert_eq!(d, Duration::from_secs(1));
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
