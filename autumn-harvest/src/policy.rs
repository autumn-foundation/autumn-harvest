//! Retry policies, trigger rules, and scheduling types.

use std::time::Duration;

use croner::Cron;
use serde::{Deserialize, Serialize};

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
    /// 1. Match `error_type` first — the structured class on `ActivityFailure`,
    ///    stable across log-format changes.
    /// 2. Fall back to a full-string match on the raw error payload — the
    ///    legacy back-compat path for activities returning `Err(String)`.
    #[must_use]
    pub fn is_non_retryable(&self, error_type: &str, raw_error: &str) -> bool {
        self.non_retryable_errors
            .iter()
            .any(|nr| nr == error_type || nr == raw_error)
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
}

impl WorkflowSchedule {
    /// Create a new workflow schedule with sensible defaults.
    ///
    /// Defaults: `input = null`, `catchup = false`, `max_active_runs = 1`,
    /// `paused = false`, `queue_name = "default"`.
    #[must_use]
    pub fn new(workflow_name: impl Into<String>, schedule: Schedule) -> Self {
        Self {
            workflow_name: workflow_name.into(),
            schedule,
            input: serde_json::Value::Null,
            catchup: false,
            max_active_runs: 1,
            paused: false,
            queue_name: "default".to_string(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

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
