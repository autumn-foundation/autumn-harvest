//! Retry policies, trigger rules, and scheduling types.

use std::time::Duration;

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
    Duration::from_secs_f64(secs.min(max_interval.as_secs_f64()))
}

/// How an activity failure is retried.
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
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::exponential(3, Duration::from_secs(1))
    }
}

/// Status of a completed DAG task, used by trigger rules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskStatus {
    Succeeded,
    Failed,
    Skipped,
}

/// When a DAG task with multiple upstreams should execute.
///
/// All rules vacuously fire when `upstream_statuses` is empty (no dependencies).
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
    #[must_use]
    pub fn should_run(&self, upstream_statuses: &[TaskStatus]) -> bool {
        match self {
            Self::AllSuccess => upstream_statuses
                .iter()
                .all(|s| *s == TaskStatus::Succeeded),
            Self::AllDone => true,
            Self::OneSuccess => upstream_statuses.contains(&TaskStatus::Succeeded),
            Self::OneFailed => upstream_statuses.contains(&TaskStatus::Failed),
            Self::AllFailed => {
                !upstream_statuses.is_empty()
                    && upstream_statuses.iter().all(|s| *s == TaskStatus::Failed)
            }
            Self::Manual => false,
        }
    }
}

/// DAG/workflow execution schedule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Schedule {
    /// Standard cron expression (e.g., `"0 2 * * *"` for daily at 2 AM).
    Cron(String),
    /// Fixed interval from the end of the previous run.
    Interval(Duration),
    /// Only runs when triggered manually via API.
    Manual,
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
