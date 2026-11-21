//! Retry envelope analysis for DAG definitions.
//!
//! The critical path assumes that every task succeeds on its first attempt.
//! A failing task runs again after a backoff, so its real cost can be many
//! times its nominal duration. This module asks how long the run takes when
//! every task uses all of its retries.
//!
//! Each task gets the worst case of [`crate::retry_envelope::analyze`]. A
//! forward pass of the critical path method then finds the worst-case makespan
//! and the worst-case critical path. That path can differ from the nominal
//! path. A short task with a long retry budget can overtake a long task with no
//! retries.
//!
//! The module is pure. It sends no SQL and reads no clock.

use crate::dag::DagDefinition;
use crate::dag_slack::to_duration;
use crate::policy::RetryPolicy;
use crate::retry_envelope::worst_case_without_deadline;
use std::collections::HashMap;
use std::time::Duration;

/// The retry figures of one task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRetryEnvelope {
    /// Topological index of the task.
    pub index: usize,
    /// Name of the task activity.
    pub name: String,
    /// The longest run of one attempt.
    pub attempt_bound: Duration,
    /// The attempts that the retry policy of the task allows.
    pub attempts: u32,
    /// The longest time from the task start to its final outcome.
    ///
    /// It is `None` when the retry list is too long to bound.
    pub worst_case: Option<Duration>,
}

impl TaskRetryEnvelope {
    /// The extra time that retries can add to the task.
    #[must_use]
    pub fn inflation(&self) -> Option<Duration> {
        self.worst_case
            .map(|total| total.saturating_sub(self.attempt_bound))
    }
}

/// Result of a DAG retry envelope analysis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DagRetryEnvelopeReport {
    /// The makespan when every task succeeds on its first attempt.
    pub nominal_makespan: Duration,
    /// The critical path when every task succeeds on its first attempt.
    pub nominal_path: Vec<usize>,
    /// The makespan when every task uses all of its retries.
    ///
    /// It is `None` when one task has no bound.
    pub worst_case_makespan: Option<Duration>,
    /// The critical path when every task uses all of its retries.
    ///
    /// It is empty when one task has no bound.
    pub worst_case_path: Vec<usize>,
    /// One entry per task, in topological index order.
    pub tasks: Vec<TaskRetryEnvelope>,
}

impl DagRetryEnvelopeReport {
    /// The extra time that retries can add to the whole run.
    #[must_use]
    pub fn inflation(&self) -> Option<Duration> {
        self.worst_case_makespan
            .map(|total| total.saturating_sub(self.nominal_makespan))
    }

    /// Return `true` when retries move the critical path to other tasks.
    #[must_use]
    pub fn path_shifts(&self) -> bool {
        !self.worst_case_path.is_empty() && self.worst_case_path != self.nominal_path
    }

    /// The indices of the tasks that have no worst-case bound.
    #[must_use]
    pub fn unbounded_indices(&self) -> Vec<usize> {
        self.tasks
            .iter()
            .filter(|task| task.worst_case.is_none())
            .map(|task| task.index)
            .collect()
    }
}

/// Analyzer that computes the retry envelope of a DAG.
pub struct DagRetryEnvelope {
    dag: DagDefinition,
    activity_durations: HashMap<String, Duration>,
    default_duration: Duration,
    fallback_policy: Option<RetryPolicy>,
}

impl DagRetryEnvelope {
    /// Create an analyzer. Unmocked tasks default to one second.
    #[must_use]
    pub fn new(dag: DagDefinition) -> Self {
        Self {
            dag,
            activity_durations: HashMap::new(),
            default_duration: Duration::from_secs(1),
            fallback_policy: None,
        }
    }

    /// Set the duration of an activity that has no mocked duration.
    #[must_use]
    pub const fn with_default_duration(mut self, duration: Duration) -> Self {
        self.default_duration = duration;
        self
    }

    /// Set the expected duration of one attempt of an activity by name.
    #[must_use]
    pub fn mock_duration(mut self, name: &str, duration: Duration) -> Self {
        self.activity_durations.insert(name.to_string(), duration);
        self
    }

    /// Set the retry policy of the tasks that set none.
    #[must_use]
    pub fn with_fallback_policy(mut self, policy: RetryPolicy) -> Self {
        self.fallback_policy = Some(policy);
        self
    }

    /// Compute the nominal and the worst-case makespan and paths.
    ///
    /// A `start_to_close` timeout on a task overrides any mocked duration, as in
    /// `DagSlackAnalyzer`. A DAG task sets no `schedule_to_close`, so no deadline
    /// cuts the retries short.
    #[must_use]
    pub fn analyze(&self) -> DagRetryEnvelopeReport {
        let tasks: Vec<TaskRetryEnvelope> = self
            .dag
            .tasks()
            .iter()
            .enumerate()
            .map(|(index, task)| {
                let attempt_bound = task
                    .start_to_close
                    .or_else(|| self.activity_durations.get(&task.activity_name).copied())
                    .unwrap_or(self.default_duration);
                let (attempts, worst_case) = task
                    .retry_policy
                    .as_ref()
                    .or(self.fallback_policy.as_ref())
                    .map_or((1, Some(attempt_bound)), |policy| {
                        worst_case_without_deadline(policy, Some(attempt_bound))
                    });
                TaskRetryEnvelope {
                    index,
                    name: task.activity_name.clone(),
                    attempt_bound,
                    attempts,
                    worst_case,
                }
            })
            .collect();

        let nominal: Vec<u128> = tasks.iter().map(|t| t.attempt_bound.as_nanos()).collect();
        let (nominal_makespan, nominal_path) = self.longest_path(&nominal);

        let worst: Option<Vec<u128>> = tasks
            .iter()
            .map(|t| t.worst_case.map(|d| d.as_nanos()))
            .collect();
        let (worst_case_makespan, worst_case_path) =
            worst.map_or((None, Vec::new()), |durations| {
                let (makespan, path) = self.longest_path(&durations);
                (Some(to_duration(makespan)), path)
            });

        DagRetryEnvelopeReport {
            nominal_makespan: to_duration(nominal_makespan),
            nominal_path,
            worst_case_makespan,
            worst_case_path,
            tasks,
        }
    }

    /// Find the makespan and one longest path for the given task durations.
    ///
    /// A tie goes to the lower index, so the result is deterministic.
    fn longest_path(&self, durations: &[u128]) -> (u128, Vec<usize>) {
        let tasks = self.dag.tasks();
        let mut finish = vec![0u128; tasks.len()];
        let mut previous: Vec<Option<usize>> = vec![None; tasks.len()];
        for level in self.dag.execution_levels() {
            for &i in level {
                let mut start = 0u128;
                for &up in &tasks[i].upstreams {
                    let candidate = finish[up];
                    let better = previous[i]
                        .is_none_or(|best| candidate > start || (candidate == start && up < best));
                    if better {
                        start = candidate;
                        previous[i] = Some(up);
                    }
                }
                finish[i] = start.saturating_add(durations[i]);
            }
        }

        let mut end: Option<usize> = None;
        for i in 0..tasks.len() {
            if end.is_none_or(|best| finish[i] > finish[best]) {
                end = Some(i);
            }
        }
        let Some(last) = end else {
            return (0, Vec::new());
        };

        let mut path = vec![last];
        let mut cursor = last;
        while let Some(up) = previous[cursor] {
            path.push(up);
            cursor = up;
        }
        path.reverse();
        (finish[last], path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagBuilder;
    use crate::policy::JitterPolicy;

    fn a() {}
    fn b() {}
    fn c() {}
    fn d() {}

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    fn exact(max_attempts: u32, initial: u64) -> RetryPolicy {
        RetryPolicy::exponential(max_attempts, secs(initial)).with_jitter(JitterPolicy::None)
    }

    #[test]
    fn empty_dag_has_zero_makespans() {
        let report = DagRetryEnvelope::new(DagBuilder::new().build().unwrap()).analyze();
        assert_eq!(report.nominal_makespan, Duration::ZERO);
        assert_eq!(report.worst_case_makespan, Some(Duration::ZERO));
        assert_eq!(report.inflation(), Some(Duration::ZERO));
        assert_eq!(report.nominal_path, Vec::<usize>::new());
        assert!(!report.path_shifts());
    }

    #[test]
    fn task_without_policy_runs_once() {
        let mut builder = DagBuilder::new();
        let _ta = builder.activity(a).start_to_close(secs(5));
        let report = DagRetryEnvelope::new(builder.build().unwrap()).analyze();
        let task = &report.tasks[0];
        assert_eq!(task.attempts, 1);
        assert_eq!(task.worst_case, Some(secs(5)));
        assert_eq!(task.inflation(), Some(Duration::ZERO));
        assert_eq!(report.worst_case_makespan, Some(secs(5)));
    }

    #[test]
    fn retries_inflate_a_chain() {
        let mut builder = DagBuilder::new();
        let ta = builder
            .activity(a)
            .start_to_close(secs(10))
            .retry(exact(3, 1));
        let _tb = builder.activity(b).upstream(&ta);
        let report = DagRetryEnvelope::new(builder.build().unwrap())
            .mock_duration("b", secs(2))
            .analyze();
        // Three 10 s runs and backoffs of 1 s and 2 s.
        assert_eq!(report.tasks[0].worst_case, Some(secs(33)));
        assert_eq!(report.tasks[0].inflation(), Some(secs(23)));
        assert_eq!(report.nominal_makespan, secs(12));
        assert_eq!(report.worst_case_makespan, Some(secs(35)));
        assert_eq!(report.inflation(), Some(secs(23)));
        assert_eq!(report.worst_case_path, vec![0, 1]);
        assert!(!report.path_shifts());
    }

    /// Diamond: a -> b (10 s, no retries) -> d, a -> c (3 s, 4 attempts) -> d.
    #[test]
    fn retries_move_the_critical_path() {
        let mut builder = DagBuilder::new();
        let ta = builder.activity(a);
        let tb = builder.activity(b).upstream(&ta);
        let tc = builder.activity(c).upstream(&ta).retry(exact(4, 1));
        let _td = builder.activity(d).upstream(&tb).upstream(&tc);
        let report = DagRetryEnvelope::new(builder.build().unwrap())
            .mock_duration("b", secs(10))
            .mock_duration("c", secs(3))
            .analyze();
        assert_eq!(report.nominal_path, vec![0, 1, 3]);
        assert_eq!(report.nominal_makespan, secs(12));
        // Four 3 s runs and backoffs of 1 s, 2 s and 4 s.
        assert_eq!(report.tasks[2].worst_case, Some(secs(19)));
        assert_eq!(report.worst_case_path, vec![0, 2, 3]);
        assert_eq!(report.worst_case_makespan, Some(secs(21)));
        assert!(report.path_shifts());
    }

    #[test]
    fn fallback_policy_covers_tasks_without_one() {
        let mut builder = DagBuilder::new();
        let _ta = builder.activity(a);
        let _tb = builder.activity(b).retry(exact(1, 1));
        let report = DagRetryEnvelope::new(builder.build().unwrap())
            .with_default_duration(secs(2))
            .with_fallback_policy(exact(2, 5))
            .analyze();
        assert_eq!(report.tasks[0].attempts, 2);
        assert_eq!(report.tasks[0].worst_case, Some(secs(9)));
        // An explicit policy wins over the fallback.
        assert_eq!(report.tasks[1].attempts, 1);
        assert_eq!(report.worst_case_path, vec![0]);
    }

    #[test]
    fn jitter_uses_the_longest_delay() {
        let mut builder = DagBuilder::new();
        let _ta = builder
            .activity(a)
            .start_to_close(secs(1))
            .retry(RetryPolicy::exponential(2, secs(4)).with_jitter(JitterPolicy::Full));
        let report = DagRetryEnvelope::new(builder.build().unwrap()).analyze();
        assert_eq!(report.tasks[0].worst_case, Some(secs(6)));
    }

    #[test]
    fn an_unlistable_retry_budget_has_no_bound() {
        let mut builder = DagBuilder::new();
        let ta = builder.activity(a).retry(exact(u32::MAX, 0));
        let _tb = builder.activity(b).upstream(&ta);
        let report = DagRetryEnvelope::new(builder.build().unwrap()).analyze();
        assert_eq!(report.tasks[0].worst_case, None);
        assert_eq!(report.unbounded_indices(), vec![0]);
        assert_eq!(report.worst_case_makespan, None);
        assert_eq!(report.inflation(), None);
        assert_eq!(report.worst_case_path, Vec::<usize>::new());
        assert!(!report.path_shifts());
        assert_eq!(report.nominal_makespan, secs(2));
    }
}
