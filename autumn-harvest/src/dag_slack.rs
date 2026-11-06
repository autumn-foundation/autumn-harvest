//! Slack (float) analysis for DAG definitions.
//!
//! [`crate::critical_path::CriticalPathAnalyzer`] names the one longest path.
//! This module answers the follow-up question: how late may each other task
//! start before it delays the whole DAG? The classic critical path method
//! calls that gap the task's *slack*. A task with zero slack is critical.
//! A task with positive slack is a safe place to spend a cheaper queue, a
//! lower priority or a longer retry backoff.

use crate::dag::DagDefinition;
use std::collections::HashMap;
use std::time::Duration;

/// Timing window of one task in a [`SlackReport`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskSlack {
    /// Topological index of the task.
    pub index: usize,
    /// Name of the task activity.
    pub activity_name: String,
    /// Estimated duration of the task.
    pub duration: Duration,
    /// Earliest time the task can start, measured from the DAG start.
    pub earliest_start: Duration,
    /// Latest time the task can start without delaying the DAG.
    pub latest_start: Duration,
    /// Time the task can slip without delaying the DAG.
    pub slack: Duration,
}

impl TaskSlack {
    /// Return true when any delay of this task delays the whole DAG.
    #[must_use]
    pub const fn is_critical(&self) -> bool {
        self.slack.is_zero()
    }
}

/// Result of a slack analysis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlackReport {
    /// Estimated duration of the whole DAG.
    pub total_duration: Duration,
    /// One entry per task, in topological index order.
    pub tasks: Vec<TaskSlack>,
}

impl SlackReport {
    /// Indices of the tasks with zero slack, in ascending order.
    #[must_use]
    pub fn critical_indices(&self) -> Vec<usize> {
        self.tasks
            .iter()
            .filter(|task| task.is_critical())
            .map(|task| task.index)
            .collect()
    }

    /// Tasks sorted by descending slack. Ties keep index order.
    #[must_use]
    pub fn most_flexible(&self) -> Vec<&TaskSlack> {
        let mut sorted: Vec<&TaskSlack> = self.tasks.iter().collect();
        sorted.sort_by_key(|task| std::cmp::Reverse(task.slack));
        sorted
    }
}

/// Computes per-task slack for a DAG with mocked activity durations.
pub struct SlackAnalyzer {
    dag: DagDefinition,
    activity_durations: HashMap<String, Duration>,
    default_duration: Duration,
}

impl SlackAnalyzer {
    /// Create an analyzer. Unmocked activities take one second.
    #[must_use]
    pub fn new(dag: DagDefinition) -> Self {
        Self {
            dag,
            activity_durations: HashMap::new(),
            default_duration: Duration::from_secs(1),
        }
    }

    /// Set the duration of activities with no mocked duration.
    #[must_use]
    pub const fn with_default_duration(mut self, duration: Duration) -> Self {
        self.default_duration = duration;
        self
    }

    /// Mock the expected duration of an activity by name.
    #[must_use]
    pub fn mock_duration(mut self, name: &str, duration: Duration) -> Self {
        self.activity_durations.insert(name.to_string(), duration);
        self
    }

    /// Run the forward and backward passes.
    ///
    /// Arithmetic uses `u128` nanoseconds so long chains cannot overflow.
    /// A `start_to_close` timeout on a task wins over any mocked duration,
    /// as in the critical path analyzer.
    #[must_use]
    pub fn analyze(&self) -> SlackReport {
        let tasks = self.dag.tasks();
        let durations: Vec<u128> = tasks
            .iter()
            .map(|task| {
                task.start_to_close
                    .or_else(|| self.activity_durations.get(&task.activity_name).copied())
                    .unwrap_or(self.default_duration)
                    .as_nanos()
            })
            .collect();

        // Forward pass: earliest start is the latest finish among upstreams.
        let mut earliest = vec![0u128; tasks.len()];
        for level in self.dag.execution_levels() {
            for &i in level {
                earliest[i] = tasks[i]
                    .upstreams
                    .iter()
                    .map(|&up| earliest[up].saturating_add(durations[up]))
                    .max()
                    .unwrap_or(0);
            }
        }
        let total = earliest
            .iter()
            .zip(&durations)
            .map(|(start, dur)| start.saturating_add(*dur))
            .max()
            .unwrap_or(0);

        // Backward pass: latest finish is the earliest latest-start among
        // successors. A sink may finish as late as the DAG total.
        let mut latest_finish = vec![total; tasks.len()];
        for level in self.dag.execution_levels().iter().rev() {
            for &i in level {
                let latest_start = latest_finish[i].saturating_sub(durations[i]);
                for &up in &tasks[i].upstreams {
                    latest_finish[up] = latest_finish[up].min(latest_start);
                }
            }
        }

        let entries = tasks
            .iter()
            .enumerate()
            .map(|(i, task)| {
                let latest_start = latest_finish[i].saturating_sub(durations[i]);
                TaskSlack {
                    index: i,
                    activity_name: task.activity_name.clone(),
                    duration: nanos_to_duration(durations[i]),
                    earliest_start: nanos_to_duration(earliest[i]),
                    latest_start: nanos_to_duration(latest_start),
                    slack: nanos_to_duration(latest_start.saturating_sub(earliest[i])),
                }
            })
            .collect();

        SlackReport {
            total_duration: nanos_to_duration(total),
            tasks: entries,
        }
    }
}

/// Convert nanoseconds to a `Duration`, saturating at `Duration::MAX`.
fn nanos_to_duration(nanos: u128) -> Duration {
    u64::try_from(nanos / 1_000_000_000).map_or(Duration::MAX, |secs| {
        // The remainder is below 1e9, so it fits in `u32`.
        Duration::new(secs, (nanos % 1_000_000_000) as u32)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::critical_path::CriticalPathAnalyzer;
    use crate::dag::DagBuilder;

    fn activity_a() {}
    fn activity_b() {}
    fn activity_c() {}
    fn activity_d() {}

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    fn diamond() -> DagDefinition {
        let mut builder = DagBuilder::new();
        let start = builder.activity(activity_a);
        let short = builder.activity(activity_b).upstream(&start);
        let long = builder.activity(activity_c).upstream(&start);
        let _end = builder
            .activity(activity_d)
            .upstream(&short)
            .upstream(&long);
        builder.build().unwrap()
    }

    #[test]
    fn empty_dag_has_no_tasks_and_zero_total() {
        let report = SlackAnalyzer::new(DagBuilder::new().build().unwrap()).analyze();
        assert_eq!(report.total_duration, Duration::ZERO);
        assert_eq!(report.tasks.len(), 0);
    }

    #[test]
    fn short_branch_has_slack_and_long_branch_has_none() {
        let report = SlackAnalyzer::new(diamond())
            .mock_duration("activity_a", secs(1))
            .mock_duration("activity_b", secs(2))
            .mock_duration("activity_c", secs(10))
            .mock_duration("activity_d", secs(1))
            .analyze();

        assert_eq!(report.total_duration, secs(12));
        assert_eq!(report.critical_indices(), vec![0, 2, 3]);
        let short = &report.tasks[1];
        assert_eq!(short.earliest_start, secs(1));
        assert_eq!(short.latest_start, secs(9));
        assert_eq!(short.slack, secs(8));
        assert!(!short.is_critical());
    }

    #[test]
    fn zero_slack_tasks_match_the_critical_path_analyzer() {
        let build = || {
            [
                ("activity_a", 3),
                ("activity_b", 4),
                ("activity_c", 7),
                ("activity_d", 2),
            ]
        };
        let mut slack = SlackAnalyzer::new(diamond());
        let mut cp = CriticalPathAnalyzer::new(diamond());
        for (name, n) in build() {
            slack = slack.mock_duration(name, secs(n));
            cp = cp.mock_duration(name, secs(n));
        }
        let report = slack.analyze();
        let path = cp.analyze();
        assert_eq!(report.total_duration, path.total_duration);
        assert_eq!(report.critical_indices(), path.path_indices);
    }

    #[test]
    fn tied_branches_are_both_critical() {
        let report = SlackAnalyzer::new(diamond())
            .with_default_duration(secs(5))
            .analyze();
        assert_eq!(report.critical_indices(), vec![0, 1, 2, 3]);
    }

    #[test]
    fn most_flexible_lists_the_largest_slack_first() {
        let report = SlackAnalyzer::new(diamond())
            .mock_duration("activity_b", secs(1))
            .mock_duration("activity_c", secs(6))
            .analyze();
        let first = report.most_flexible()[0];
        assert_eq!(first.activity_name, "activity_b");
        assert_eq!(first.slack, secs(5));
    }

    #[test]
    fn start_to_close_overrides_a_mocked_duration() {
        let mut builder = DagBuilder::new();
        let _a = builder.activity(activity_a).start_to_close(secs(42));
        let report = SlackAnalyzer::new(builder.build().unwrap())
            .mock_duration("activity_a", secs(1))
            .analyze();
        assert_eq!(report.total_duration, secs(42));
    }
}
