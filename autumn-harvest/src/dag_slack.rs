//! Slack analysis for DAG definitions.
//!
//! [`CriticalPathAnalyzer`](crate::critical_path::CriticalPathAnalyzer) names
//! the tasks that set the total duration. This module answers the next
//! question: how late can each other task run before it delays the whole DAG?
//! That delay budget is the *slack* of the task. A task with zero slack is
//! critical. A task with positive slack can start late, run on a slower
//! queue, or take more retries, up to its slack.
//!
//! The analysis is a pure function of the DAG and the durations. It reads no
//! database, adds no `WorkflowEvent` variant and has no replay impact.

use std::collections::HashMap;
use std::time::Duration;

use crate::dag::DagDefinition;

/// Timing window of one task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskSlack {
    /// Topological index of the task.
    pub index: usize,
    /// Activity name of the task.
    pub name: String,
    /// Earliest time that the task can start.
    pub earliest_start: Duration,
    /// Latest time that the task can start without a longer total duration.
    pub latest_start: Duration,
    /// Delay budget: `latest_start - earliest_start`.
    pub slack: Duration,
    /// True when the slack is zero.
    pub is_critical: bool,
}

/// Slack analysis of a whole DAG.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlackReport {
    /// Total duration of the DAG.
    pub total_duration: Duration,
    /// One entry per task, in index order.
    pub tasks: Vec<TaskSlack>,
}

impl SlackReport {
    /// Indices of the tasks with zero slack.
    #[must_use]
    pub fn critical_indices(&self) -> Vec<usize> {
        self.tasks
            .iter()
            .filter(|t| t.is_critical)
            .map(|t| t.index)
            .collect()
    }

    /// The task with the most slack, if any. The first one wins a tie.
    #[must_use]
    pub fn most_flexible(&self) -> Option<&TaskSlack> {
        self.tasks
            .iter()
            .fold(None, |best: Option<&TaskSlack>, t| match best {
                Some(b) if b.slack >= t.slack => Some(b),
                _ => Some(t),
            })
    }
}

/// Computes the slack of each task of a DAG.
pub struct SlackAnalyzer {
    dag: DagDefinition,
    activity_durations: HashMap<String, Duration>,
    default_duration: Duration,
}

impl SlackAnalyzer {
    /// Create an analyzer. A task with no known duration takes one second.
    #[must_use]
    pub fn new(dag: DagDefinition) -> Self {
        Self {
            dag,
            activity_durations: HashMap::new(),
            default_duration: Duration::from_secs(1),
        }
    }

    /// Set the duration of an activity with no mock.
    #[must_use]
    pub const fn with_default_duration(mut self, duration: Duration) -> Self {
        self.default_duration = duration;
        self
    }

    /// Mock the expected duration of an activity by name.
    ///
    /// A `start_to_close` timeout on the task takes precedence, as in the
    /// critical path analyzer.
    #[must_use]
    pub fn mock_duration(mut self, name: &str, duration: Duration) -> Self {
        self.activity_durations.insert(name.to_string(), duration);
        self
    }

    /// Run the forward and backward passes.
    #[must_use]
    pub fn analyze(&self) -> SlackReport {
        let tasks = self.dag.tasks();
        let n = tasks.len();
        let dur: Vec<u128> = tasks
            .iter()
            .map(|t| {
                t.start_to_close
                    .or_else(|| self.activity_durations.get(&t.activity_name).copied())
                    .unwrap_or(self.default_duration)
                    .as_nanos()
            })
            .collect();
        let order: Vec<usize> = self
            .dag
            .execution_levels()
            .iter()
            .flatten()
            .copied()
            .collect();

        // Forward pass: earliest start is the latest finish of any upstream.
        let mut earliest = vec![0u128; n];
        for &i in &order {
            earliest[i] = tasks[i]
                .upstreams
                .iter()
                .map(|&u| earliest[u].saturating_add(dur[u]))
                .max()
                .unwrap_or(0);
        }
        let total = (0..n)
            .map(|i| earliest[i].saturating_add(dur[i]))
            .max()
            .unwrap_or(0);

        // Backward pass: latest finish is the earliest latest-start of any
        // downstream task, or the total for a sink.
        let mut latest_finish = vec![total; n];
        for &i in order.iter().rev() {
            let latest_start = latest_finish[i].saturating_sub(dur[i]);
            for &u in &tasks[i].upstreams {
                latest_finish[u] = latest_finish[u].min(latest_start);
            }
        }

        let report = (0..n)
            .map(|i| {
                let latest_start = latest_finish[i].saturating_sub(dur[i]);
                let slack = latest_start.saturating_sub(earliest[i]);
                TaskSlack {
                    index: i,
                    name: tasks[i].activity_name.clone(),
                    earliest_start: to_duration(earliest[i]),
                    latest_start: to_duration(latest_start),
                    slack: to_duration(slack),
                    is_critical: slack == 0,
                }
            })
            .collect();
        SlackReport {
            total_duration: to_duration(total),
            tasks: report,
        }
    }
}

/// Convert nanoseconds to a `Duration`, saturating at `Duration::MAX`.
fn to_duration(nanos: u128) -> Duration {
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

    fn a() {}
    fn b() {}
    fn c() {}
    fn d() {}

    /// A -> B(5s) -> D and A -> C(1s) -> D, with A and D at 1s.
    fn diamond() -> DagDefinition {
        let mut builder = DagBuilder::new();
        let ta = builder.activity(a);
        let tb = builder.activity(b).upstream(&ta);
        let tc = builder.activity(c).upstream(&ta);
        let _td = builder.activity(d).upstream(&tb).upstream(&tc);
        builder.build().unwrap()
    }

    fn analyzer() -> SlackAnalyzer {
        SlackAnalyzer::new(diamond())
            .mock_duration("b", Duration::from_secs(5))
            .mock_duration("c", Duration::from_secs(1))
    }

    #[test]
    fn empty_dag_has_no_tasks() {
        let dag = DagBuilder::new().build().unwrap();
        let report = SlackAnalyzer::new(dag).analyze();
        assert_eq!(report.total_duration, Duration::ZERO);
        assert!(report.tasks.is_empty());
        assert!(report.most_flexible().is_none());
    }

    #[test]
    fn short_branch_has_slack_and_long_branch_is_critical() {
        let report = analyzer().analyze();
        assert_eq!(report.total_duration, Duration::from_secs(7));
        let by_name = |n: &str| report.tasks.iter().find(|t| t.name == n).unwrap();
        assert_eq!(by_name("c").slack, Duration::from_secs(4));
        assert_eq!(by_name("c").earliest_start, Duration::from_secs(1));
        assert_eq!(by_name("c").latest_start, Duration::from_secs(5));
        assert!(!by_name("c").is_critical);
        for n in ["a", "b", "d"] {
            assert!(by_name(n).is_critical, "{n} must be critical");
            assert_eq!(by_name(n).slack, Duration::ZERO);
        }
    }

    #[test]
    fn critical_tasks_match_the_critical_path_analyzer() {
        let report = analyzer().analyze();
        let path = CriticalPathAnalyzer::new(diamond())
            .mock_duration("b", Duration::from_secs(5))
            .mock_duration("c", Duration::from_secs(1))
            .analyze();
        assert_eq!(report.total_duration, path.total_duration);
        assert_eq!(report.critical_indices(), path.path_indices);
    }

    #[test]
    fn most_flexible_names_the_task_with_most_slack() {
        let report = analyzer().analyze();
        assert_eq!(report.most_flexible().unwrap().name, "c");
    }

    #[test]
    fn start_to_close_overrides_the_mock() {
        let mut builder = DagBuilder::new();
        let _x = builder.activity(a).start_to_close(Duration::from_secs(9));
        let dag = builder.build().unwrap();
        let report = SlackAnalyzer::new(dag)
            .mock_duration("a", Duration::from_secs(1))
            .analyze();
        assert_eq!(report.total_duration, Duration::from_secs(9));
    }

    #[test]
    fn saturates_instead_of_panicking_on_max_durations() {
        let mut builder = DagBuilder::new();
        let x = builder.activity(a).start_to_close(Duration::MAX);
        let _y = builder
            .activity(b)
            .start_to_close(Duration::MAX)
            .upstream(&x);
        let dag = builder.build().unwrap();
        let report = SlackAnalyzer::new(dag).analyze();
        assert_eq!(report.total_duration, Duration::MAX);
    }
}
