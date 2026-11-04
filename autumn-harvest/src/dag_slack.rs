//! Slack (float) analysis for DAG definitions.
//!
//! The critical path names the tasks that set the run time. This module answers
//! the next question: how long can each *other* task slip before it joins the
//! critical path? It runs the classic critical path method. A forward pass finds
//! the earliest start of each task. A backward pass finds the latest start that
//! still meets the overall finish time. The difference is the slack.

use crate::dag::DagDefinition;
use std::collections::HashMap;
use std::time::Duration;

/// Timing figures for one task. All offsets count from the start of the run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskSlack {
    /// Topological index of the task.
    pub index: usize,
    /// Name of the task activity.
    pub name: String,
    /// Expected duration of the task.
    pub duration: Duration,
    /// Earliest time the task can start.
    pub earliest_start: Duration,
    /// Earliest time the task can finish.
    pub earliest_finish: Duration,
    /// Latest time the task can start without delaying the run.
    pub latest_start: Duration,
    /// Latest time the task can finish without delaying the run.
    pub latest_finish: Duration,
    /// Time the task can slip without delaying the run.
    pub slack: Duration,
}

impl TaskSlack {
    /// Return `true` when the task has no slack, so any delay delays the run.
    #[must_use]
    pub const fn is_critical(&self) -> bool {
        self.slack.is_zero()
    }
}

/// Result of a slack analysis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DagSlackReport {
    /// Expected total duration of the run.
    pub makespan: Duration,
    /// One entry per task, in topological index order.
    pub tasks: Vec<TaskSlack>,
}

impl DagSlackReport {
    /// Indices of the tasks with zero slack, in ascending order.
    #[must_use]
    pub fn critical_indices(&self) -> Vec<usize> {
        self.tasks
            .iter()
            .filter(|task| task.is_critical())
            .map(|task| task.index)
            .collect()
    }

    /// The task with the most slack, or `None` for an empty DAG.
    ///
    /// This task is the cheapest one to run on slower or cheaper capacity.
    #[must_use]
    pub fn most_flexible(&self) -> Option<&TaskSlack> {
        self.tasks.iter().max_by_key(|task| task.slack)
    }
}

/// Analyzer that computes the slack of each task in a DAG.
pub struct DagSlackAnalyzer {
    dag: DagDefinition,
    activity_durations: HashMap<String, Duration>,
    default_duration: Duration,
}

impl DagSlackAnalyzer {
    /// Create an analyzer. Unmocked tasks default to one second.
    #[must_use]
    pub fn new(dag: DagDefinition) -> Self {
        Self {
            dag,
            activity_durations: HashMap::new(),
            default_duration: Duration::from_secs(1),
        }
    }

    /// Set the duration of an activity that has no mocked duration.
    #[must_use]
    pub const fn with_default_duration(mut self, duration: Duration) -> Self {
        self.default_duration = duration;
        self
    }

    /// Set the expected duration of an activity by name.
    #[must_use]
    pub fn mock_duration(mut self, name: &str, duration: Duration) -> Self {
        self.activity_durations.insert(name.to_string(), duration);
        self
    }

    /// Compute earliest and latest times and the slack of every task.
    ///
    /// A `start_to_close` timeout on a task overrides any mocked duration, as in
    /// `CriticalPathAnalyzer`.
    #[must_use]
    pub fn analyze(&self) -> DagSlackReport {
        let tasks = self.dag.tasks();
        let levels = self.dag.execution_levels();

        // Nanoseconds in `u128` keep the sums exact.
        let durations: Vec<u128> = tasks
            .iter()
            .map(|task| {
                task.start_to_close
                    .or_else(|| self.activity_durations.get(&task.activity_name).copied())
                    .unwrap_or(self.default_duration)
                    .as_nanos()
            })
            .collect();

        let mut earliest_start = vec![0u128; tasks.len()];
        for level in levels {
            for &i in level {
                earliest_start[i] = tasks[i]
                    .upstreams
                    .iter()
                    .map(|&up| earliest_start[up].saturating_add(durations[up]))
                    .max()
                    .unwrap_or(0);
            }
        }

        let makespan = (0..tasks.len())
            .map(|i| earliest_start[i].saturating_add(durations[i]))
            .max()
            .unwrap_or(0);

        // Each task must finish before the latest start of every task that uses it.
        let mut latest_finish = vec![makespan; tasks.len()];
        for level in levels.iter().rev() {
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
                    name: task.activity_name.clone(),
                    duration: to_duration(durations[i]),
                    earliest_start: to_duration(earliest_start[i]),
                    earliest_finish: to_duration(earliest_start[i].saturating_add(durations[i])),
                    latest_start: to_duration(latest_start),
                    latest_finish: to_duration(latest_finish[i]),
                    slack: to_duration(latest_start.saturating_sub(earliest_start[i])),
                }
            })
            .collect();

        DagSlackReport {
            makespan: to_duration(makespan),
            tasks: entries,
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

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    /// Diamond: a -> b (10s) -> d, a -> c (3s) -> d.
    fn diamond() -> DagSlackAnalyzer {
        let mut builder = DagBuilder::new();
        let ta = builder.activity(a);
        let tb = builder.activity(b).upstream(&ta);
        let tc = builder.activity(c).upstream(&ta);
        let _td = builder.activity(d).upstream(&tb).upstream(&tc);
        DagSlackAnalyzer::new(builder.build().unwrap())
            .mock_duration("a", secs(2))
            .mock_duration("b", secs(10))
            .mock_duration("c", secs(3))
            .mock_duration("d", secs(1))
    }

    #[test]
    fn empty_dag_has_no_tasks_and_no_makespan() {
        let report = DagSlackAnalyzer::new(DagBuilder::new().build().unwrap()).analyze();
        assert_eq!(report.makespan, Duration::ZERO);
        assert!(report.tasks.is_empty());
        assert!(report.most_flexible().is_none());
    }

    #[test]
    fn diamond_short_branch_has_slack() {
        let report = diamond().analyze();
        assert_eq!(report.makespan, secs(13));

        let short = &report.tasks[2];
        assert_eq!(short.name, "c");
        assert_eq!(short.earliest_start, secs(2));
        assert_eq!(short.latest_start, secs(9));
        assert_eq!(short.slack, secs(7));
        assert!(!short.is_critical());

        assert_eq!(report.critical_indices(), vec![0, 1, 3]);
        assert_eq!(report.most_flexible().unwrap().name, "c");
    }

    #[test]
    fn linear_chain_is_fully_critical() {
        let mut builder = DagBuilder::new();
        let ta = builder.activity(a);
        let _tb = builder.activity(b).upstream(&ta);
        let report = DagSlackAnalyzer::new(builder.build().unwrap())
            .with_default_duration(secs(4))
            .analyze();
        assert_eq!(report.makespan, secs(8));
        assert_eq!(report.critical_indices(), vec![0, 1]);
    }

    #[test]
    fn parallel_roots_slack_against_the_longest_root() {
        let mut builder = DagBuilder::new();
        let _ta = builder.activity(a);
        let _tb = builder.activity(b);
        let report = DagSlackAnalyzer::new(builder.build().unwrap())
            .mock_duration("a", secs(5))
            .mock_duration("b", secs(2))
            .analyze();
        assert_eq!(report.tasks[0].slack, Duration::ZERO);
        assert_eq!(report.tasks[1].slack, secs(3));
    }

    #[test]
    fn critical_tasks_match_the_critical_path_analyzer() {
        let mut builder = DagBuilder::new();
        let ta = builder.activity(a);
        let tb = builder.activity(b).upstream(&ta);
        let tc = builder.activity(c).upstream(&ta);
        let _td = builder.activity(d).upstream(&tb).upstream(&tc);
        let dag = builder.build().unwrap();
        let path = CriticalPathAnalyzer::new(dag.clone())
            .mock_duration("b", secs(10))
            .analyze();
        let report = DagSlackAnalyzer::new(dag)
            .mock_duration("b", secs(10))
            .analyze();
        assert_eq!(report.makespan, path.total_duration);
        assert_eq!(report.critical_indices(), path.path_indices);
    }
}
