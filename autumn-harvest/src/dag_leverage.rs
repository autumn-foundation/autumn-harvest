//! What-if leverage analysis for DAG definitions.
//!
//! Slack says how far a task can slip. Leverage asks the opposite question:
//! how much faster does the whole run get if this one task gets faster? A
//! task off the critical path has zero leverage. A critical task has leverage
//! up to the gap to the next longest path. The ranking tells an operator
//! which activity to optimize first.

use crate::dag::DagDefinition;
use crate::dag_slack::DagSlackReport;
use std::time::Duration;

/// Effect of making one task instant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskLeverage {
    /// Topological index of the task.
    pub index: usize,
    /// Name of the task activity.
    pub name: String,
    /// Run time of the DAG if the task took no time.
    pub makespan_if_free: Duration,
    /// Run time saved when the task takes no time.
    pub saved: Duration,
}

/// What-if analyzer built from a DAG and its slack report.
pub struct DagLeverage<'a> {
    dag: &'a DagDefinition,
    report: &'a DagSlackReport,
}

impl<'a> DagLeverage<'a> {
    /// Create an analyzer. The report must come from the same DAG.
    ///
    /// # Panics
    ///
    /// Panics when the report and the DAG hold a different number of tasks.
    #[must_use]
    pub fn new(dag: &'a DagDefinition, report: &'a DagSlackReport) -> Self {
        assert_eq!(
            dag.tasks().len(),
            report.tasks.len(),
            "slack report does not match the DAG"
        );
        Self { dag, report }
    }

    /// Run time of the DAG when task `index` takes `duration` instead.
    ///
    /// Returns `None` when `index` is out of range.
    #[must_use]
    pub fn makespan_with(&self, index: usize, duration: Duration) -> Option<Duration> {
        let tasks = self.dag.tasks();
        if index >= tasks.len() {
            return None;
        }
        // Nanoseconds in `u128` keep the sums exact.
        let durations: Vec<u128> = self
            .report
            .tasks
            .iter()
            .map(|task| {
                if task.index == index {
                    duration.as_nanos()
                } else {
                    task.duration.as_nanos()
                }
            })
            .collect();
        let mut finish = vec![0u128; tasks.len()];
        let mut makespan = 0u128;
        for level in self.dag.execution_levels() {
            for &i in level {
                let start = tasks[i]
                    .upstreams
                    .iter()
                    .map(|&up| finish[up])
                    .max()
                    .unwrap_or(0);
                finish[i] = start.saturating_add(durations[i]);
                makespan = makespan.max(finish[i]);
            }
        }
        Some(to_duration(makespan))
    }

    /// Rank every task by the time saved when it takes no time.
    ///
    /// The most valuable task comes first. Equal savings keep index order.
    #[must_use]
    pub fn rank(&self) -> Vec<TaskLeverage> {
        let mut ranking: Vec<TaskLeverage> = self
            .report
            .tasks
            .iter()
            .map(|task| {
                let free = self
                    .makespan_with(task.index, Duration::ZERO)
                    .unwrap_or(self.report.makespan);
                TaskLeverage {
                    index: task.index,
                    name: task.name.clone(),
                    makespan_if_free: free,
                    saved: self.report.makespan.saturating_sub(free),
                }
            })
            .collect();
        ranking.sort_by(|x, y| y.saved.cmp(&x.saved).then(x.index.cmp(&y.index)));
        ranking
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
    use crate::dag::DagBuilder;
    use crate::dag_slack::DagSlackAnalyzer;

    fn a() {}
    fn b() {}
    fn c() {}
    fn d() {}

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    /// Diamond: a (2s) -> b (10s) -> d (1s), a -> c (3s) -> d.
    fn diamond() -> (DagDefinition, DagSlackReport) {
        let mut builder = DagBuilder::new();
        let ta = builder.activity(a);
        let tb = builder.activity(b).upstream(&ta);
        let tc = builder.activity(c).upstream(&ta);
        let _td = builder.activity(d).upstream(&tb).upstream(&tc);
        let dag = builder.build().unwrap();
        let report = DagSlackAnalyzer::new(dag.clone())
            .mock_duration("a", secs(2))
            .mock_duration("b", secs(10))
            .mock_duration("c", secs(3))
            .mock_duration("d", secs(1))
            .analyze();
        (dag, report)
    }

    #[test]
    fn off_critical_task_has_zero_leverage() {
        let (dag, report) = diamond();
        let ranking = DagLeverage::new(&dag, &report).rank();
        let c = ranking.iter().find(|t| t.name == "c").unwrap();
        assert_eq!(c.saved, Duration::ZERO);
        assert_eq!(c.makespan_if_free, secs(13));
    }

    #[test]
    fn critical_task_is_capped_by_the_next_path() {
        let (dag, report) = diamond();
        let ranking = DagLeverage::new(&dag, &report).rank();
        // Without `b` the run follows a -> c -> d: 2 + 3 + 1 = 6s.
        assert_eq!(ranking[0].name, "b");
        assert_eq!(ranking[0].makespan_if_free, secs(6));
        assert_eq!(ranking[0].saved, secs(7));
        // Shared tasks save their whole duration.
        assert_eq!(ranking[1].name, "a");
        assert_eq!(ranking[1].saved, secs(2));
        assert_eq!(ranking[2].name, "d");
    }

    #[test]
    fn makespan_with_supports_partial_speedups() {
        let (dag, report) = diamond();
        let lever = DagLeverage::new(&dag, &report);
        assert_eq!(lever.makespan_with(1, secs(4)), Some(secs(7)));
        assert_eq!(lever.makespan_with(2, secs(30)), Some(secs(33)));
        assert_eq!(lever.makespan_with(9, secs(1)), None);
    }

    #[test]
    fn empty_dag_ranks_nothing() {
        let dag = DagBuilder::new().build().unwrap();
        let report = DagSlackAnalyzer::new(dag.clone()).analyze();
        assert!(DagLeverage::new(&dag, &report).rank().is_empty());
    }

    #[test]
    #[should_panic(expected = "does not match")]
    fn mismatched_report_panics() {
        let (dag, _) = diamond();
        let empty = DagSlackAnalyzer::new(DagBuilder::new().build().unwrap()).analyze();
        let _ = DagLeverage::new(&dag, &empty);
    }
}
