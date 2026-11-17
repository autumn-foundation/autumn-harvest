//! Deadline crash planning for DAG definitions.
//!
//! Leverage asks what one faster task buys. A crash plan asks the reverse
//! question: what is the cheapest way to make the whole run meet a deadline?
//! Each task can declare a floor duration and a cost for each second it gives
//! up. The planner then shortens tasks until the run fits the target.
//!
//! The method is the greedy form of classic project crashing. Each step
//! shortens the task that buys a second of run time at the lowest price. Only
//! the useful part of a cut is bought. A cut that would only add slack to the
//! task is never paid for. When two parallel paths tie, no single cut helps.
//! The plan then stops and reports the deadline as missed.

use crate::dag::DagDefinition;
use crate::dag_slack::DagSlackReport;
use std::collections::HashMap;
use std::time::Duration;

/// How far one activity can be shortened, and at what price.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CrashOption {
    /// Shortest duration the activity can reach.
    pub floor: Duration,
    /// Cost of one second removed from the activity, in caller units.
    pub cost_per_second: u64,
}

/// One cut in a crash plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrashStep {
    /// Topological index of the task.
    pub index: usize,
    /// Name of the task activity.
    pub name: String,
    /// Time removed from the task by this step.
    pub shortened_by: Duration,
    /// Cost of this step, rounded up to a whole unit.
    pub cost: u128,
    /// Run time of the DAG after this step.
    pub makespan_after: Duration,
}

/// Result of a crash planning run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrashPlan {
    /// Requested run time.
    pub target: Duration,
    /// Run time before any cut.
    pub baseline: Duration,
    /// Run time after every cut in the plan.
    pub achieved: Duration,
    /// Cuts in the order the planner chose them.
    pub steps: Vec<CrashStep>,
    /// Sum of the step costs.
    pub total_cost: u128,
    /// New duration of each task, in topological index order.
    pub durations: Vec<Duration>,
}

impl CrashPlan {
    /// Return `true` when the plan meets the target.
    #[must_use]
    pub fn meets_target(&self) -> bool {
        self.achieved <= self.target
    }
}

/// Crash planner built from a DAG and its slack report.
pub struct DagCrashPlanner<'a> {
    dag: &'a DagDefinition,
    report: &'a DagSlackReport,
    options: HashMap<String, CrashOption>,
}

impl<'a> DagCrashPlanner<'a> {
    /// Create a planner. The report must come from the same DAG.
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
        Self {
            dag,
            report,
            options: HashMap::new(),
        }
    }

    /// Let the activity `name` shrink to `floor` at `cost_per_second`.
    ///
    /// A task with no option keeps its duration.
    #[must_use]
    pub fn crashable(mut self, name: &str, floor: Duration, cost_per_second: u64) -> Self {
        self.options.insert(
            name.to_string(),
            CrashOption {
                floor,
                cost_per_second,
            },
        );
        self
    }

    /// Build the cheapest greedy plan that brings the run down to `target`.
    #[must_use]
    pub fn plan(&self, target: Duration) -> CrashPlan {
        // Nanoseconds in `u128` keep the sums exact.
        let mut durations: Vec<u128> = self
            .report
            .tasks
            .iter()
            .map(|task| task.duration.as_nanos())
            .collect();
        let target_nanos = target.as_nanos();
        let baseline = self.makespan_of(&durations);
        let mut makespan = baseline;
        let mut steps = Vec::new();
        let mut total_cost = 0u128;
        while makespan > target_nanos {
            let Some((index, gain, rate)) = self.cheapest_cut(&durations, makespan, target_nanos)
            else {
                break;
            };
            // The makespan falls with slope one, then stays flat.
            // So the gain is exactly the useful part of the cut.
            durations[index] -= gain;
            makespan -= gain;
            let cost = step_cost(gain, rate);
            total_cost = total_cost.saturating_add(cost);
            steps.push(CrashStep {
                index,
                name: self.report.tasks[index].name.clone(),
                shortened_by: to_duration(gain),
                cost,
                makespan_after: to_duration(makespan),
            });
        }
        CrashPlan {
            target,
            baseline: to_duration(baseline),
            achieved: to_duration(makespan),
            steps,
            total_cost,
            durations: durations.into_iter().map(to_duration).collect(),
        }
    }

    /// Find the cut with the lowest price per second of run time.
    ///
    /// Returns the task index, the run time gained and the price per second.
    /// Equal prices keep index order. Returns `None` when no cut helps.
    fn cheapest_cut(
        &self,
        durations: &[u128],
        makespan: u128,
        target: u128,
    ) -> Option<(usize, u128, u64)> {
        let gap = makespan - target;
        let mut best: Option<(usize, u128, u64)> = None;
        let mut trial = durations.to_vec();
        for task in &self.report.tasks {
            let Some(option) = self.options.get(&task.name) else {
                continue;
            };
            let current = durations[task.index];
            let floor = option.floor.as_nanos().min(current);
            trial[task.index] = floor.max(current.saturating_sub(gap));
            let gain = makespan - self.makespan_of(&trial);
            trial[task.index] = current;
            let cheaper = best.is_none_or(|(_, _, rate)| option.cost_per_second < rate);
            if gain > 0 && cheaper {
                best = Some((task.index, gain, option.cost_per_second));
            }
        }
        best
    }

    /// Run time of the DAG for the given task durations.
    fn makespan_of(&self, durations: &[u128]) -> u128 {
        let tasks = self.dag.tasks();
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
        makespan
    }
}

/// Cost of removing `nanos` at `rate` per second, rounded up.
///
/// Whole seconds and the remainder are priced apart. So the product never
/// scales a large duration by one billion before the division.
fn step_cost(nanos: u128, rate: u64) -> u128 {
    let rate = u128::from(rate);
    let whole = (nanos / 1_000_000_000).saturating_mul(rate);
    let part = (nanos % 1_000_000_000 * rate).div_ceil(1_000_000_000);
    whole.saturating_add(part)
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
    fn met_target_needs_no_cuts() {
        let (dag, report) = diamond();
        let plan = DagCrashPlanner::new(&dag, &report)
            .crashable("b", secs(1), 5)
            .plan(secs(13));
        assert!(plan.meets_target());
        assert_eq!(plan.steps, Vec::new());
        assert_eq!(plan.total_cost, 0);
        assert_eq!(plan.durations, vec![secs(2), secs(10), secs(3), secs(1)]);
    }

    #[test]
    fn cheapest_critical_task_is_cut_first() {
        let (dag, report) = diamond();
        let plan = DagCrashPlanner::new(&dag, &report)
            .crashable("a", secs(0), 50)
            .crashable("b", secs(1), 5)
            .plan(secs(10));
        assert!(plan.meets_target());
        assert_eq!(plan.achieved, secs(10));
        assert_eq!(plan.steps.len(), 1);
        assert_eq!(plan.steps[0].name, "b");
        assert_eq!(plan.steps[0].shortened_by, secs(3));
        assert_eq!(plan.total_cost, 15);
        assert_eq!(plan.durations[1], secs(7));
    }

    #[test]
    fn off_critical_task_is_never_bought() {
        let (dag, report) = diamond();
        let plan = DagCrashPlanner::new(&dag, &report)
            .crashable("c", secs(0), 1)
            .plan(secs(12));
        assert!(!plan.meets_target());
        assert_eq!(plan.steps, Vec::new());
        assert_eq!(plan.achieved, secs(13));
    }

    #[test]
    fn cut_stops_where_a_parallel_path_takes_over() {
        let (dag, report) = diamond();
        // Past 6s the path a -> c -> d sets the run time.
        // Only `a` is shared, so the plan cuts it next.
        let plan = DagCrashPlanner::new(&dag, &report)
            .crashable("a", secs(1), 20)
            .crashable("b", secs(0), 2)
            .plan(secs(4));
        assert_eq!(plan.steps[0].name, "b");
        assert_eq!(plan.steps[0].shortened_by, secs(7));
        assert_eq!(plan.steps[0].makespan_after, secs(6));
        assert_eq!(plan.steps[1].name, "a");
        assert_eq!(plan.steps[1].shortened_by, secs(1));
        assert_eq!(plan.achieved, secs(5));
        assert!(!plan.meets_target());
        assert_eq!(plan.total_cost, 7 * 2 + 20);
    }

    #[test]
    fn fractional_cost_rounds_up() {
        let (dag, report) = diamond();
        let plan = DagCrashPlanner::new(&dag, &report)
            .crashable("d", Duration::ZERO, 3)
            .plan(Duration::from_millis(12_500));
        assert_eq!(plan.steps[0].shortened_by, Duration::from_millis(500));
        assert_eq!(plan.total_cost, 2);
    }

    #[test]
    fn huge_cut_cost_does_not_overflow() {
        let gain = u128::from(u64::MAX) * 1_000_000_000;
        let expected = u128::from(u64::MAX) * u128::from(u64::MAX);
        assert_eq!(step_cost(gain, u64::MAX), expected);
        assert_eq!(step_cost(1, 1), 1);
    }

    #[test]
    fn empty_dag_plans_nothing() {
        let dag = DagBuilder::new().build().unwrap();
        let report = DagSlackAnalyzer::new(dag.clone()).analyze();
        let plan = DagCrashPlanner::new(&dag, &report).plan(Duration::ZERO);
        assert!(plan.meets_target());
        assert_eq!(plan.durations, Vec::new());
    }

    #[test]
    #[should_panic(expected = "does not match")]
    fn mismatched_report_panics() {
        let (dag, _) = diamond();
        let empty = DagSlackAnalyzer::new(DagBuilder::new().build().unwrap()).analyze();
        let _ = DagCrashPlanner::new(&dag, &empty);
    }
}
