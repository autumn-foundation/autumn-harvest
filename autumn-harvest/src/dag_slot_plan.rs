//! Slot-limited schedule planning for DAG definitions.
//!
//! The concurrency profile gives the peak demand of the earliest and the
//! latest schedule. It cannot say what a run does with fewer slots than that.
//! This module answers that question. It runs the DAG on a fixed number of
//! worker slots and reports the makespan that the run then reaches.
//!
//! The planner is a list scheduler. When a slot is free, it starts the ready
//! task with the smallest latest start, so the task with the least slack goes
//! first. Ties go to the lower index. An optimal plan is NP-hard to find, so
//! this plan is a heuristic. Its makespan is an upper bound on the best one.
//!
//! The module is pure. It sends no SQL and reads no clock.

use crate::dag::DagDefinition;
use crate::dag_concurrency::peak_bounds;
use crate::dag_slack::{DagSlackReport, to_duration};
use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap};
use std::num::NonZeroUsize;
use std::time::Duration;

/// Where and when one task runs in a slot plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedTask {
    /// Topological index of the task.
    pub index: usize,
    /// Name of the task activity.
    pub name: String,
    /// Slot that runs the task. A zero-duration task takes no slot.
    pub slot: Option<usize>,
    /// Start of the task, from the start of the run.
    pub start: Duration,
    /// Finish of the task, from the start of the run.
    pub finish: Duration,
    /// Time the task waits for a slot after its earliest start.
    pub waited: Duration,
}

/// The result of a slot-limited plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotPlan {
    /// Worker slots that the plan uses.
    pub slots: usize,
    /// Run time of the DAG on those slots.
    pub makespan: Duration,
    /// Run time added by the slot limit.
    pub delay: Duration,
    /// One entry per task, in topological index order.
    pub tasks: Vec<PlannedTask>,
}

/// Plan the DAG on `slots` worker slots.
///
/// A zero-duration task starts and finishes when it is ready. It takes no
/// slot, as in the concurrency profile.
///
/// # Panics
///
/// Panics when the report and the DAG hold a different number of tasks.
#[must_use]
pub fn plan(dag: &DagDefinition, report: &DagSlackReport, slots: NonZeroUsize) -> SlotPlan {
    let tasks = dag.tasks();
    assert_eq!(
        tasks.len(),
        report.tasks.len(),
        "slack report does not match the DAG"
    );
    let n = tasks.len();
    // Nanoseconds in `u128` keep the sums exact.
    let durations: Vec<u128> = report.tasks.iter().map(|t| t.duration.as_nanos()).collect();
    let mut downstreams = vec![Vec::new(); n];
    let mut waiting: Vec<usize> = tasks.iter().map(|t| t.upstreams.len()).collect();
    for (i, task) in tasks.iter().enumerate() {
        for &up in &task.upstreams {
            downstreams[up].push(i);
        }
    }

    // The ready heap pops the smallest `(latest start, index)` first.
    let priority = |i: usize| Reverse((report.tasks[i].latest_start, i));
    let mut ready: BinaryHeap<_> = (0..n).filter(|&i| waiting[i] == 0).map(priority).collect();
    let mut free: BTreeSet<usize> = (0..slots.get()).collect();
    let mut running: BinaryHeap<Reverse<(u128, usize)>> = BinaryHeap::new();
    let mut start = vec![0u128; n];
    let mut slot = vec![None; n];
    let mut now = 0u128;

    loop {
        // Start ready tasks while a slot is free. A zero-duration task needs none.
        let mut held = Vec::new();
        while let Some(Reverse((_, i))) = ready.pop() {
            if durations[i] == 0 {
                start[i] = now;
                running.push(Reverse((now, i)));
            } else if let Some(s) = free.pop_first() {
                start[i] = now;
                slot[i] = Some(s);
                running.push(Reverse((now.saturating_add(durations[i]), i)));
            } else {
                held.push(priority(i));
            }
        }
        ready.extend(held);

        // Move the clock to the next finish and release its downstream tasks.
        let Some(Reverse((finish, _))) = running.peek().copied() else {
            break;
        };
        now = finish;
        while let Some(&Reverse((f, i))) = running.peek() {
            if f != now {
                break;
            }
            running.pop();
            if let Some(s) = slot[i] {
                free.insert(s);
            }
            for &down in &downstreams[i] {
                waiting[down] -= 1;
                if waiting[down] == 0 {
                    ready.push(priority(down));
                }
            }
        }
    }

    let planned: Vec<PlannedTask> = report
        .tasks
        .iter()
        .map(|t| PlannedTask {
            index: t.index,
            name: t.name.clone(),
            slot: slot[t.index],
            start: to_duration(start[t.index]),
            finish: to_duration(start[t.index].saturating_add(durations[t.index])),
            waited: to_duration(start[t.index]).saturating_sub(t.earliest_start),
        })
        .collect();
    let makespan = planned
        .iter()
        .map(|t| t.finish)
        .max()
        .unwrap_or(Duration::ZERO);
    SlotPlan {
        slots: slots.get(),
        makespan,
        delay: makespan.saturating_sub(report.makespan),
        tasks: planned,
    }
}

/// Fewest slots with which [`plan`] meets the unconstrained makespan.
///
/// The search stops at the larger peak of [`peak_bounds`]. With that many
/// slots, no ready task ever waits, so the plan meets the makespan. A list
/// scheduler can get slower with more slots, so the search tests each count
/// in turn and does not bisect.
///
/// # Panics
///
/// Panics when the report and the DAG hold a different number of tasks.
#[must_use]
pub fn fewest_slots(dag: &DagDefinition, report: &DagSlackReport) -> NonZeroUsize {
    let upper = NonZeroUsize::new(peak_bounds(report).1).unwrap_or(NonZeroUsize::MIN);
    (1..upper.get())
        .filter_map(NonZeroUsize::new)
        .find(|&k| plan(dag, report, k).delay.is_zero())
        .unwrap_or(upper)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagBuilder;
    use crate::dag_concurrency::peak_bounds;
    use crate::dag_slack::DagSlackAnalyzer;

    fn a() {}
    fn b() {}
    fn c() {}
    fn d() {}
    fn x() {}
    fn y() {}
    fn z() {}

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    fn slots(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).unwrap()
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

    /// Three independent tasks. The long one is added last.
    fn fan() -> (DagDefinition, DagSlackReport) {
        let mut builder = DagBuilder::new();
        let _ = builder.activity(y);
        let _ = builder.activity(z);
        let _ = builder.activity(x);
        let dag = builder.build().unwrap();
        let report = DagSlackAnalyzer::new(dag.clone())
            .mock_duration("x", secs(10))
            .mock_duration("y", secs(1))
            .mock_duration("z", secs(1))
            .analyze();
        (dag, report)
    }

    #[test]
    fn one_slot_runs_the_diamond_serially() {
        let (dag, report) = diamond();
        let p = plan(&dag, &report, slots(1));
        assert_eq!(p.makespan, secs(16));
        assert_eq!(p.delay, secs(3));
        // b has less slack than c, so b runs first and c waits for it.
        let c = &p.tasks[2];
        assert_eq!(c.name, "c");
        assert_eq!(c.start, secs(12));
        assert_eq!(c.waited, secs(10));
    }

    #[test]
    fn two_slots_meet_the_diamond_makespan() {
        let (dag, report) = diamond();
        let p = plan(&dag, &report, slots(2));
        assert_eq!(p.makespan, report.makespan);
        assert_eq!(p.delay, Duration::ZERO);
        assert_eq!(fewest_slots(&dag, &report).get(), 2);
    }

    #[test]
    fn least_slack_first_beats_the_peak_bound() {
        let (dag, report) = fan();
        // Both bounding schedules run all three tasks at once.
        assert_eq!(peak_bounds(&report), (3, 3));
        // The short tasks can share one slot, so two slots suffice.
        let p = plan(&dag, &report, slots(2));
        assert_eq!(p.makespan, secs(10));
        assert_eq!(p.tasks[2].start, Duration::ZERO);
        assert_eq!(fewest_slots(&dag, &report).get(), 2);
    }

    #[test]
    fn tasks_on_one_slot_never_overlap() {
        let (dag, report) = diamond();
        let p = plan(&dag, &report, slots(2));
        for t in &p.tasks {
            for u in p.tasks.iter().filter(|u| u.index != t.index) {
                if t.slot.is_some() && t.slot == u.slot {
                    assert!(t.finish <= u.start || u.finish <= t.start);
                }
            }
        }
    }

    #[test]
    fn upstream_tasks_finish_before_their_downstream_starts() {
        let (dag, report) = diamond();
        let p = plan(&dag, &report, slots(1));
        for (i, task) in dag.tasks().iter().enumerate() {
            for &up in &task.upstreams {
                assert!(p.tasks[up].finish <= p.tasks[i].start);
            }
        }
    }

    #[test]
    fn zero_duration_tasks_take_no_slot() {
        let mut builder = DagBuilder::new();
        let ta = builder.activity(a);
        let _tb = builder.activity(b).upstream(&ta);
        let _tc = builder.activity(c);
        let dag = builder.build().unwrap();
        let report = DagSlackAnalyzer::new(dag.clone())
            .mock_duration("a", Duration::ZERO)
            .analyze();
        let p = plan(&dag, &report, slots(1));
        assert_eq!(p.tasks[0].slot, None);
        assert_eq!(p.makespan, secs(2));
    }

    #[test]
    fn empty_dag_needs_one_slot_and_no_time() {
        let dag = DagBuilder::new().build().unwrap();
        let report = DagSlackAnalyzer::new(dag.clone()).analyze();
        let p = plan(&dag, &report, slots(4));
        assert_eq!(p.makespan, Duration::ZERO);
        assert_eq!(p.tasks, [] as [PlannedTask; 0]);
        assert_eq!(fewest_slots(&dag, &report).get(), 1);
    }
}
