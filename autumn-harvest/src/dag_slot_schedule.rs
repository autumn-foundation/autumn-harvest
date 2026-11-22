//! Slot-bounded schedules for DAG definitions.
//!
//! The concurrency profile gives the peak demand of a run with unlimited
//! workers. A real pool has a fixed number of slots. This module answers the
//! next question: how long does the run take with only `k` slots? It also
//! finds the fewest slots that still meet a target run time.
//!
//! The method is classic list scheduling. When a slot is free, the ready task
//! with the earliest latest start takes it. That is the task with the least
//! room to wait, so the critical path goes first. The schedule never leaves a
//! slot idle while a task is ready.
//!
//! List scheduling is a heuristic. Its makespan is at most twice the optimum
//! (Graham's bound). It is not monotone either: one more slot can make a rare
//! DAG finish later. `min_slots` therefore scans every slot count from one up.

use crate::dag::DagDefinition;
use crate::dag_slack::DagSlackReport;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::num::NonZeroUsize;
use std::time::Duration;

/// Ready tasks keyed by latest start, then by index.
type ReadyHeap = BinaryHeap<Reverse<(Duration, usize)>>;

/// Placement of one task in a slot-bounded schedule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotPlacement {
    /// Topological index of the task.
    pub index: usize,
    /// Name of the task activity.
    pub name: String,
    /// Slot that runs the task. `None` for a task with zero duration.
    pub slot: Option<usize>,
    /// Time the task starts, from the start of the run.
    pub start: Duration,
    /// Time the task finishes, from the start of the run.
    pub finish: Duration,
}

impl SlotPlacement {
    /// Time the task waits for a slot after its earliest start.
    #[must_use]
    pub fn queued(&self, earliest_start: Duration) -> Duration {
        self.start.saturating_sub(earliest_start)
    }
}

/// A run of the DAG on a fixed number of worker slots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotSchedule {
    /// Number of slots the schedule uses.
    pub slots: usize,
    /// Run time with this number of slots.
    pub makespan: Duration,
    /// Run time added by the slot limit, against unlimited slots.
    pub delay: Duration,
    /// One entry per task, in topological index order.
    pub placements: Vec<SlotPlacement>,
}

impl SlotSchedule {
    /// Share of slot time that runs a task, from 0.0 to 1.0.
    ///
    /// An empty run has a utilization of zero.
    #[must_use]
    pub fn utilization(&self) -> f64 {
        let capacity = self.makespan.as_secs_f64() * self.slots as f64;
        if capacity == 0.0 {
            return 0.0;
        }
        let busy: f64 = self
            .placements
            .iter()
            .map(|p| p.finish.saturating_sub(p.start).as_secs_f64())
            .sum();
        busy / capacity
    }
}

/// List scheduler built from a DAG and its slack report.
pub struct DagSlotScheduler<'a> {
    dag: &'a DagDefinition,
    report: &'a DagSlackReport,
}

impl<'a> DagSlotScheduler<'a> {
    /// Create a scheduler. The report must come from the same DAG.
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

    /// Schedule the run on `slots` worker slots.
    #[must_use]
    pub fn schedule(&self, slots: NonZeroUsize) -> SlotSchedule {
        let tasks = self.dag.tasks();
        let mut waiting: Vec<usize> = tasks.iter().map(|t| t.upstreams.len()).collect();
        let mut downstreams: Vec<Vec<usize>> = vec![Vec::new(); tasks.len()];
        for (i, task) in tasks.iter().enumerate() {
            for &up in &task.upstreams {
                downstreams[up].push(i);
            }
        }

        // Each heap pops its smallest key first. A ready task sorts by its
        // latest start, then by index, so the order is deterministic.
        let mut ready = ReadyHeap::new();
        let mut instant = Vec::new();
        let mut free: BinaryHeap<Reverse<usize>> = (0..slots.get()).map(Reverse).collect();
        let mut running: BinaryHeap<Reverse<(Duration, usize, usize)>> = BinaryHeap::new();
        let mut start = vec![Duration::ZERO; tasks.len()];
        let mut slot_of = vec![None; tasks.len()];
        let mut now = Duration::ZERO;

        // A task with zero duration needs no slot. It goes to `instant`.
        let release = |i: usize, ready: &mut ReadyHeap, instant: &mut Vec<usize>| {
            let task = &self.report.tasks[i];
            if task.duration.is_zero() {
                instant.push(i);
            } else {
                ready.push(Reverse((task.latest_start, i)));
            }
        };
        let unlock =
            |i: usize, waiting: &mut [usize], ready: &mut ReadyHeap, instant: &mut Vec<usize>| {
                for &down in &downstreams[i] {
                    waiting[down] -= 1;
                    if waiting[down] == 0 {
                        release(down, ready, instant);
                    }
                }
            };
        for i in (0..tasks.len()).filter(|&i| waiting[i] == 0) {
            release(i, &mut ready, &mut instant);
        }

        loop {
            // An instant task finishes as soon as it is ready.
            while let Some(i) = instant.pop() {
                start[i] = now;
                unlock(i, &mut waiting, &mut ready, &mut instant);
            }
            while !free.is_empty() && !ready.is_empty() {
                let (Some(Reverse(slot)), Some(Reverse((_, i)))) = (free.pop(), ready.pop()) else {
                    break;
                };
                start[i] = now;
                slot_of[i] = Some(slot);
                running.push(Reverse((now + self.report.tasks[i].duration, slot, i)));
            }
            let Some(Reverse((finish, _, _))) = running.peek().copied() else {
                break;
            };
            now = finish;
            while let Some(&Reverse((at, slot, i))) = running.peek() {
                if at != now {
                    break;
                }
                running.pop();
                free.push(Reverse(slot));
                unlock(i, &mut waiting, &mut ready, &mut instant);
            }
        }

        let placements: Vec<SlotPlacement> = self
            .report
            .tasks
            .iter()
            .map(|task| SlotPlacement {
                index: task.index,
                name: task.name.clone(),
                slot: slot_of[task.index],
                start: start[task.index],
                finish: start[task.index] + task.duration,
            })
            .collect();
        let makespan = placements
            .iter()
            .map(|p| p.finish)
            .max()
            .unwrap_or(Duration::ZERO);
        SlotSchedule {
            slots: slots.get(),
            makespan,
            delay: makespan.saturating_sub(self.report.makespan),
            placements,
        }
    }

    /// Fewest slots whose schedule finishes by `target`.
    ///
    /// Returns `None` when `target` is shorter than the run time with
    /// unlimited slots. No slot count can meet that target.
    #[must_use]
    pub fn min_slots(&self, target: Duration) -> Option<SlotSchedule> {
        if target < self.report.makespan {
            return None;
        }
        // With one slot per task, every task starts at its earliest start.
        // So the scan always ends by that slot count.
        (1..=self.report.tasks.len().max(1))
            .filter_map(NonZeroUsize::new)
            .map(|k| self.schedule(k))
            .find(|plan| plan.makespan <= target)
    }
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

    /// Four independent tasks: a (4s), b (3s), c (2s), d (1s).
    fn fan() -> (DagDefinition, DagSlackReport) {
        let mut builder = DagBuilder::new();
        let _ = builder.activity(a);
        let _ = builder.activity(b);
        let _ = builder.activity(c);
        let _ = builder.activity(d);
        let dag = builder.build().unwrap();
        let report = DagSlackAnalyzer::new(dag.clone())
            .mock_duration("a", secs(4))
            .mock_duration("b", secs(3))
            .mock_duration("c", secs(2))
            .mock_duration("d", secs(1))
            .analyze();
        (dag, report)
    }

    #[test]
    fn enough_slots_match_the_unbounded_makespan() {
        let (dag, report) = diamond();
        let plan = DagSlotScheduler::new(&dag, &report).schedule(slots(2));
        assert_eq!(plan.makespan, secs(13));
        assert_eq!(plan.delay, Duration::ZERO);
        for (p, t) in plan.placements.iter().zip(&report.tasks) {
            assert_eq!(p.start, t.earliest_start, "task {}", t.name);
        }
    }

    #[test]
    fn slack_absorbs_the_side_branch_on_one_slot() {
        let (dag, report) = diamond();
        let plan = DagSlotScheduler::new(&dag, &report).schedule(slots(1));
        // One slot runs every task in series: 2 + 10 + 3 + 1.
        assert_eq!(plan.makespan, secs(16));
        assert_eq!(plan.delay, secs(3));
        // The critical task b takes the slot before c.
        assert_eq!(plan.placements[1].start, secs(2));
        assert_eq!(plan.placements[2].start, secs(12));
        assert_eq!(
            plan.placements[2].queued(report.tasks[2].earliest_start),
            secs(10)
        );
    }

    #[test]
    fn critical_work_takes_a_slot_first() {
        let (dag, report) = fan();
        let plan = DagSlotScheduler::new(&dag, &report).schedule(slots(2));
        // a (no slack) and b start first. c and d fill the gaps.
        assert_eq!(plan.placements[0].start, Duration::ZERO);
        assert_eq!(plan.placements[1].start, Duration::ZERO);
        assert_eq!(plan.placements[2].start, secs(3));
        assert_eq!(plan.placements[3].start, secs(4));
        assert_eq!(plan.makespan, secs(5));
        assert_eq!(plan.delay, secs(1));
    }

    #[test]
    fn no_two_tasks_share_a_slot_at_once() {
        let (dag, report) = fan();
        let plan = DagSlotScheduler::new(&dag, &report).schedule(slots(2));
        for x in &plan.placements {
            for y in &plan.placements {
                if x.index != y.index && x.slot == y.slot {
                    assert!(x.finish <= y.start || y.finish <= x.start);
                }
            }
            assert!(x.slot.unwrap() < 2);
        }
    }

    #[test]
    fn utilization_counts_busy_slot_time() {
        let (dag, report) = fan();
        let plan = DagSlotScheduler::new(&dag, &report).schedule(slots(2));
        // 10 task-seconds over 2 slots for 5 seconds.
        assert!((plan.utilization() - 1.0).abs() < 1e-9);
    }

    #[test]
    fn min_slots_finds_the_smallest_pool_that_meets_the_target() {
        let (dag, report) = fan();
        let scheduler = DagSlotScheduler::new(&dag, &report);
        assert_eq!(scheduler.min_slots(secs(4)).unwrap().slots, 3);
        assert_eq!(scheduler.min_slots(secs(5)).unwrap().slots, 2);
        assert_eq!(scheduler.min_slots(secs(10)).unwrap().slots, 1);
    }

    #[test]
    fn min_slots_beats_the_earliest_start_peak() {
        let (dag, report) = diamond();
        let scheduler = DagSlotScheduler::new(&dag, &report);
        let (_, upper) = crate::dag_concurrency::peak_bounds(&report);
        let plan = scheduler.min_slots(report.makespan).unwrap();
        assert!(plan.slots <= upper);
        assert_eq!(plan.makespan, report.makespan);
    }

    #[test]
    fn min_slots_refuses_a_target_below_the_critical_path() {
        let (dag, report) = diamond();
        let scheduler = DagSlotScheduler::new(&dag, &report);
        assert_eq!(scheduler.min_slots(secs(12)), None);
    }

    #[test]
    fn zero_duration_tasks_take_no_slot() {
        let mut builder = DagBuilder::new();
        let ta = builder.activity(a);
        let _tb = builder.activity(b).upstream(&ta);
        let dag = builder.build().unwrap();
        let report = DagSlackAnalyzer::new(dag.clone())
            .mock_duration("a", Duration::ZERO)
            .analyze();
        let plan = DagSlotScheduler::new(&dag, &report).schedule(slots(1));
        assert_eq!(plan.placements[0].slot, None);
        assert_eq!(plan.placements[1].slot, Some(0));
        assert_eq!(plan.makespan, secs(1));
    }

    #[test]
    fn empty_dag_schedules_nothing() {
        let dag = DagBuilder::new().build().unwrap();
        let report = DagSlackAnalyzer::new(dag.clone()).analyze();
        let scheduler = DagSlotScheduler::new(&dag, &report);
        let plan = scheduler.schedule(slots(3));
        assert_eq!(plan.makespan, Duration::ZERO);
        assert_eq!(plan.placements, Vec::new());
        assert!(plan.utilization().abs() < f64::EPSILON);
        assert_eq!(scheduler.min_slots(Duration::ZERO).unwrap().slots, 1);
    }
}
