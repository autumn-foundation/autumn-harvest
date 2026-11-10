//! Worker-demand profile for DAG definitions.
//!
//! Slack says when a task may run. This module asks how many tasks run at the
//! same time. Run every task as early as possible and the demand peaks early.
//! Run every task as late as possible and it peaks elsewhere. The peak sizes
//! the worker slots a run needs to meet its makespan. The gap between the two
//! peaks shows how much slack can level the load.

use crate::dag_slack::DagSlackReport;
use std::time::Duration;

/// Which start time each task takes in a profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Schedule {
    /// Every task starts at its earliest start.
    Earliest,
    /// Every task starts at its latest start.
    Latest,
}

/// A span of time with a constant number of running tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Segment {
    /// Start of the span.
    pub start: Duration,
    /// End of the span.
    pub end: Duration,
    /// Tasks that run during the span.
    pub running: usize,
}

/// Step function of running tasks over the life of a run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConcurrencyProfile {
    /// Spans in time order. Idle spans are omitted.
    pub segments: Vec<Segment>,
}

impl ConcurrencyProfile {
    /// Most tasks that run at once. Zero for an empty profile.
    #[must_use]
    pub fn peak(&self) -> usize {
        self.segments.iter().map(|s| s.running).max().unwrap_or(0)
    }

    /// Start of the first span that reaches the peak.
    #[must_use]
    pub fn peak_at(&self) -> Option<Duration> {
        let peak = self.peak();
        self.segments
            .iter()
            .find(|s| s.running == peak)
            .map(|s| s.start)
    }

    /// Total task time in the profile, in task-seconds.
    #[must_use]
    pub fn task_time(&self) -> Duration {
        self.segments
            .iter()
            .map(|s| s.end.saturating_sub(s.start) * u32::try_from(s.running).unwrap_or(u32::MAX))
            .sum()
    }
}

/// Build the worker-demand profile of a slack report.
///
/// A task with zero duration never runs, so it adds no demand.
#[must_use]
pub fn profile(report: &DagSlackReport, schedule: Schedule) -> ConcurrencyProfile {
    // Each edge is `(time, +1 or -1)`. A finish sorts before a start at the
    // same time, so back-to-back tasks do not count as concurrent.
    let mut edges: Vec<(Duration, i64)> = Vec::new();
    for task in report.tasks.iter().filter(|t| !t.duration.is_zero()) {
        let start = match schedule {
            Schedule::Earliest => task.earliest_start,
            Schedule::Latest => task.latest_start,
        };
        edges.push((start, 1));
        edges.push((start.saturating_add(task.duration), -1));
    }
    edges.sort();

    let mut segments = Vec::new();
    let mut running: i64 = 0;
    let mut cursor = Duration::ZERO;
    for (time, delta) in edges {
        if running > 0 && time > cursor {
            segments.push(Segment {
                start: cursor,
                end: time,
                running: usize::try_from(running).unwrap_or(0),
            });
        }
        running += delta;
        cursor = time;
    }
    ConcurrencyProfile { segments }
}

/// Smallest and largest peak across the two bounding schedules.
///
/// Returns `(lower, upper)`. A run with `upper` slots always meets its
/// makespan. A run with fewer than `lower` slots may still meet it when slack
/// levels the load, but that is not guaranteed by these two schedules alone.
#[must_use]
pub fn peak_bounds(report: &DagSlackReport) -> (usize, usize) {
    let early = profile(report, Schedule::Earliest).peak();
    let late = profile(report, Schedule::Latest).peak();
    (early.min(late), early.max(late))
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
    fn diamond() -> DagSlackReport {
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
            .analyze()
    }

    #[test]
    fn earliest_schedule_overlaps_b_and_c_right_after_a() {
        let p = profile(&diamond(), Schedule::Earliest);
        assert_eq!(p.peak(), 2);
        assert_eq!(p.peak_at(), Some(secs(2)));
        assert_eq!(p.segments.first().unwrap().running, 1);
    }

    #[test]
    fn latest_schedule_pushes_c_toward_d() {
        let p = profile(&diamond(), Schedule::Latest);
        // c runs from 9s to 12s, so it overlaps b for 3s at the end.
        assert_eq!(p.peak(), 2);
        assert_eq!(p.peak_at(), Some(secs(9)));
    }

    #[test]
    fn task_time_is_schedule_independent() {
        let report = diamond();
        let total = secs(2 + 10 + 3 + 1);
        assert_eq!(profile(&report, Schedule::Earliest).task_time(), total);
        assert_eq!(profile(&report, Schedule::Latest).task_time(), total);
    }

    #[test]
    fn back_to_back_tasks_do_not_overlap() {
        let mut builder = DagBuilder::new();
        let ta = builder.activity(a);
        let _tb = builder.activity(b).upstream(&ta);
        let report = DagSlackAnalyzer::new(builder.build().unwrap()).analyze();
        assert_eq!(peak_bounds(&report), (1, 1));
    }

    #[test]
    fn independent_tasks_all_run_at_once() {
        let mut builder = DagBuilder::new();
        let _ = builder.activity(a);
        let _ = builder.activity(b);
        let _ = builder.activity(c);
        let report = DagSlackAnalyzer::new(builder.build().unwrap()).analyze();
        assert_eq!(peak_bounds(&report), (3, 3));
    }

    #[test]
    fn empty_dag_has_no_demand() {
        let report = DagSlackAnalyzer::new(DagBuilder::new().build().unwrap()).analyze();
        let p = profile(&report, Schedule::Earliest);
        assert_eq!(p.peak(), 0);
        assert_eq!(p.peak_at(), None);
        assert!(p.segments.is_empty());
    }

    #[test]
    fn zero_duration_tasks_add_no_demand() {
        let mut builder = DagBuilder::new();
        let _ = builder.activity(a);
        let _ = builder.activity(b);
        let report = DagSlackAnalyzer::new(builder.build().unwrap())
            .mock_duration("b", Duration::ZERO)
            .analyze();
        assert_eq!(peak_bounds(&report), (1, 1));
    }
}
